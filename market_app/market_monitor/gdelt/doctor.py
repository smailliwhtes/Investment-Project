from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

from market_monitor.gdelt.utils import (
    EVENTS_HEADER_COLUMNS,
    EVENTS_REQUIRED_FIELDS,
    GKG_HEADER_COLUMNS,
    GKG_REQUIRED_FIELDS,
    analyze_file,
    detect_schema_type,
    estimate_rows,
    list_files,
    map_columns,
    parse_day,
    utc_now_iso,
)


READY_STABLE = "READY_STABLE"
NEEDS_NORMALIZATION = "NEEDS_NORMALIZATION"
UNUSABLE = "UNUSABLE"


@dataclass
class DialectInfo:
    delimiter: str
    has_header: bool
    quoting_irregularities: bool


@dataclass
class DateStats:
    min_day: str | None
    max_day: str | None
    parseable_rate: float


@dataclass
class RequiredFieldReport:
    availability: dict[str, bool]
    coverage: dict[str, float]


@dataclass
class FileAudit:
    path: str
    size_bytes: int
    estimated_rows: int
    rows_estimated: bool
    dialect: DialectInfo
    schema_type: str
    date_stats: DateStats
    required_fields: RequiredFieldReport
    gkg_semicolon_fields: dict[str, bool]
    readiness_verdict: str
    issues: list[str]


@dataclass
class AuditReport:
    schema_version: int
    created_utc: str
    raw_dir: str
    file_glob: str
    format_hint: str
    inventory: dict[str, Any]
    files: list[FileAudit]
    overall_verdict: str
    coverage: dict[str, Any]
    permission_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["files"] = [asdict(file) for file in self.files]
        return payload


def _schema_columns(schema_type: str, has_header: bool, column_count: int) -> list[str] | None:
    if has_header:
        return None
    if schema_type == "events":
        return EVENTS_HEADER_COLUMNS[:column_count]
    if schema_type == "gkg":
        return GKG_HEADER_COLUMNS[:column_count]
    return None


def _required_fields(schema_type: str) -> list[str]:
    if schema_type == "events":
        return EVENTS_REQUIRED_FIELDS
    if schema_type == "gkg":
        return GKG_REQUIRED_FIELDS
    return []


def _schema_aliases(schema_type: str) -> dict[str, list[str]]:
    if schema_type == "events":
        from market_monitor.gdelt.utils import _EVENTS_ALIASES as aliases
    else:
        from market_monitor.gdelt.utils import _GKG_ALIASES as aliases
    return aliases


def _load_sample_frame(
    path: Path,
    *,
    delimiter: str,
    has_header: bool,
    schema_type: str,
    column_count: int,
    max_rows: int,
) -> "pd.DataFrame":
    import pandas as pd
    schema_columns = _schema_columns(schema_type, has_header, column_count)
    return pd.read_csv(
        path,
        sep=delimiter,
        header=0 if has_header else None,
        names=schema_columns,
        dtype=str,
        nrows=max_rows,
        low_memory=False,
    )


def _detect_quoting_irregularities(path: Path, delimiter: str) -> bool:
    try:
        sample_lines = []
        with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
            for _ in range(20):
                line = handle.readline()
                if not line:
                    break
                sample_lines.append(line.rstrip("\n"))
    except OSError:
        return True
    if not sample_lines:
        return False
    expected = None
    for line in sample_lines:
        if not line:
            continue
        if line.count('"') % 2 != 0:
            return True
        parts = line.split(delimiter)
        if expected is None:
            expected = len(parts)
        if expected != len(parts):
            return True
    return False


def _parse_date_stats(
    frame: "pd.DataFrame",
    schema_type: str,
    mapping: dict[str, str],
) -> DateStats:
    import pandas as pd
    if schema_type == "events":
        source = mapping.get("day")
        if source is None:
            return DateStats(None, None, 0.0)
        parsed = parse_day(frame[source])
    else:
        source = mapping.get("datetime")
        if source is None:
            return DateStats(None, None, 0.0)
        parsed = pd.to_datetime(frame[source], errors="coerce")

    parseable_rate = float(parsed.notna().mean()) if len(parsed) else 0.0
    parsed = parsed.dropna()
    if parsed.empty:
        return DateStats(None, None, parseable_rate)
    min_day = parsed.min().date().isoformat()
    max_day = parsed.max().date().isoformat()
    return DateStats(min_day, max_day, parseable_rate)


def _required_field_report(
    frame: "pd.DataFrame",
    *,
    schema_type: str,
    mapping: dict[str, str],
) -> RequiredFieldReport:
    availability: dict[str, bool] = {}
    coverage: dict[str, float] = {}
    for field in _required_fields(schema_type):
        source = mapping.get(field)
        if source is None:
            availability[field] = False
            coverage[field] = 0.0
        else:
            series = frame[source]
            available = series.notna().any()
            availability[field] = bool(available)
            coverage[field] = float(series.notna().mean()) if len(series) else 0.0
    return RequiredFieldReport(availability=availability, coverage=coverage)


def _events_capabilities(mapping: dict[str, str]) -> dict[str, bool]:
    def _has(key: str) -> bool:
        return key in mapping

    return {
        "event_code": _has("event_code"),
        "event_root_code": _has("event_root_code"),
        "goldstein_scale": _has("goldstein_scale"),
        "avg_tone": _has("avg_tone"),
        "num_mentions": _has("num_mentions"),
        "num_sources": _has("num_sources"),
        "num_articles": _has("num_articles"),
        "actor1_country_code": _has("actor1_country_code"),
        "actor2_country_code": _has("actor2_country_code"),
        "actiongeo_country_code": _has("actiongeo_country_code"),
    }


def _gkg_semicolon_fields(frame: "pd.DataFrame", mapping: dict[str, str]) -> dict[str, bool]:
    results: dict[str, bool] = {}
    for field in ["themes", "persons", "organizations", "locations"]:
        source = mapping.get(field)
        if not source or source not in frame.columns:
            results[field] = False
            continue
        series = frame[source].dropna().astype(str)
        results[field] = series.str.contains(";", regex=False).any()
    return results


def _classify_readiness(
    *,
    schema_type: str,
    dialect: DialectInfo,
    date_stats: DateStats,
    mapping: dict[str, str],
    issues: list[str],
) -> str:
    if schema_type == "unknown":
        issues.append("Schema could not be identified from headers or column count.")
        return UNUSABLE
    if date_stats.parseable_rate == 0.0:
        issues.append("No parseable dates detected in sample.")
        return UNUSABLE
    if schema_type == "events":
        if "event_code" not in mapping and "event_root_code" not in mapping:
            issues.append("Event codes missing; cannot recover event identifiers.")
            return UNUSABLE
    if not dialect.has_header:
        issues.append("Header row missing; normalization is required to map columns.")
    if dialect.delimiter != ",":
        issues.append("Non-comma delimiter detected; normalization will standardize output.")
    if dialect.quoting_irregularities:
        issues.append("Quoting irregularities detected in sample rows.")
    if date_stats.parseable_rate < 0.95:
        issues.append("Date parse rate below 95%; normalization recommended.")
    if issues:
        return NEEDS_NORMALIZATION
    return READY_STABLE


def audit_corpus(
    *,
    raw_dir: Path,
    file_glob: str,
    format_hint: str,
    max_rows: int = 2000,
) -> AuditReport:
    files = list_files(raw_dir, file_glob)
    total_size = sum(path.stat().st_size for path in files)
    permission_errors: list[str] = []
    file_audits: list[FileAudit] = []
    overall_min: str | None = None
    overall_max: str | None = None
    total_parseable = 0.0
    total_rows = 0
    row_estimated_any = False

    for path in files:
        try:
            file_spec = analyze_file(path)
        except OSError as exc:
            permission_errors.append(f"{path}: {exc}")
            continue
        rows_estimate, rows_estimated = estimate_rows(path, delimiter=file_spec.delimiter)
        row_estimated_any = row_estimated_any or rows_estimated
        schema_type = format_hint
        if format_hint == "auto":
            schema_type = detect_schema_type(file_spec.columns, file_spec.column_count)
        dialect = DialectInfo(
            delimiter=file_spec.delimiter,
            has_header=file_spec.has_header,
            quoting_irregularities=_detect_quoting_irregularities(path, file_spec.delimiter),
        )
        mapping: dict[str, str] = {}
        date_stats = DateStats(None, None, 0.0)
        required_fields = RequiredFieldReport(availability={}, coverage={})
        gkg_semicolons: dict[str, bool] = {}
        issues: list[str] = []
        try:
            if schema_type != "unknown":
                frame = _load_sample_frame(
                    path,
                    delimiter=file_spec.delimiter,
                    has_header=file_spec.has_header,
                    schema_type=schema_type,
                    column_count=file_spec.column_count,
                    max_rows=max_rows,
                )
                mapping = map_columns(frame.columns, _schema_aliases(schema_type))
                date_stats = _parse_date_stats(frame, schema_type, mapping)
                required_fields = _required_field_report(
                    frame, schema_type=schema_type, mapping=mapping
                )
                if schema_type == "events":
                    events_caps = _events_capabilities(mapping)
                    for key, value in events_caps.items():
                        required_fields.availability.setdefault(key, value)
                if schema_type == "gkg":
                    gkg_semicolons = _gkg_semicolon_fields(frame, mapping)
            else:
                issues.append("Schema detection returned unknown.")
        except PermissionError as exc:
            permission_errors.append(f"{path}: {exc}")
            issues.append("Permission error while reading sample rows.")
        except OSError as exc:
            issues.append(str(exc))

        readiness = _classify_readiness(
            schema_type=schema_type,
            dialect=dialect,
            date_stats=date_stats,
            mapping=mapping,
            issues=issues,
        )

        file_audits.append(
            FileAudit(
                path=str(path),
                size_bytes=path.stat().st_size,
                estimated_rows=rows_estimate,
                rows_estimated=rows_estimated,
                dialect=dialect,
                schema_type=schema_type,
                date_stats=date_stats,
                required_fields=required_fields,
                gkg_semicolon_fields=gkg_semicolons,
                readiness_verdict=readiness,
                issues=issues,
            )
        )

        if date_stats.min_day:
            overall_min = min(filter(None, [overall_min, date_stats.min_day]))
        if date_stats.max_day:
            overall_max = max(filter(None, [overall_max, date_stats.max_day]))
        if rows_estimate > 0:
            total_parseable += date_stats.parseable_rate * rows_estimate
            total_rows += rows_estimate

    if not files:
        overall_verdict = UNUSABLE
    else:
        verdicts = {audit.readiness_verdict for audit in file_audits}
        if UNUSABLE in verdicts or permission_errors:
            overall_verdict = UNUSABLE
        elif NEEDS_NORMALIZATION in verdicts:
            overall_verdict = NEEDS_NORMALIZATION
        else:
            overall_verdict = READY_STABLE

    coverage = {
        "min_day": overall_min,
        "max_day": overall_max,
        "parseable_rate": float(total_parseable / total_rows) if total_rows else 0.0,
    }
    inventory = {
        "matched_files": [str(path) for path in files],
        "file_count": len(files),
        "total_size_bytes": total_size,
        "estimated_rows": sum(audit.estimated_rows for audit in file_audits),
        "rows_estimated": row_estimated_any,
        "permission_error": bool(permission_errors),
    }
    return AuditReport(
        schema_version=1,
        created_utc=utc_now_iso(),
        raw_dir=str(raw_dir),
        file_glob=file_glob,
        format_hint=format_hint,
        inventory=inventory,
        files=file_audits,
        overall_verdict=overall_verdict,
        coverage=coverage,
        permission_errors=permission_errors,
    )


def warn_if_unusable(raw_dir: Path, *, file_glob: str, context: str) -> None:
    if not raw_dir.exists():
        return
    report = audit_corpus(raw_dir=raw_dir, file_glob=file_glob, format_hint="auto", max_rows=200)
    if report.overall_verdict == UNUSABLE:
        print(
            f"[{context}] Raw corpus at {raw_dir} appears unusable. "
            "Run: python -m market_monitor.gdelt.doctor audit --raw-dir \"...\""
        )


def _default_audit_path() -> Path:
    outputs_dir = Path("outputs")
    run_id = os.getenv("MARKET_APP_RUN_ID")
    if run_id:
        return outputs_dir / run_id / "gdelt_audit.json"
    return outputs_dir / "gdelt_audit.json"


def _print_summary(report: AuditReport) -> None:
    print(f"[gdelt.doctor] files matched: {report.inventory['file_count']}")
    print(f"[gdelt.doctor] total size: {report.inventory['total_size_bytes']} bytes")
    print(f"[gdelt.doctor] coverage: {report.coverage['min_day']} -> {report.coverage['max_day']}")
    print(f"[gdelt.doctor] overall verdict: {report.overall_verdict}")
    if report.permission_errors:
        print("[gdelt.doctor] permission errors detected:")
        for error in report.permission_errors:
            print(f"  - {error}")
    issues = [
        (audit.path, audit.issues)
        for audit in report.files
        if audit.readiness_verdict != READY_STABLE
    ]
    if issues:
        print("[gdelt.doctor] top remediation steps:")
        for path, audit_issues in issues:
            print(f"  - {path}:")
            if audit_issues:
                for issue in audit_issues[:3]:
                    print(f"      * {issue}")
            print("      * Run: python -m market_monitor.gdelt.doctor normalize --raw-dir \"...\" --gdelt-dir \"...\" --format events")
    else:
        print("[gdelt.doctor] No critical issues detected.")


def _write_report(report: AuditReport, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")


def normalize_corpus(
    *,
    raw_dir: Path,
    gdelt_dir: Path,
    file_glob: str,
    format_hint: str,
    write_format: str,
) -> None:
    from market_monitor.gdelt.ingest import ingest_gdelt

    ingest_gdelt(
        raw_dir=raw_dir,
        out_dir=gdelt_dir,
        file_glob=file_glob,
        format_hint=format_hint,
        write_format=write_format,
        require_files=True,
    )


def verify_cache(*, gdelt_dir: Path) -> tuple[bool, list[str]]:
    issues: list[str] = []
    events_root = gdelt_dir / "events"
    if not events_root.exists():
        issues.append(f"Missing events cache at {events_root}.")
        return False, issues
    manifest_path = events_root / "manifest.json"
    if not manifest_path.exists():
        issues.append("Missing events manifest.json; run normalize first.")
        return False, issues
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    coverage = manifest.get("coverage", {}) if isinstance(manifest, dict) else {}
    n_days = coverage.get("n_days", 0)
    if not n_days:
        issues.append("Manifest coverage indicates zero days.")
    day_dirs = sorted([path for path in events_root.glob("day=*") if path.is_dir()])
    if not day_dirs:
        issues.append("No day partitions found under events/.")
    missing_parts = []
    for day_dir in day_dirs:
        files = list(day_dir.glob("part-00000.*"))
        if not files:
            missing_parts.append(day_dir.name)
    if missing_parts:
        issues.append(f"Missing partitions for: {', '.join(missing_parts[:5])}")
    if "content_hash" not in manifest:
        issues.append("Manifest content_hash is missing.")
    return len(issues) == 0, issues


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit and normalize local GDELT corpora.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    audit_parser = subparsers.add_parser("audit", help="Audit raw GDELT corpus for readiness.")
    audit_parser.add_argument("--raw-dir", required=True, help="Raw corpus root directory.")
    audit_parser.add_argument("--glob", default="*.csv", help="File glob pattern.")
    audit_parser.add_argument("--format", default="auto", choices=["auto", "events", "gkg"])
    audit_parser.add_argument("--out", help="Output JSON report path.")
    audit_parser.add_argument("--strict", action="store_true", help="Exit nonzero if not READY.")

    normalize_parser = subparsers.add_parser("normalize", help="Normalize raw corpus into cache.")
    normalize_parser.add_argument("--raw-dir", required=True, help="Raw corpus root directory.")
    normalize_parser.add_argument("--gdelt-dir", required=True, help="Normalized cache directory.")
    normalize_parser.add_argument("--format", default="events", choices=["events", "gkg"])
    normalize_parser.add_argument("--glob", default="*.csv", help="File glob pattern.")
    normalize_parser.add_argument("--write", default="csv", choices=["csv", "parquet"])

    verify_parser = subparsers.add_parser("verify-cache", help="Verify normalized cache integrity.")
    verify_parser.add_argument("--gdelt-dir", required=True, help="Normalized cache directory.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "audit":
        raw_dir = Path(args.raw_dir).expanduser()
        report = audit_corpus(raw_dir=raw_dir, file_glob=args.glob, format_hint=args.format)
        out_path = Path(args.out).expanduser() if args.out else _default_audit_path()
        _write_report(report, out_path)
        _print_summary(report)
        print(f"[gdelt.doctor] report: {out_path}")
        if args.strict and report.overall_verdict != READY_STABLE:
            return 2
        return 0

    if args.command == "normalize":
        normalize_corpus(
            raw_dir=Path(args.raw_dir).expanduser(),
            gdelt_dir=Path(args.gdelt_dir).expanduser(),
            file_glob=args.glob,
            format_hint=args.format,
            write_format=args.write,
        )
        print("[gdelt.doctor] normalization complete.")
        return 0

    if args.command == "verify-cache":
        ok, issues = verify_cache(gdelt_dir=Path(args.gdelt_dir).expanduser())
        if ok:
            print("[gdelt.doctor] cache verified.")
            return 0
        print("[gdelt.doctor] cache verification failed:")
        for issue in issues:
            print(f"  - {issue}")
        return 2

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
