from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    import pandas as pd

from market_monitor.gdelt.utils import (
    EVENTS_HEADER_COLUMNS,
    EVENTS_REQUIRED_FIELDS,
    GKG_HEADER_COLUMNS,
    GKG_REQUIRED_FIELDS,
    analyze_file,
    build_content_hash,
    build_file_fingerprint,
    detect_schema_type,
    estimate_rows,
    ensure_dir,
    list_files,
    map_columns,
    normalize_columns,
    parse_day,
)
from market_monitor.time_utils import utc_now_iso


READY_STABLE = "READY_STABLE"
NEEDS_NORMALIZATION = "NEEDS_NORMALIZATION"
UNUSABLE = "UNUSABLE"

EVENTS_RAW = "events_raw"
DAILY_FEATURES_PRECOMPUTED = "daily_features_precomputed"
ANNUAL_AGGREGATES = "annual_aggregates"
UNKNOWN = "unknown"


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
class DateCandidate:
    column: str
    parseable_rate: float
    min_day: str | None
    max_day: str | None
    inferred_frequency: str


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
    file_type: str
    candidate_date_columns: list[DateCandidate]
    inferred_frequency: str
    columns: list[str]
    inferred_dtypes: dict[str, str]
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


def _infer_frequency(parsed: "pd.Series", original: "pd.Series") -> str:
    import pandas as pd

    parsed = parsed.dropna()
    if parsed.empty:
        return "unknown"
    if original.dtype.kind in {"i", "u"}:
        years = original.dropna().astype(int)
        if not years.empty and years.between(1800, 2300).all():
            return "annual"
    dates = parsed.dt.normalize().sort_values().unique()
    if len(dates) < 2:
        return "unknown"
    deltas = (dates[1:] - dates[:-1]) / pd.Timedelta(days=1)
    if (deltas >= 360).all():
        return "annual"
    if (deltas >= 28).all() and (deltas <= 31).all():
        return "monthly"
    return "daily"


def _evaluate_date_candidates(frame: "pd.DataFrame") -> list[DateCandidate]:
    import pandas as pd

    candidates = ["dt", "day", "date", "sqldate", "datetime", "year"]
    normalized = normalize_columns(frame.columns)
    name_map = {name: frame.columns[idx] for idx, name in enumerate(normalized)}
    results: list[DateCandidate] = []
    for candidate in candidates:
        if candidate not in name_map:
            continue
        column = name_map[candidate]
        series = frame[column]
        if series.dtype.kind in {"i", "u", "f"}:
            parsed = parse_day(series)
        else:
            parsed = pd.to_datetime(series, errors="coerce")
            if parsed.notna().sum() == 0:
                parsed = parse_day(series)
        parseable_rate = float(parsed.notna().mean()) if len(parsed) else 0.0
        parsed = parsed.dropna()
        min_day = parsed.min().date().isoformat() if not parsed.empty else None
        max_day = parsed.max().date().isoformat() if not parsed.empty else None
        frequency = _infer_frequency(parsed, series)
        results.append(
            DateCandidate(
                column=column,
                parseable_rate=parseable_rate,
                min_day=min_day,
                max_day=max_day,
                inferred_frequency=frequency,
            )
        )
    return results


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


def _best_candidate(candidates: list[DateCandidate]) -> DateCandidate | None:
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.parseable_rate)


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
    file_type: str,
    dialect: DialectInfo,
    date_stats: DateStats,
    mapping: dict[str, str],
    issues: list[str],
) -> str:
    if file_type == UNKNOWN:
        issues.append("Schema could not be identified from headers or column count.")
        return UNUSABLE
    if file_type == ANNUAL_AGGREGATES:
        issues.append(
            "Annual aggregates are not usable for daily joins. "
            "Re-run with --allow-annual to normalize separately."
        )
        return UNUSABLE
    if date_stats.parseable_rate == 0.0:
        issues.append("No parseable dates detected in sample.")
        return UNUSABLE
    if schema_type == "events":
        if "event_code" not in mapping and "event_root_code" not in mapping:
            issues.append("Event codes missing; cannot recover event identifiers.")
            return UNUSABLE
    if file_type == DAILY_FEATURES_PRECOMPUTED:
        issues.append("Precomputed daily features require normalization.")
        return NEEDS_NORMALIZATION
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
        candidate_date_columns: list[DateCandidate] = []
        inferred_frequency = "unknown"
        columns: list[str] = []
        inferred_dtypes: dict[str, str] = {}
        file_type = UNKNOWN
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
                columns = list(frame.columns)
                inferred_dtypes = {col: str(dtype) for col, dtype in frame.dtypes.items()}
                candidate_date_columns = _evaluate_date_candidates(frame)
                mapping = map_columns(frame.columns, _schema_aliases(schema_type))
                date_stats = _parse_date_stats(frame, schema_type, mapping)
                best_candidate = _best_candidate(candidate_date_columns)
                if best_candidate:
                    inferred_frequency = best_candidate.inferred_frequency
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
                if file_spec.has_header:
                    import pandas as pd

                    frame = pd.read_csv(
                        path,
                        sep=file_spec.delimiter,
                        header=0,
                        dtype=str,
                        nrows=max_rows,
                        low_memory=False,
                    )
                    columns = list(frame.columns)
                    inferred_dtypes = {col: str(dtype) for col, dtype in frame.dtypes.items()}
                    candidate_date_columns = _evaluate_date_candidates(frame)
                    best_candidate = _best_candidate(candidate_date_columns)
                    if best_candidate:
                        inferred_frequency = best_candidate.inferred_frequency
                        date_stats = DateStats(
                            min_day=best_candidate.min_day,
                            max_day=best_candidate.max_day,
                            parseable_rate=best_candidate.parseable_rate,
                        )
                issues.append("Schema detection returned unknown.")
        except PermissionError as exc:
            permission_errors.append(f"{path}: {exc}")
            issues.append("Permission error while reading sample rows.")
        except OSError as exc:
            issues.append(str(exc))

        if schema_type in {"events", "gkg"}:
            file_type = EVENTS_RAW
        elif candidate_date_columns:
            best_candidate = _best_candidate(candidate_date_columns)
            if best_candidate and best_candidate.inferred_frequency == "annual":
                file_type = ANNUAL_AGGREGATES
            elif best_candidate and best_candidate.inferred_frequency == "daily":
                file_type = DAILY_FEATURES_PRECOMPUTED
            else:
                file_type = UNKNOWN

        readiness = _classify_readiness(
            schema_type=schema_type,
            file_type=file_type,
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
                file_type=file_type,
                candidate_date_columns=candidate_date_columns,
                inferred_frequency=inferred_frequency,
                columns=columns,
                inferred_dtypes=inferred_dtypes,
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
    by_type: dict[str, int] = {}
    for audit in report.files:
        by_type[audit.file_type] = by_type.get(audit.file_type, 0) + 1
    if by_type:
        print(f"[gdelt.doctor] file type counts: {by_type}")
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
    with out_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(report.to_dict(), indent=2))


def _resolve_date_column(columns: list[str], date_col: str | None) -> str | None:
    if not columns:
        return None
    if date_col:
        lowered = {col.lower(): col for col in columns}
        return lowered.get(date_col.lower())
    normalized = normalize_columns(columns)
    for candidate in ["dt", "day", "date", "sqldate", "datetime"]:
        if candidate in normalized:
            return columns[normalized.index(candidate)]
    return None


def _normalize_daily_frame(
    frame: "pd.DataFrame",
    *,
    date_column: str,
    issues: list[str],
) -> "pd.DataFrame":
    import pandas as pd

    parsed = pd.to_datetime(frame[date_column], errors="coerce")
    if parsed.notna().sum() == 0:
        parsed = parse_day(frame[date_column])
    if parsed.notna().sum() == 0:
        raise ValueError(f"Date column '{date_column}' could not be parsed.")
    normalized = frame.copy()
    normalized["day"] = parsed.dt.strftime("%Y-%m-%d")
    normalized = normalized.drop(columns=[date_column])
    if normalized["day"].isna().any():
        issues.append("Dropped rows with unparseable dates.")
        normalized = normalized[normalized["day"].notna()]

    numeric_cols: list[str] = []
    for col in normalized.columns:
        if col == "day":
            continue
        series = pd.to_numeric(normalized[col], errors="coerce")
        if series.notna().any():
            normalized[col] = series
            numeric_cols.append(col)
        else:
            normalized[col] = normalized[col].astype(str)

    if normalized["day"].duplicated().any():
        issues.append("Duplicate days detected; aggregated with mean (numeric) and first (non-numeric).")

    agg: dict[str, str] = {}
    for col in normalized.columns:
        if col == "day":
            continue
        agg[col] = "mean" if col in numeric_cols else "first"

    grouped = normalized.groupby("day", as_index=False).agg(agg)
    return _canonicalize_daily_frame(grouped)


def _aggregate_by_day(frame: "pd.DataFrame") -> "pd.DataFrame":
    import pandas as pd

    numeric_cols = [
        col for col in frame.columns if col != "day" and pd.api.types.is_numeric_dtype(frame[col])
    ]
    agg: dict[str, str] = {}
    for col in frame.columns:
        if col == "day":
            continue
        agg[col] = "mean" if col in numeric_cols else "first"
    grouped = frame.groupby("day", as_index=False).agg(agg)
    return _canonicalize_daily_frame(grouped)


def _canonicalize_daily_frame(frame: "pd.DataFrame") -> "pd.DataFrame":
    ordered_cols = ["day"] + sorted([col for col in frame.columns if col != "day"])
    return frame.sort_values("day")[ordered_cols].reset_index(drop=True)


def _stream_precomputed_daily(
    *,
    path: Path,
    delimiter: str,
    date_column: str,
    chunksize: int,
    issues: list[str],
) -> "pd.DataFrame":
    import pandas as pd

    def _iter_chunks() -> Iterable[pd.DataFrame]:
        return pd.read_csv(
            path,
            sep=delimiter,
            header=0,
            dtype=str,
            chunksize=chunksize,
            low_memory=False,
        )

    header = pd.read_csv(path, sep=delimiter, header=0, dtype=str, nrows=0, low_memory=False)
    numeric_columns: set[str] = set()
    for chunk in _iter_chunks():
        for col in chunk.columns:
            if col == date_column:
                continue
            series = pd.to_numeric(chunk[col], errors="coerce")
            if series.notna().any():
                numeric_columns.add(col)

    sum_by_day: dict[str, dict[str, float]] = {}
    count_by_day: dict[str, dict[str, int]] = {}
    first_by_day: dict[str, dict[str, str]] = {}
    row_counts: dict[str, int] = {}

    for chunk in _iter_chunks():
        parsed = pd.to_datetime(chunk[date_column], errors="coerce")
        if parsed.notna().sum() == 0:
            parsed = parse_day(chunk[date_column])
        if parsed.notna().sum() == 0:
            raise ValueError(f"Date column '{date_column}' could not be parsed.")
        chunk = chunk.copy()
        chunk["day"] = parsed.dt.strftime("%Y-%m-%d")
        chunk = chunk.drop(columns=[date_column])
        chunk = chunk[chunk["day"].notna()]
        if chunk.empty:
            continue
        grouped_counts = chunk.groupby("day", as_index=False).size()
        for _, row in grouped_counts.iterrows():
            day_value = row["day"]
            row_counts[day_value] = row_counts.get(day_value, 0) + int(row["size"])

        non_numeric_cols = [col for col in chunk.columns if col not in {"day", *numeric_columns}]

        for col in numeric_columns:
            series = pd.to_numeric(chunk[col], errors="coerce")
            sums = series.groupby(chunk["day"]).sum(min_count=1)
            counts = series.groupby(chunk["day"]).count()
            for day_value, total in sums.items():
                if pd.isna(total):
                    continue
                sum_by_day.setdefault(day_value, {})
                sum_by_day[day_value][col] = sum_by_day[day_value].get(col, 0.0) + float(total)
            for day_value, count in counts.items():
                count_by_day.setdefault(day_value, {})
                count_by_day[day_value][col] = count_by_day[day_value].get(col, 0) + int(count)

        for col in non_numeric_cols:
            series = chunk[col].dropna().astype(str)
            if series.empty:
                continue
            firsts = series.groupby(chunk["day"]).first()
            for day_value, value in firsts.items():
                if value is None:
                    continue
                first_by_day.setdefault(day_value, {})
                first_by_day[day_value].setdefault(col, value)

    if any(count > 1 for count in row_counts.values()):
        issues.append("Duplicate days detected; aggregated with mean (numeric) and first (non-numeric).")

    rows: list[dict[str, object]] = []
    all_columns = [col for col in header.columns if col != date_column]
    for day_value in sorted(row_counts.keys()):
        row: dict[str, object] = {"day": day_value}
        for col in all_columns:
            if col in numeric_columns:
                count = count_by_day.get(day_value, {}).get(col, 0)
                if count:
                    row[col] = sum_by_day.get(day_value, {}).get(col, 0.0) / count
                else:
                    row[col] = float("nan")
            else:
                row[col] = first_by_day.get(day_value, {}).get(col)
        rows.append(row)

    frame = pd.DataFrame(rows)
    return _canonicalize_daily_frame(frame)


def _write_partitioned(
    frame: "pd.DataFrame",
    *,
    out_dir: Path,
    write_format: str,
) -> dict[str, int]:
    import importlib.util

    ensure_dir(out_dir)
    output_ext = ".parquet" if write_format == "parquet" else ".csv"
    if output_ext == ".parquet" and importlib.util.find_spec("pyarrow") is None:
        raise ImportError("pyarrow is required for parquet output. Use --write csv instead.")
    rows_per_day: dict[str, int] = {}
    for day in sorted(frame["day"].unique()):
        day_dir = out_dir / f"day={day}"
        ensure_dir(day_dir)
        day_frame = frame[frame["day"] == day]
        out_path = day_dir / f"part-00000{output_ext}"
        if output_ext == ".parquet":
            day_frame.to_parquet(out_path, index=False)
        else:
            day_frame.to_csv(out_path, index=False, lineterminator="\n")
        rows_per_day[day] = int(len(day_frame))
    return rows_per_day


def _normalize_precomputed_daily(
    *,
    file_paths: list[Path],
    gdelt_dir: Path,
    date_col: str | None,
    write_format: str,
    classification_summary: dict[str, Any],
    streaming_threshold_bytes: int = 25_000_000,
    streaming_chunk_rows: int = 50_000,
) -> dict[str, Any]:
    import pandas as pd

    if not file_paths:
        raise ValueError("No daily features files available for normalization.")
    all_frames: list[pd.DataFrame] = []
    issues: list[str] = []
    for path in file_paths:
        file_spec = analyze_file(path)
        if not file_spec.has_header:
            raise ValueError(f"{path} is missing headers; cannot normalize daily features.")
        if path.stat().st_size >= streaming_threshold_bytes:
            header = pd.read_csv(
                path,
                sep=file_spec.delimiter,
                header=0,
                dtype=str,
                nrows=0,
                low_memory=False,
            )
            date_column = _resolve_date_column(list(header.columns), date_col)
            if not date_column:
                raise ValueError(f"{path} missing a recognizable date column; use --date-col to map.")
            normalized = _stream_precomputed_daily(
                path=path,
                delimiter=file_spec.delimiter,
                date_column=date_column,
                chunksize=streaming_chunk_rows,
                issues=issues,
            )
        else:
            frame = pd.read_csv(
                path,
                sep=file_spec.delimiter,
                header=0,
                dtype=str,
                low_memory=False,
            )
            date_column = _resolve_date_column(list(frame.columns), date_col)
            if not date_column:
                raise ValueError(f"{path} missing a recognizable date column; use --date-col to map.")
            normalized = _normalize_daily_frame(frame, date_column=date_column, issues=issues)
        all_frames.append(normalized)

    combined = pd.concat(all_frames, ignore_index=True, sort=False)
    if combined["day"].duplicated().any():
        issues.append("Duplicate days across files detected; aggregated with mean/first.")
        combined = _aggregate_by_day(combined)
    combined = _canonicalize_daily_frame(combined)
    daily_root = gdelt_dir / "daily_features"
    rows_per_day = _write_partitioned(combined, out_dir=daily_root, write_format=write_format)
    manifest_path = daily_root / "features_manifest.json"
    columns = list(combined.columns)
    inferred_dtypes = {col: str(dtype) for col, dtype in combined.dtypes.items()}
    manifest_payload = {
        "schema_version": 1,
        "created_utc": utc_now_iso(),
        "coverage": {
            "min_day": combined["day"].min(),
            "max_day": combined["day"].max(),
            "n_days": int(combined["day"].nunique()),
        },
        "row_counts": {
            "total_rows": int(len(combined)),
            "rows_per_day": rows_per_day,
        },
        "schema": {
            "columns": columns,
            "dtypes": inferred_dtypes,
        },
        "inputs": {
            "raw_files": build_file_fingerprint(file_paths),
        },
        "config": {
            "date_col": date_col,
            "write_format": write_format,
            "aggregation": {
                "numeric": "mean",
                "non_numeric": "first",
            },
        },
        "classification_summary": classification_summary,
        "issues": issues,
    }
    manifest_payload["content_hash"] = build_content_hash(manifest_payload)
    with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(manifest_payload, indent=2))
    return {
        "daily_root": daily_root,
        "manifest_path": manifest_path,
        "issues": issues,
    }


def _normalize_annual_features(
    *,
    file_paths: list[Path],
    gdelt_dir: Path,
    write_format: str,
    classification_summary: dict[str, Any],
) -> dict[str, Any]:
    import pandas as pd

    if not file_paths:
        return {}
    frames: list[pd.DataFrame] = []
    issues: list[str] = []
    for path in file_paths:
        file_spec = analyze_file(path)
        frame = pd.read_csv(
            path,
            sep=file_spec.delimiter,
            header=0 if file_spec.has_header else None,
            dtype=str,
            low_memory=False,
        )
        if not file_spec.has_header:
            raise ValueError(f"{path} is missing headers; cannot normalize annual features.")
        normalized_names = normalize_columns(frame.columns)
        if "year" not in normalized_names:
            raise ValueError(f"{path} missing a Year column; cannot normalize annual features.")
        year_col = frame.columns[normalized_names.index("year")]
        frame = frame.rename(columns={year_col: "year"})
        frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype("Int64")
        frame = frame.dropna(subset=["year"])
        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.sort_values("year")
    annual_root = gdelt_dir / "annual_features"
    ensure_dir(annual_root)
    output_ext = ".parquet" if write_format == "parquet" else ".csv"
    if output_ext == ".parquet":
        import importlib.util

        if importlib.util.find_spec("pyarrow") is None:
            raise ImportError("pyarrow is required for parquet output. Use --write csv instead.")
    rows_per_year: dict[str, int] = {}
    for year in sorted(combined["year"].dropna().unique()):
        year_dir = annual_root / f"year={int(year)}"
        ensure_dir(year_dir)
        year_frame = combined[combined["year"] == year]
        out_path = year_dir / f"part-00000{output_ext}"
        if output_ext == ".parquet":
            year_frame.to_parquet(out_path, index=False)
        else:
            year_frame.to_csv(out_path, index=False, lineterminator="\n")
        rows_per_year[str(int(year))] = int(len(year_frame))

    manifest_path = annual_root / "annual_features_manifest.json"
    manifest_payload = {
        "schema_version": 1,
        "created_utc": utc_now_iso(),
        "coverage": {
            "min_year": int(combined["year"].min()),
            "max_year": int(combined["year"].max()),
            "n_years": int(combined["year"].nunique()),
        },
        "row_counts": {
            "total_rows": int(len(combined)),
            "rows_per_year": rows_per_year,
        },
        "schema": {
            "columns": list(combined.columns),
            "dtypes": {col: str(dtype) for col, dtype in combined.dtypes.items()},
        },
        "inputs": {
            "raw_files": build_file_fingerprint(file_paths),
        },
        "config": {
            "write_format": write_format,
        },
        "classification_summary": classification_summary,
        "issues": issues,
    }
    manifest_payload["content_hash"] = build_content_hash(manifest_payload)
    with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(manifest_payload, indent=2))
    return {
        "annual_root": annual_root,
        "manifest_path": manifest_path,
        "issues": issues,
    }


def normalize_corpus(
    *,
    raw_dir: Path,
    gdelt_dir: Path,
    file_glob: str,
    format_hint: str,
    write_format: str,
    date_col: str | None = None,
    allow_annual: bool = False,
) -> None:
    from market_monitor.gdelt.ingest import ingest_gdelt

    if format_hint in {"events", "gkg"}:
        ingest_gdelt(
            raw_dir=raw_dir,
            out_dir=gdelt_dir,
            file_glob=file_glob,
            format_hint=format_hint,
            write_format=write_format,
            require_files=True,
        )
        return

    report = audit_corpus(raw_dir=raw_dir, file_glob=file_glob, format_hint="auto")
    daily_files = [
        Path(item.path) for item in report.files if item.file_type == DAILY_FEATURES_PRECOMPUTED
    ]
    annual_files = [Path(item.path) for item in report.files if item.file_type == ANNUAL_AGGREGATES]
    events_files = [Path(item.path) for item in report.files if item.file_type == EVENTS_RAW]
    unknown_files = [Path(item.path) for item in report.files if item.file_type == UNKNOWN]
    classification_summary = {
        DAILY_FEATURES_PRECOMPUTED: [str(path) for path in daily_files],
        ANNUAL_AGGREGATES: [str(path) for path in annual_files],
        EVENTS_RAW: [str(path) for path in events_files],
        UNKNOWN: [str(path) for path in unknown_files],
    }

    if daily_files:
        result = _normalize_precomputed_daily(
            file_paths=daily_files,
            gdelt_dir=gdelt_dir,
            date_col=date_col,
            write_format=write_format,
            classification_summary=classification_summary,
        )
        print(f"[gdelt.doctor] wrote daily features cache: {result['daily_root']}")
        print(f"[gdelt.doctor] daily features manifest: {result['manifest_path']}")
        for issue in result["issues"]:
            print(f"[gdelt.doctor] warning: {issue}")

    if annual_files:
        if allow_annual:
            annual_result = _normalize_annual_features(
                file_paths=annual_files,
                gdelt_dir=gdelt_dir,
                write_format=write_format,
                classification_summary=classification_summary,
            )
            if annual_result:
                print(
                    "[gdelt.doctor] wrote annual features cache: "
                    f"{annual_result['annual_root']}"
                )
                print(
                    "[gdelt.doctor] annual features manifest: "
                    f"{annual_result['manifest_path']}"
                )
                for issue in annual_result["issues"]:
                    print(f"[gdelt.doctor] warning: {issue}")
        else:
            print(
                "[gdelt.doctor] annual aggregates detected and excluded. "
                "Re-run with --allow-annual to normalize separately."
            )

    if events_files and not daily_files:
        ingest_gdelt(
            raw_dir=raw_dir,
            out_dir=gdelt_dir,
            file_glob=file_glob,
            format_hint="events",
            write_format=write_format,
            require_files=True,
        )
        return

    if not daily_files and not events_files:
        if annual_files and not allow_annual:
            raise ValueError(
                "Annual aggregates were detected but excluded. "
                "Re-run with --allow-annual to normalize annual_features."
            )
        raise ValueError(
            "No daily features or events files were normalized. "
            "Check the audit report for classification details."
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
    normalize_parser.add_argument("--format", default="events", choices=["auto", "events", "gkg"])
    normalize_parser.add_argument("--glob", default="*.csv", help="File glob pattern.")
    normalize_parser.add_argument("--write", default="csv", choices=["csv", "parquet"])
    normalize_parser.add_argument(
        "--date-col",
        default=None,
        help="Date column name for precomputed daily features (auto-detected if omitted).",
    )
    normalize_parser.add_argument(
        "--allow-annual",
        action="store_true",
        help="Normalize annual aggregates into a separate annual_features cache.",
    )

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
            date_col=args.date_col,
            allow_annual=args.allow_annual,
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
