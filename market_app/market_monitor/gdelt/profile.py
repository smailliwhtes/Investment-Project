from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from market_monitor.gdelt.utils import (
    EVENTS_HEADER_COLUMNS,
    EVENTS_REQUIRED_FIELDS,
    GKG_HEADER_COLUMNS,
    GKG_REQUIRED_FIELDS,
    analyze_file,
    detect_schema_type,
    estimate_rows,
    format_missing_rate,
    list_files,
    map_columns,
    parse_day,
    utc_now_iso,
)
from market_monitor.gdelt.doctor import warn_if_unusable


@dataclass
class ProfileSummary:
    files: list[str]
    rows: int
    rows_estimated: bool
    delimiter: str
    has_header: bool
    schema_type: str
    min_day: str | None
    max_day: str | None
    unique_days: int
    gap_summary: list[dict[str, Any]]
    required_field_missing: dict[str, float]
    required_field_present: dict[str, bool]


def _resolve_schema_columns(
    schema_type: str,
    has_header: bool,
    column_count: int,
) -> list[str] | None:
    if has_header:
        return None
    if schema_type == "events":
        return EVENTS_HEADER_COLUMNS[:column_count]
    if schema_type == "gkg":
        return GKG_HEADER_COLUMNS[:column_count]
    return None


def _load_required_fields(schema_type: str) -> list[str]:
    if schema_type == "events":
        return EVENTS_REQUIRED_FIELDS
    if schema_type == "gkg":
        return GKG_REQUIRED_FIELDS
    return []


def _get_required_mapping(schema_type: str, columns: Iterable[str]) -> dict[str, str]:
    if schema_type == "events":
        from market_monitor.gdelt.utils import _EVENTS_ALIASES as aliases
    else:
        from market_monitor.gdelt.utils import _GKG_ALIASES as aliases
    return map_columns(columns, aliases)


def _summarize_gaps(days: list[pd.Timestamp]) -> list[dict[str, Any]]:
    if not days:
        return []
    days_sorted = sorted(days)
    gaps = []
    for prev, current in zip(days_sorted, days_sorted[1:]):
        gap_days = (current.date() - prev.date()).days - 1
        if gap_days > 0:
            gaps.append(
                {
                    "start": (prev.date() + pd.Timedelta(days=1)).isoformat(),
                    "end": (current.date() - pd.Timedelta(days=1)).isoformat(),
                    "length_days": gap_days,
                }
            )
    gaps.sort(key=lambda item: item["length_days"], reverse=True)
    return gaps[:5]


def profile_gdelt(
    *,
    raw_dir: Path,
    file_glob: str,
    format_hint: str,
    max_rows: int = 200_000,
) -> ProfileSummary | None:
    files = list_files(raw_dir, file_glob)
    if not files:
        return None

    file_spec = analyze_file(files[0])
    delimiter = file_spec.delimiter
    has_header = file_spec.has_header
    columns = file_spec.columns
    column_count = file_spec.column_count
    schema_type = format_hint if format_hint != "auto" else detect_schema_type(columns, column_count)

    total_rows = 0
    estimated = False
    day_values: list[pd.Timestamp] = []
    missing_counts = {field: 0 for field in _load_required_fields(schema_type)}
    present_fields = {field: False for field in _load_required_fields(schema_type)}
    rows_seen = 0

    for path in files:
        file_spec = analyze_file(path)
        delimiter = file_spec.delimiter
        has_header = file_spec.has_header
        file_columns = file_spec.columns
        column_count = file_spec.column_count
        file_schema = schema_type
        if format_hint == "auto":
            file_schema = detect_schema_type(file_columns, column_count)
        if file_schema == "unknown":
            raise ValueError(
                f"Unable to detect schema for {path}. Provide --format events|gkg and confirm headers."
            )

        rows, rows_estimated = estimate_rows(path, delimiter=delimiter)
        total_rows += rows
        estimated = estimated or rows_estimated

        schema_columns = _resolve_schema_columns(file_schema, has_header, column_count)
        required_fields = _load_required_fields(file_schema)

        try:
            reader = pd.read_csv(
                path,
                sep=delimiter,
                header=0 if has_header else None,
                names=schema_columns,
                dtype=str,
                chunksize=50_000,
                low_memory=False,
            )
        except OSError as exc:
            raise OSError(
                f"Failed to read {path}. Ensure the file is readable and not locked by sync tools (e.g., OneDrive)."
            ) from exc

        for chunk in reader:
            rows_seen += len(chunk)
            mapped = _get_required_mapping(file_schema, chunk.columns)
            for field in required_fields:
                source = mapped.get(field)
                if source:
                    present_fields[field] = True
                    missing_counts[field] += chunk[source].isna().sum()
                else:
                    missing_counts[field] += len(chunk)

            if file_schema == "events":
                day_source = mapped.get("day")
                if day_source:
                    parsed = parse_day(chunk[day_source])
                    day_values.extend(parsed.dropna().unique().tolist())
            else:
                day_source = mapped.get("datetime")
                if day_source:
                    parsed = pd.to_datetime(chunk[day_source], errors="coerce")
                    day_values.extend(parsed.dropna().unique().tolist())
            if rows_seen >= max_rows:
                break
        if rows_seen >= max_rows:
            break

    days_unique = sorted({day.normalize() for day in day_values if not pd.isna(day)})
    min_day = days_unique[0].date().isoformat() if days_unique else None
    max_day = days_unique[-1].date().isoformat() if days_unique else None
    gap_summary = _summarize_gaps(days_unique)

    missing_rates = {
        field: format_missing_rate(rows_seen, missing)
        for field, missing in missing_counts.items()
    }

    return ProfileSummary(
        files=[str(path) for path in files],
        rows=total_rows,
        rows_estimated=estimated,
        delimiter=delimiter,
        has_header=has_header,
        schema_type=schema_type,
        min_day=min_day,
        max_day=max_day,
        unique_days=len(days_unique),
        gap_summary=gap_summary,
        required_field_missing=missing_rates,
        required_field_present=present_fields,
    )


def _print_summary(summary: ProfileSummary) -> None:
    print(f"[gdelt.profile] files matched: {len(summary.files)}")
    print(f"[gdelt.profile] rows: {summary.rows}{' (estimated)' if summary.rows_estimated else ''}")
    print(f"[gdelt.profile] delimiter: {repr(summary.delimiter)} header: {summary.has_header}")
    print(f"[gdelt.profile] schema: {summary.schema_type}")
    print(f"[gdelt.profile] date range: {summary.min_day} -> {summary.max_day}")
    print(f"[gdelt.profile] unique days: {summary.unique_days}")
    if summary.gap_summary:
        print("[gdelt.profile] top gaps:")
        for gap in summary.gap_summary:
            print(
                f"  - {gap['start']} to {gap['end']} ({gap['length_days']} days)"
            )
    else:
        print("[gdelt.profile] top gaps: none")
    print("[gdelt.profile] required field missing rates:")
    for field, rate in summary.required_field_missing.items():
        present = summary.required_field_present.get(field, False)
        status = "present" if present else "missing"
        print(f"  - {field}: {rate:.2%} ({status})")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile local GDELT CSVs for coverage and schema.")
    parser.add_argument("--raw-dir", required=True, help="Directory containing raw GDELT CSVs.")
    parser.add_argument("--glob", default="*.csv", help="File glob pattern.")
    parser.add_argument("--format", default="auto", choices=["auto", "events", "gkg"])
    parser.add_argument("--json-out", help="Optional JSON output path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    raw_dir = Path(args.raw_dir).expanduser()
    env_raw = os.getenv("MARKET_APP_GDELT_RAW_DIR") or os.getenv("MARKET_APP_GDELT_EVENTS_RAW_DIR")
    if env_raw:
        warn_if_unusable(Path(env_raw).expanduser(), file_glob="*.csv", context="gdelt.profile")
    summary = profile_gdelt(raw_dir=raw_dir, file_glob=args.glob, format_hint=args.format)
    if summary is None:
        print("[gdelt.profile] no files matched.")
        return 0
    _print_summary(summary)
    if args.json_out:
        payload = asdict(summary)
        payload["created_utc"] = utc_now_iso()
        Path(args.json_out).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
