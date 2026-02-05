from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from market_monitor.gdelt.utils import (
    EVENTS_CANONICAL_COLUMNS,
    EVENTS_HEADER_COLUMNS,
    GKG_CANONICAL_COLUMNS,
    GKG_HEADER_COLUMNS,
    analyze_file,
    build_content_hash,
    build_file_fingerprint,
    coerce_numeric,
    detect_schema_type,
    ensure_dir,
    list_files,
    map_columns,
    normalize_event_root_code,
    parse_day,
)
from market_monitor.gdelt.doctor import warn_if_unusable
from market_monitor.time_utils import utc_now_iso


@dataclass
class IngestResult:
    manifest_path: Path
    rows_total: int
    rows_per_day: dict[str, int]
    min_day: str | None
    max_day: str | None


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


def _canonical_columns(schema_type: str) -> list[str]:
    if schema_type == "events":
        return EVENTS_CANONICAL_COLUMNS
    if schema_type == "gkg":
        return GKG_CANONICAL_COLUMNS
    return []


def _canonical_dtypes(schema_type: str) -> dict[str, str]:
    if schema_type == "events":
        return {
            "day": "string",
            "event_id": "string",
            "event_code": "string",
            "event_base_code": "string",
            "event_root_code": "string",
            "quad_class": "string",
            "goldstein_scale": "float",
            "avg_tone": "float",
            "num_mentions": "float",
            "num_sources": "float",
            "num_articles": "float",
            "actor1_country_code": "string",
            "actor2_country_code": "string",
            "actiongeo_country_code": "string",
            "source_url": "string",
        }
    if schema_type == "gkg":
        return {
            "datetime": "string",
            "document_identifier": "string",
            "themes": "string",
            "persons": "string",
            "organizations": "string",
            "locations": "string",
            "tone": "string",
        }
    return {}
def _schema_aliases(schema_type: str) -> dict[str, list[str]]:
    if schema_type == "events":
        from market_monitor.gdelt.utils import _EVENTS_ALIASES as aliases
        return aliases
    from market_monitor.gdelt.utils import _GKG_ALIASES as aliases
    return aliases


def _normalize_events(chunk: pd.DataFrame, *, file_path: Path, chunk_index: int) -> pd.DataFrame:
    mapping = map_columns(chunk.columns, _schema_aliases("events"))
    if "day" not in mapping:
        sample = chunk.head(1).to_dict(orient="records")
        raise ValueError(
            "Missing required day/date column in events data. "
            f"File: {file_path}. Row sample: {sample}. "
            "Remediation: ensure the file has SQLDATE/day/date columns or pass --format events with headers."
        )
    df = pd.DataFrame()
    df["day"] = parse_day(chunk[mapping["day"]]) if "day" in mapping else pd.NaT
    df["event_id"] = chunk[mapping["event_id"]] if "event_id" in mapping else pd.NA
    df["event_code"] = chunk[mapping["event_code"]] if "event_code" in mapping else pd.NA
    df["event_base_code"] = (
        chunk[mapping["event_base_code"]] if "event_base_code" in mapping else pd.NA
    )
    df["event_root_code"] = (
        chunk[mapping["event_root_code"]] if "event_root_code" in mapping else pd.NA
    )
    df["quad_class"] = chunk[mapping["quad_class"]] if "quad_class" in mapping else pd.NA
    df["goldstein_scale"] = (
        coerce_numeric(chunk[mapping["goldstein_scale"]])
        if "goldstein_scale" in mapping
        else pd.NA
    )
    df["avg_tone"] = coerce_numeric(chunk[mapping["avg_tone"]]) if "avg_tone" in mapping else pd.NA
    df["num_mentions"] = (
        coerce_numeric(chunk[mapping["num_mentions"]]) if "num_mentions" in mapping else pd.NA
    )
    df["num_sources"] = (
        coerce_numeric(chunk[mapping["num_sources"]]) if "num_sources" in mapping else pd.NA
    )
    df["num_articles"] = (
        coerce_numeric(chunk[mapping["num_articles"]]) if "num_articles" in mapping else pd.NA
    )
    df["actor1_country_code"] = (
        chunk[mapping["actor1_country_code"]] if "actor1_country_code" in mapping else pd.NA
    )
    df["actor2_country_code"] = (
        chunk[mapping["actor2_country_code"]] if "actor2_country_code" in mapping else pd.NA
    )
    df["actiongeo_country_code"] = (
        chunk[mapping["actiongeo_country_code"]] if "actiongeo_country_code" in mapping else pd.NA
    )
    df["source_url"] = chunk[mapping["source_url"]] if "source_url" in mapping else pd.NA

    df["event_root_code"] = normalize_event_root_code(df["event_root_code"])
    df["day"] = pd.to_datetime(df["day"], errors="coerce")
    if df["day"].notna().sum() == 0 and not chunk.empty:
        sample = chunk.head(1).to_dict(orient="records")
        raise ValueError(
            "Unable to parse day/date values in events data. "
            f"File: {file_path}. Chunk index: {chunk_index}. Row sample: {sample}. "
            "Remediation: verify date formats (YYYYMMDD or ISO) and ensure the delimiter is correct."
        )
    df = df.dropna(subset=["day"])
    df["day"] = df["day"].dt.date.astype(str)
    return df[EVENTS_CANONICAL_COLUMNS]


def _normalize_gkg(chunk: pd.DataFrame, *, file_path: Path, chunk_index: int) -> pd.DataFrame:
    mapping = map_columns(chunk.columns, _schema_aliases("gkg"))
    if "datetime" not in mapping:
        sample = chunk.head(1).to_dict(orient="records")
        raise ValueError(
            "Missing required datetime column in GKG data. "
            f"File: {file_path}. Row sample: {sample}. "
            "Remediation: ensure the file has DATE/datetime columns or pass --format gkg with headers."
        )
    df = pd.DataFrame()
    df["datetime"] = (
        pd.to_datetime(chunk[mapping["datetime"]], errors="coerce")
        if "datetime" in mapping
        else pd.NaT
    )
    df["document_identifier"] = (
        chunk[mapping["document_identifier"]] if "document_identifier" in mapping else pd.NA
    )
    df["themes"] = chunk[mapping["themes"]] if "themes" in mapping else pd.NA
    df["persons"] = chunk[mapping["persons"]] if "persons" in mapping else pd.NA
    df["organizations"] = (
        chunk[mapping["organizations"]] if "organizations" in mapping else pd.NA
    )
    df["locations"] = chunk[mapping["locations"]] if "locations" in mapping else pd.NA
    df["tone"] = chunk[mapping["tone"]] if "tone" in mapping else pd.NA
    if df["datetime"].notna().sum() == 0 and not chunk.empty:
        sample = chunk.head(1).to_dict(orient="records")
        raise ValueError(
            "Unable to parse datetime values in GKG data. "
            f"File: {file_path}. Chunk index: {chunk_index}. Row sample: {sample}. "
            "Remediation: verify date formats and delimiter."
        )
    df = df.dropna(subset=["datetime"])
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df[GKG_CANONICAL_COLUMNS]


def _write_csv_partition(
    df: pd.DataFrame,
    partition_dir: Path,
    *,
    filename: str,
    header_written: bool,
) -> None:
    ensure_dir(partition_dir)
    path = partition_dir / filename
    df.to_csv(path, mode="a", header=not header_written, index=False, lineterminator="\n")


def _write_parquet_partition(
    df: pd.DataFrame,
    partition_dir: Path,
    *,
    filename: str,
    writer_cache: dict[Path, Any],
) -> None:
    import importlib.util

    if importlib.util.find_spec("pyarrow") is None:
        raise ImportError("pyarrow is required for parquet output. Install it or use --write csv.")

    import pyarrow as pa
    import pyarrow.parquet as pq

    ensure_dir(partition_dir)
    path = partition_dir / filename
    table = pa.Table.from_pandas(df, preserve_index=False)
    writer = writer_cache.get(path)
    if writer is None:
        writer = pq.ParquetWriter(path, table.schema)
        writer_cache[path] = writer
    writer.write_table(table)


def ingest_gdelt(
    *,
    raw_dir: Path,
    out_dir: Path,
    file_glob: str,
    format_hint: str,
    write_format: str,
    require_files: bool = False,
) -> IngestResult | None:
    files = list_files(raw_dir, file_glob)
    if not files:
        if format_hint == "gkg" and not require_files:
            print("[gdelt.ingest] no files matched; exiting without error.")
            return None
        raise FileNotFoundError(f"No files matched {file_glob} in {raw_dir}.")

    schema_type = format_hint
    if format_hint == "auto":
        first_spec = analyze_file(files[0])
        schema_type = detect_schema_type(first_spec.columns, first_spec.column_count)
        if schema_type == "unknown":
            raise ValueError(
                f"Unable to detect schema for {files[0]}. Provide --format events|gkg and confirm headers."
            )

    output_root = out_dir / schema_type
    ensure_dir(output_root)
    writer_cache: dict[Path, Any] = {}
    header_written: set[Path] = set()
    rows_per_day: dict[str, int] = {}
    total_rows = 0

    for path in files:
        file_spec = analyze_file(path)
        schema_columns = _resolve_schema_columns(
            schema_type, file_spec.has_header, file_spec.column_count
        )
        try:
            reader = pd.read_csv(
                path,
                sep=file_spec.delimiter,
                header=0 if file_spec.has_header else None,
                names=schema_columns,
                dtype=str,
                chunksize=100_000,
                low_memory=False,
            )
        except OSError as exc:
            raise OSError(
                f"Failed to read {path}. Ensure the file is readable and not locked by sync tools (e.g., OneDrive)."
            ) from exc

        for chunk_index, chunk in enumerate(reader):
            if schema_type == "events":
                normalized = _normalize_events(chunk, file_path=path, chunk_index=chunk_index)
                if normalized.empty:
                    continue
                for day, group in normalized.groupby("day"):
                    partition_dir = output_root / f"day={day}"
                    filename = f"part-00000.{write_format}"
                    if write_format == "csv":
                        already = partition_dir / filename in header_written
                        _write_csv_partition(group, partition_dir, filename=filename, header_written=already)
                        header_written.add(partition_dir / filename)
                    else:
                        _write_parquet_partition(
                            group, partition_dir, filename=filename, writer_cache=writer_cache
                        )
                    rows_per_day[day] = rows_per_day.get(day, 0) + len(group)
                    total_rows += len(group)
            else:
                normalized = _normalize_gkg(chunk, file_path=path, chunk_index=chunk_index)
                if normalized.empty:
                    continue
                normalized["day"] = normalized["datetime"].str.slice(0, 10)
                for day, group in normalized.groupby("day"):
                    partition_dir = output_root / f"day={day}"
                    filename = f"part-00000.{write_format}"
                    if write_format == "csv":
                        already = partition_dir / filename in header_written
                        _write_csv_partition(group, partition_dir, filename=filename, header_written=already)
                        header_written.add(partition_dir / filename)
                    else:
                        _write_parquet_partition(
                            group, partition_dir, filename=filename, writer_cache=writer_cache
                        )
                    rows_per_day[day] = rows_per_day.get(day, 0) + len(group)
                    total_rows += len(group)

    for writer in writer_cache.values():
        writer.close()

    min_day = min(rows_per_day.keys()) if rows_per_day else None
    max_day = max(rows_per_day.keys()) if rows_per_day else None

    manifest = {
        "schema_version": 1,
        "created_utc": utc_now_iso(),
        "raw_dir": str(raw_dir),
        "file_glob": file_glob,
        "file_list": [str(path) for path in files] if len(files) <= 1000 else None,
        "file_list_hash": None if len(files) <= 1000 else build_content_hash([str(path) for path in files]),
        "coverage": {
            "min_day": min_day,
            "max_day": max_day,
            "n_days": len(rows_per_day),
        },
        "row_counts": {
            "total_rows": total_rows,
            "per_day_rows": rows_per_day,
        },
        "columns": _canonical_columns(schema_type),
        "dtypes": _canonical_dtypes(schema_type),
        "content_hash": build_content_hash(
            {
                "raw_dir": str(raw_dir),
                "glob": file_glob,
                "schema_type": schema_type,
                "write_format": write_format,
                "files": build_file_fingerprint(files),
            }
        ),
    }
    manifest_path = output_root / "manifest.json"
    with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(manifest, indent=2))

    return IngestResult(
        manifest_path=manifest_path,
        rows_total=total_rows,
        rows_per_day=rows_per_day,
        min_day=min_day,
        max_day=max_day,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest local GDELT CSVs into a partitioned cache.")
    parser.add_argument("--raw-dir", required=True, help="Directory containing raw GDELT files.")
    parser.add_argument("--out-dir", required=True, help="Output directory for normalized cache.")
    parser.add_argument("--format", default="events", choices=["auto", "events", "gkg"])
    parser.add_argument("--glob", default="*.csv", help="File glob pattern.")
    parser.add_argument("--write", default="csv", choices=["csv", "parquet"])
    parser.add_argument("--require", action="store_true", help="Fail if no files matched.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    raw_dir = Path(args.raw_dir).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    env_raw = os.getenv("MARKET_APP_GDELT_RAW_DIR") or os.getenv("MARKET_APP_GDELT_EVENTS_RAW_DIR")
    if env_raw:
        warn_if_unusable(Path(env_raw).expanduser(), file_glob="*.csv", context="gdelt.ingest")
    try:
        result = ingest_gdelt(
            raw_dir=raw_dir,
            out_dir=out_dir,
            file_glob=args.glob,
            format_hint=args.format,
            write_format=args.write,
            require_files=args.require,
        )
    except (FileNotFoundError, ValueError, OSError) as exc:
        print(f"[gdelt.ingest] {exc}")
        return 2
    if result is None:
        return 0
    print(f"[gdelt.ingest] wrote manifest: {result.manifest_path}")
    print(
        f"[gdelt.ingest] coverage: {result.min_day} -> {result.max_day} ({len(result.rows_per_day)} days)"
    )
    print(f"[gdelt.ingest] rows: {result.rows_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
