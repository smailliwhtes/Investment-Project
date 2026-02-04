from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from market_monitor.ohlcv_utils import (
    REQUIRED_COLUMNS,
    aggregate_ohlcv,
    build_result_manifest,
    detect_delimiter,
    finalize_frame,
    normalize_columns,
    parse_dates,
    write_manifest,
    _coerce_numeric,
    _ensure_columns,
)


def _read_sample(path: Path, max_bytes: int = 2048) -> str:
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        return handle.read(max_bytes)


def _load_chunked(path: Path, delimiter: str, date_col: str | None, coerce: bool, chunk_rows: int) -> pd.DataFrame:
    chunks = []
    for chunk in pd.read_csv(path, delimiter=delimiter, chunksize=chunk_rows):
        mapping = normalize_columns(chunk.columns)
        chunk = chunk.rename(columns=mapping)
        if date_col and date_col in chunk.columns:
            chunk = chunk.rename(columns={date_col: "date"})
        if "date" not in chunk.columns:
            raise ValueError(f"Missing date column in {path}")
        chunk["date"] = parse_dates(chunk["date"])
        chunk = chunk.dropna(subset=["date"]).copy()
        if coerce:
            chunk = _coerce_numeric(chunk, ["open", "high", "low", "close", "volume", "adj_close"])
        chunk = _ensure_columns(chunk)
        chunks.append(chunk)
    if not chunks:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return pd.concat(chunks, ignore_index=True)


def _stream_aggregate(
    path: Path,
    delimiter: str,
    date_col: str | None,
    coerce: bool,
    chunk_rows: int,
) -> tuple[pd.DataFrame, int, int, list[str]]:
    aggregates: dict[str, dict[str, float | None | int]] = {}
    dupes = 0
    issues: list[str] = []
    has_data = False

    for chunk in pd.read_csv(path, delimiter=delimiter, chunksize=chunk_rows):
        mapping = normalize_columns(chunk.columns)
        chunk = chunk.rename(columns=mapping)
        if date_col and date_col in chunk.columns:
            chunk = chunk.rename(columns={date_col: "date"})
        if "date" not in chunk.columns:
            raise ValueError(f"Missing date column in {path}")
        chunk["date"] = parse_dates(chunk["date"])
        chunk = chunk.dropna(subset=["date"]).copy()
        if coerce:
            chunk = _coerce_numeric(chunk, ["open", "high", "low", "close", "volume", "adj_close"])
        chunk = _ensure_columns(chunk)
        chunk, chunk_issues = finalize_frame(chunk, strict=False)
        issues.extend(chunk_issues)
        if chunk.empty:
            continue
        has_data = True
        for _, row in chunk.iterrows():
            day = row.get("date")
            if not isinstance(day, str) or not day:
                continue
            item = aggregates.get(day)
            if item is None:
                item = {
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": 0.0,
                    "adj_close": None,
                    "count": 0,
                }
                aggregates[day] = item
            if item["count"] >= 1:
                dupes += 1
            item["count"] += 1
            open_val = row.get("open")
            if item["open"] is None and pd.notna(open_val):
                item["open"] = float(open_val)
            high_val = row.get("high")
            if pd.notna(high_val):
                item["high"] = float(high_val) if item["high"] is None else max(item["high"], float(high_val))
            low_val = row.get("low")
            if pd.notna(low_val):
                item["low"] = float(low_val) if item["low"] is None else min(item["low"], float(low_val))
            close_val = row.get("close")
            if pd.notna(close_val):
                item["close"] = float(close_val)
            volume_val = row.get("volume")
            if pd.notna(volume_val):
                item["volume"] = float(item["volume"]) + float(volume_val)
            adj_val = row.get("adj_close")
            if pd.notna(adj_val):
                item["adj_close"] = float(adj_val)

    if not has_data:
        return pd.DataFrame(columns=REQUIRED_COLUMNS + ["volume", "adj_close"]), 0, 0, issues

    rows = []
    missing_volume_days = 0
    for day in sorted(aggregates.keys()):
        item = aggregates[day]
        if item.get("volume") is None or (isinstance(item.get("volume"), float) and np.isnan(item["volume"])):
            missing_volume_days += 1
        rows.append(
            {
                "date": day,
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close"),
                "volume": item.get("volume"),
                "adj_close": item.get("adj_close"),
            }
        )

    return pd.DataFrame(rows), dupes, missing_volume_days, issues


def normalize_file(
    *,
    path: Path,
    out_dir: Path,
    symbol: str,
    date_col: str | None,
    delimiter: str | None,
    coerce: bool,
    strict: bool,
    streaming: bool,
    chunk_rows: int,
) -> dict:
    sample = _read_sample(path)
    resolved_delimiter = delimiter or detect_delimiter(sample)
    if streaming:
        aggregated, dupes, missing_volume_days, issues = _stream_aggregate(
            path, resolved_delimiter, date_col, coerce, chunk_rows
        )
    else:
        df = _load_chunked(path, resolved_delimiter, date_col, coerce, chunk_rows)
        df = df[REQUIRED_COLUMNS + [col for col in ["volume", "adj_close"] if col in df.columns]]
        df, issues = finalize_frame(df, strict=strict)
        aggregated, dupes, missing_volume_days = aggregate_ohlcv(df)
    aggregated, more_issues = finalize_frame(aggregated, strict=strict)
    issues.extend(more_issues)
    aggregated = aggregated.sort_values("date")

    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"{symbol}.csv"
    aggregated.to_csv(output_path, index=False)

    result = build_result_manifest(
        symbol=symbol,
        output_path=output_path,
        df=aggregated,
        issues=issues,
        duplicates=dupes,
        missing_volume_days=missing_volume_days,
    )
    return {
        "result": result,
        "output_path": output_path,
        "issues": issues,
    }


def normalize_directory(
    *,
    raw_dir: Path,
    out_dir: Path,
    date_col: str | None,
    delimiter: str | None,
    symbol_from_filename: bool,
    coerce: bool,
    strict: bool,
    streaming: bool,
    chunk_rows: int,
) -> dict:
    results = []
    raw_dir = raw_dir.resolve()
    out_dir = out_dir.resolve()
    for file_path in sorted(raw_dir.glob("*.csv")):
        symbol = file_path.stem.upper() if symbol_from_filename else file_path.stem
        result_payload = normalize_file(
            path=file_path,
            out_dir=out_dir,
            symbol=symbol,
            date_col=date_col,
            delimiter=delimiter,
            coerce=coerce,
            strict=strict,
            streaming=streaming,
            chunk_rows=chunk_rows,
        )
        results.append(result_payload["result"])

    manifest_path = out_dir / "ohlcv_manifest.json"
    manifest = write_manifest(manifest_path, results)
    return {
        "manifest_path": manifest_path,
        "results": results,
        "manifest": manifest,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize OHLCV CSV files.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    normalize_parser = subparsers.add_parser("normalize", help="Normalize OHLCV files")
    normalize_parser.add_argument("--raw-dir", required=True)
    normalize_parser.add_argument("--out-dir", required=True)
    normalize_parser.add_argument("--date-col", default=None)
    normalize_parser.add_argument("--delimiter", default=None)
    normalize_parser.add_argument("--symbol-from-filename", action="store_true", default=True)
    normalize_parser.add_argument("--coerce", action="store_true", default=True)
    normalize_parser.add_argument("--strict", action="store_true", default=False)
    normalize_parser.add_argument("--streaming", action="store_true", default=False)
    normalize_parser.add_argument("--chunk-rows", type=int, default=200_000)

    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "normalize":
        result = normalize_directory(
            raw_dir=Path(args.raw_dir),
            out_dir=Path(args.out_dir),
            date_col=args.date_col,
            delimiter=args.delimiter,
            symbol_from_filename=args.symbol_from_filename,
            coerce=args.coerce,
            strict=args.strict,
            streaming=args.streaming,
            chunk_rows=args.chunk_rows,
        )
        print(json.dumps({"manifest_path": str(result["manifest_path"])}, indent=2))
        return 0
    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
