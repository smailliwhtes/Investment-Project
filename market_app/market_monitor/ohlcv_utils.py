from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from market_monitor.gdelt.utils import build_content_hash
from market_monitor.hash_utils import hash_file

REQUIRED_COLUMNS = ["date", "open", "high", "low", "close"]
OPTIONAL_COLUMNS = ["volume", "adj_close"]


@dataclass(frozen=True)
class OhlcvAggregate:
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float = 0.0
    adj_close: float | None = None
    count: int = 0


@dataclass(frozen=True)
class NormalizedResult:
    symbol: str
    output_path: Path
    row_count: int
    date_min: str | None
    date_max: str | None
    duplicates_resolved_count: int
    missing_volume_days: int
    schema: dict[str, str]
    issues: list[str]
    content_hash: str


def detect_delimiter(sample: str, fallback: str = ",") -> str:
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=[",", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return fallback


def normalize_columns(columns: Iterable[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for col in columns:
        key = col.strip().lower()
        if key in {"date", "day", "timestamp"}:
            mapping[col] = "date"
        elif key in {"open", "o"}:
            mapping[col] = "open"
        elif key in {"high", "h"}:
            mapping[col] = "high"
        elif key in {"low", "l"}:
            mapping[col] = "low"
        elif key in {"close", "c", "adj close", "adjusted close", "adjusted_close"}:
            mapping[col] = "close"
        elif key in {"volume", "vol"}:
            mapping[col] = "volume"
        elif key in {"adj_close", "adjclose"}:
            mapping[col] = "adj_close"
    return mapping


def parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def _coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    return df


def aggregate_ohlcv(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    duplicates_resolved = 0
    missing_volume_days = 0
    aggregated: dict[str, dict[str, float | None | int]] = {}

    for _, row in df.iterrows():
        day = row.get("date")
        if not isinstance(day, str) or not day:
            continue
        item = aggregated.get(day)
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
            aggregated[day] = item
        if item["count"] >= 1:
            duplicates_resolved += 1
        item["count"] += 1

        open_val = row.get("open")
        if item["open"] is None and pd.notna(open_val):
            item["open"] = float(open_val)
        high_val = row.get("high")
        if pd.notna(high_val):
            if item["high"] is None:
                item["high"] = float(high_val)
            else:
                item["high"] = max(item["high"], float(high_val))
        low_val = row.get("low")
        if pd.notna(low_val):
            if item["low"] is None:
                item["low"] = float(low_val)
            else:
                item["low"] = min(item["low"], float(low_val))
        close_val = row.get("close")
        if pd.notna(close_val):
            item["close"] = float(close_val)
        volume_val = row.get("volume")
        if pd.notna(volume_val):
            item["volume"] = float(item["volume"]) + float(volume_val)
        adj_val = row.get("adj_close")
        if pd.notna(adj_val):
            item["adj_close"] = float(adj_val)

    rows = []
    for day in sorted(aggregated.keys()):
        item = aggregated[day]
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

    return pd.DataFrame(rows), duplicates_resolved, missing_volume_days


def finalize_frame(df: pd.DataFrame, *, strict: bool) -> tuple[pd.DataFrame, list[str]]:
    issues: list[str] = []
    missing_required = df[REQUIRED_COLUMNS].isna().any(axis=1)
    if missing_required.any():
        count = int(missing_required.sum())
        msg = f"Dropped {count} rows with missing required prices."
        if strict:
            raise ValueError(msg)
        issues.append(msg)
        df = df.loc[~missing_required].copy()
    return df, issues


def build_schema(df: pd.DataFrame) -> dict[str, str]:
    schema = {}
    for col in df.columns:
        schema[col] = str(df[col].dtype)
    return schema


def write_manifest(path: Path, results: list[NormalizedResult]) -> dict:
    payload = {
        "symbols": [
            {
                "symbol": result.symbol,
                "row_count": result.row_count,
                "date_min": result.date_min,
                "date_max": result.date_max,
                "duplicates_resolved_count": result.duplicates_resolved_count,
                "missing_volume_days": result.missing_volume_days,
                "schema": result.schema,
                "issues": result.issues,
                "content_hash": result.content_hash,
            }
            for result in results
        ]
    }
    payload["content_hash"] = build_content_hash(payload)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def build_result_manifest(symbol: str, output_path: Path, df: pd.DataFrame, issues: list[str], duplicates: int, missing_volume_days: int) -> NormalizedResult:
    file_hash = hash_file(output_path)
    entry_payload = {
        "symbol": symbol,
        "row_count": int(len(df)),
        "date_min": df["date"].min() if not df.empty else None,
        "date_max": df["date"].max() if not df.empty else None,
        "duplicates_resolved_count": duplicates,
        "missing_volume_days": missing_volume_days,
        "schema": build_schema(df),
        "issues": issues,
        "file_hash": file_hash,
    }
    content_hash = build_content_hash(entry_payload)
    return NormalizedResult(
        symbol=symbol,
        output_path=output_path,
        row_count=int(len(df)),
        date_min=entry_payload["date_min"],
        date_max=entry_payload["date_max"],
        duplicates_resolved_count=duplicates,
        missing_volume_days=missing_volume_days,
        schema=entry_payload["schema"],
        issues=issues,
        content_hash=content_hash,
    )
