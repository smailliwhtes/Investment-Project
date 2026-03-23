from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from market_monitor.features.schema import FEATURE_COLUMNS, FEATURE_SCHEMA_VERSION
from market_monitor.gdelt.utils import build_content_hash
from market_monitor.hash_utils import hash_file
from market_monitor.tabular_io import (
    PREFERRED_DATA_SUFFIXES,
    list_symbol_table_paths,
    read_tabular,
    resolve_symbol_table_path,
    write_tabular,
)

OHLCV_REQUIRED_COLUMNS = ("date", "open", "high", "low", "close")
OHLCV_OPTIONAL_COLUMNS = ("volume", "adj_close")
OHLCV_ALL_COLUMNS = OHLCV_REQUIRED_COLUMNS + OHLCV_OPTIONAL_COLUMNS
OHLCV_SUFFIX_PRIORITY = PREFERRED_DATA_SUFFIXES
_SKIP_OHLCV_STEMS = {"conversion_errors", "ohlcv_manifest", "universe"}


def _normalize_symbol_key(value: str) -> str:
    return str(value).replace("-", "").replace(".", "").replace("/", "").strip().upper()


def symbol_from_ohlcv_path(path: Path) -> str:
    if path.parent.name.startswith("symbol="):
        return path.parent.name.split("=", 1)[1].strip().upper()
    return path.stem.upper()


def iter_ohlcv_paths(ohlcv_dir: Path) -> list[Path]:
    if not ohlcv_dir.exists() or not ohlcv_dir.is_dir():
        return []
    return list_symbol_table_paths(ohlcv_dir, suffixes=OHLCV_SUFFIX_PRIORITY, skip_stems=_SKIP_OHLCV_STEMS)


def has_ohlcv_data(ohlcv_dir: Path) -> bool:
    return bool(iter_ohlcv_paths(ohlcv_dir))


def resolve_ohlcv_path(symbol: str, ohlcv_dir: Path) -> Path | None:
    if not ohlcv_dir.exists() or not ohlcv_dir.is_dir():
        return None
    target = _normalize_symbol_key(symbol)
    best: tuple[int, str, Path] | None = None
    for path in list_symbol_table_paths(ohlcv_dir, suffixes=OHLCV_SUFFIX_PRIORITY, skip_stems=_SKIP_OHLCV_STEMS):
        stem = path.parent.name.split("=", 1)[1] if path.parent.name.startswith("symbol=") else path.stem
        if _normalize_symbol_key(stem) != target:
            continue
        suffix = path.suffix.lower()
        suffix_rank = OHLCV_SUFFIX_PRIORITY.index(suffix) if suffix in OHLCV_SUFFIX_PRIORITY else len(OHLCV_SUFFIX_PRIORITY)
        rank = (suffix_rank, path.as_posix().lower(), path)
        if best is None or rank < best:
            best = rank
    if best is not None:
        return best[2]
    return resolve_symbol_table_path(ohlcv_dir, symbol, suffixes=OHLCV_SUFFIX_PRIORITY)


def _normalize_ohlcv_frame(df: pd.DataFrame, *, path: Path) -> pd.DataFrame:
    columns = {col.lower().strip(): col for col in df.columns}
    date_col = columns.get("date") or columns.get("timestamp") or columns.get("time")
    open_col = columns.get("open")
    high_col = columns.get("high")
    low_col = columns.get("low")
    close_col = columns.get("close")
    adj_close_col = (
        columns.get("adj close")
        or columns.get("adj_close")
        or columns.get("adjusted close")
        or columns.get("adjusted_close")
        or columns.get("adjclose")
    )
    volume_col = columns.get("volume") or columns.get("vol")

    missing = [
        name
        for name, value in {
            "date": date_col,
            "open": open_col,
            "high": high_col,
            "low": low_col,
            "close": close_col,
        }.items()
        if value is None
    ]
    if missing:
        raise ValueError(f"Missing required OHLCV columns in {path}: {', '.join(missing)}")

    normalized = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], errors="coerce"),
            "open": pd.to_numeric(df[open_col], errors="coerce"),
            "high": pd.to_numeric(df[high_col], errors="coerce"),
            "low": pd.to_numeric(df[low_col], errors="coerce"),
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )
    if adj_close_col is not None:
        normalized["adj_close"] = pd.to_numeric(df[adj_close_col], errors="coerce")
    if volume_col is not None:
        normalized["volume"] = pd.to_numeric(df[volume_col], errors="coerce")

    for column in OHLCV_OPTIONAL_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = np.nan

    normalized = normalized.dropna(subset=["date"]).drop_duplicates(subset=["date"], keep="last")
    normalized = normalized.sort_values("date").reset_index(drop=True)
    return normalized.loc[:, list(OHLCV_ALL_COLUMNS)]


def read_ohlcv(path: Path) -> pd.DataFrame:
    df = read_tabular(path)
    return _normalize_ohlcv_frame(df, path=path)


def write_ohlcv(path: Path, df: pd.DataFrame) -> None:
    normalized = _normalize_ohlcv_frame(df, path=path)
    if path.suffix.lower() == ".csv":
        normalized = normalized.copy()
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    write_tabular(normalized, path)


def write_features(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    for col in FEATURE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[FEATURE_COLUMNS]
    write_tabular(df, path)


def build_features_manifest(
    *,
    output_dir: Path,
    ohlcv_manifest_path: Path | None,
    feature_rows_path: Path,
) -> dict:
    payload = {
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "features_path": str(feature_rows_path),
        "features_file_hash": hash_file(feature_rows_path),
        "ohlcv_manifest_path": str(ohlcv_manifest_path) if ohlcv_manifest_path else None,
        "ohlcv_manifest_hash": hash_file(ohlcv_manifest_path) if ohlcv_manifest_path and ohlcv_manifest_path.exists() else None,
    }
    payload["content_hash"] = build_content_hash(payload)
    manifest_path = output_dir / "features_manifest.json"
    with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2))
    return payload
