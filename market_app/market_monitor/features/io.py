from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from market_monitor.gdelt.utils import build_content_hash
from market_monitor.hash_utils import hash_file
from market_monitor.features.schema import FEATURE_COLUMNS, FEATURE_SCHEMA_VERSION


def read_ohlcv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    elif "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        raise ValueError(f"Missing date column in {path}")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df


def write_features(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    for col in FEATURE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[FEATURE_COLUMNS]
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, lineterminator="\n")


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
