from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from market_monitor.hash_utils import hash_manifest


@dataclass(frozen=True)
class DatasetInfo:
    frame: pd.DataFrame
    features: list[str]
    label: str
    day_column: str
    symbol_column: str
    close_column: str
    featureset_id: str
    dataset_hash: str
    schema: list[str]
    coverage: dict[str, str | int]
    join_manifest: dict[str, object] | None
    excluded_exogenous: list[str]


def _collect_input_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.exists():
        raise FileNotFoundError(f"Input path not found: {path}")
    parquet_files = sorted(path.glob("**/*.parquet"))
    csv_files = sorted(path.glob("**/*.csv"))
    if parquet_files:
        return parquet_files
    return csv_files


def _load_frame(path: Path) -> pd.DataFrame:
    files = _collect_input_files(path)
    if not files:
        raise ValueError(f"No input files found under {path}")
    if files[0].suffix == ".parquet":
        return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    return pd.concat([pd.read_csv(file) for file in files], ignore_index=True)


def _infer_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    lower = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def _normalize_day_column(df: pd.DataFrame, day_column: str) -> pd.DataFrame:
    normalized = pd.to_datetime(df[day_column], errors="coerce")
    if normalized.isna().all():
        raise ValueError(f"Day column '{day_column}' could not be parsed to dates")
    result = df.copy()
    result[day_column] = normalized.dt.strftime("%Y-%m-%d")
    return result


def _is_lagged_or_rolling(column: str) -> bool:
    return "_lag_" in column or "_roll" in column


def _load_join_manifest(joined_dir: Path) -> dict[str, object] | None:
    manifest_path = joined_dir / "manifest.json"
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _load_market_columns(join_manifest: dict[str, object] | None) -> set[str] | None:
    if not join_manifest:
        return None
    inputs = join_manifest.get("inputs") if isinstance(join_manifest, dict) else None
    if not isinstance(inputs, dict):
        return None
    market_path = inputs.get("market_path")
    if not isinstance(market_path, str):
        return None
    market_files = _collect_input_files(Path(market_path))
    if not market_files:
        return None
    sample = market_files[0]
    if sample.suffix == ".parquet":
        frame = pd.read_parquet(sample)
    else:
        frame = pd.read_csv(sample)
    return set(frame.columns)


def _exogenous_columns(
    joined_columns: Iterable[str],
    market_columns: set[str] | None,
    day_column: str,
    symbol_column: str,
) -> set[str]:
    if market_columns is None:
        return set()
    return {
        col
        for col in joined_columns
        if col not in market_columns and col not in {day_column, symbol_column}
    }


def _compute_label(
    frame: pd.DataFrame,
    *,
    close_column: str,
    horizon_days: int,
    day_column: str,
    symbol_column: str,
) -> pd.DataFrame:
    sorted_frame = frame.sort_values([symbol_column, day_column])
    close = pd.to_numeric(sorted_frame[close_column], errors="coerce")
    label = (
        close.groupby(sorted_frame[symbol_column]).shift(-horizon_days) / close - 1.0
    )
    result = sorted_frame.copy()
    result[f"label_fwd_return_{horizon_days}d"] = label
    return result


def build_dataset(
    *,
    joined_path: Path,
    horizon_days: int = 5,
    allow_exogenous: Iterable[str] | None = None,
    day_column: str | None = None,
    symbol_column: str | None = None,
    close_column: str | None = None,
) -> DatasetInfo:
    joined_frame = _load_frame(joined_path)
    join_manifest = _load_join_manifest(joined_path) if joined_path.is_dir() else None

    day_column = day_column or _infer_column(joined_frame.columns, ["day", "date", "Date", "as_of_date"])
    if not day_column:
        raise ValueError("Joined dataset missing a recognizable day column")
    symbol_column = symbol_column or _infer_column(joined_frame.columns, ["symbol", "ticker"])
    if not symbol_column:
        raise ValueError("Joined dataset missing a recognizable symbol column")
    close_column = close_column or _infer_column(joined_frame.columns, ["close", "Close"])
    if not close_column:
        raise ValueError("Joined dataset missing a recognizable close column for labels")

    joined_frame = _normalize_day_column(joined_frame, day_column)
    joined_frame = joined_frame.rename(columns={day_column: "day", symbol_column: "symbol"})

    allowlist = {col for col in (allow_exogenous or [])}
    market_columns = _load_market_columns(join_manifest)
    missing_allowlist = [col for col in allowlist if col not in joined_frame.columns]
    if missing_allowlist:
        sample = ", ".join(missing_allowlist[:8])
        raise ValueError(f"Allowlisted exogenous columns not found: {sample}")
    exogenous = _exogenous_columns(joined_frame.columns, market_columns, "day", "symbol")

    labeled = _compute_label(
        joined_frame,
        close_column=close_column,
        horizon_days=horizon_days,
        day_column="day",
        symbol_column="symbol",
    )
    label_column = f"label_fwd_return_{horizon_days}d"

    numeric_cols = labeled.select_dtypes(include=[np.number]).columns.tolist()
    feature_cols = [
        col
        for col in numeric_cols
        if col not in {label_column} and col not in {"day", "symbol"}
    ]
    raw_exogenous = sorted(col for col in exogenous if not _is_lagged_or_rolling(col))
    excluded_exogenous = [col for col in raw_exogenous if col not in allowlist]
    if excluded_exogenous:
        feature_cols = [col for col in feature_cols if col not in excluded_exogenous]
    feature_cols = sorted(set(feature_cols))

    labeled = labeled.dropna(subset=[label_column])
    coverage = {
        "min_day": str(labeled["day"].min()) if not labeled.empty else "",
        "max_day": str(labeled["day"].max()) if not labeled.empty else "",
        "rows": int(len(labeled)),
    }
    schema = list(labeled.columns)
    featureset_payload = {
        "schema": schema,
        "feature_columns": feature_cols,
        "label": label_column,
        "horizon_days": horizon_days,
        "join_manifest_hash": join_manifest.get("content_hash") if join_manifest else None,
    }
    featureset_id = hash_manifest(featureset_payload)
    dataset_hash = hash_manifest({**featureset_payload, "coverage": coverage})

    return DatasetInfo(
        frame=labeled,
        features=feature_cols,
        label=label_column,
        day_column="day",
        symbol_column="symbol",
        close_column=close_column,
        featureset_id=featureset_id,
        dataset_hash=dataset_hash,
        schema=schema,
        coverage=coverage,
        join_manifest=join_manifest,
        excluded_exogenous=excluded_exogenous,
    )


def load_prediction_frame(
    *,
    joined_path: Path,
    feature_columns: Iterable[str],
    allow_exogenous: Iterable[str] | None = None,
    day_column: str | None = None,
    symbol_column: str | None = None,
) -> pd.DataFrame:
    joined_frame = _load_frame(joined_path)
    join_manifest = _load_join_manifest(joined_path) if joined_path.is_dir() else None

    day_column = day_column or _infer_column(joined_frame.columns, ["day", "date", "Date", "as_of_date"])
    if not day_column:
        raise ValueError("Joined dataset missing a recognizable day column")
    symbol_column = symbol_column or _infer_column(joined_frame.columns, ["symbol", "ticker"])
    if not symbol_column:
        raise ValueError("Joined dataset missing a recognizable symbol column")

    joined_frame = _normalize_day_column(joined_frame, day_column)
    joined_frame = joined_frame.rename(columns={day_column: "day", symbol_column: "symbol"})

    allowlist = {col for col in (allow_exogenous or [])}
    market_columns = _load_market_columns(join_manifest)
    missing_allowlist = [col for col in allowlist if col not in joined_frame.columns]
    if missing_allowlist:
        sample = ", ".join(missing_allowlist[:8])
        raise ValueError(f"Allowlisted exogenous columns not found: {sample}")
    exogenous = _exogenous_columns(joined_frame.columns, market_columns, "day", "symbol")
    raw_exogenous = sorted(col for col in exogenous if not _is_lagged_or_rolling(col))
    disallowed = [col for col in raw_exogenous if col not in allowlist]
    if disallowed:
        frame_drop = joined_frame.drop(columns=disallowed, errors="ignore")
    else:
        frame_drop = joined_frame

    missing = [col for col in feature_columns if col not in joined_frame.columns]
    if missing:
        sample = ", ".join(missing[:8])
        raise ValueError(f"Joined dataset missing required feature columns: {sample}")

    frame = frame_drop.copy()
    frame = frame.dropna(subset=list(feature_columns))
    frame = frame.sort_values(["day", "symbol"])
    return frame


def update_run_manifest(output_dir: Path, ml_payload: dict[str, object]) -> None:
    manifest_path = output_dir / "run_manifest.json"
    if not manifest_path.exists():
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("ml", {}).update(ml_payload)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
