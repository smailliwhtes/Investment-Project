from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import logging

import pandas as pd


@dataclass(frozen=True)
class OhlcvResult:
    symbol: str
    frame: pd.DataFrame
    source_path: Path | None
    missing_volume: bool
    missing_data: bool
    duplicate_dates: int
    non_monotonic_dates: bool
    missing_date_rows: int
    missing_required_columns: list[str]


REQUIRED_COLUMNS = {"date", "open", "high", "low", "close"}
OPTIONAL_COLUMNS = {"adj_close", "volume"}


def load_ohlcv(symbol: str, ohlcv_dir: Path) -> OhlcvResult:
    path = _resolve_symbol_path(symbol, ohlcv_dir)
    if path is None or not path.exists():
        return OhlcvResult(
            symbol=symbol,
            frame=pd.DataFrame(columns=list(REQUIRED_COLUMNS) + list(OPTIONAL_COLUMNS)),
            source_path=None,
            missing_volume=True,
            missing_data=True,
            duplicate_dates=0,
            non_monotonic_dates=False,
            missing_date_rows=0,
            missing_required_columns=list(REQUIRED_COLUMNS),
        )
    df = pd.read_csv(path)
    (
        normalized,
        missing_volume,
        missing_data,
        duplicates,
        non_monotonic,
        missing_dates,
        missing_required,
    ) = _normalize_ohlcv(df)
    return OhlcvResult(
        symbol=symbol,
        frame=normalized,
        source_path=path,
        missing_volume=missing_volume,
        missing_data=missing_data,
        duplicate_dates=duplicates,
        non_monotonic_dates=non_monotonic,
        missing_date_rows=missing_dates,
        missing_required_columns=missing_required,
    )


def resolve_ohlcv_dir(ohlcv_dir: Path, logger: logging.Logger) -> Path:
    if ohlcv_dir and ohlcv_dir.exists() and ohlcv_dir.resolve() != Path(".").resolve():
        if any(path.suffix.lower() == ".csv" for path in ohlcv_dir.iterdir()):
            return ohlcv_dir
    sample_dir = _resolve_sample_ohlcv_dir()
    if sample_dir.exists():
        logger.warning(
            "OHLCV directory missing or empty. Falling back to bundled sample data at %s.",
            sample_dir,
        )
        return sample_dir
    return ohlcv_dir


def _resolve_symbol_path(symbol: str, ohlcv_dir: Path) -> Path | None:
    candidates = [ohlcv_dir / f"{symbol}.csv", ohlcv_dir / f"{symbol.upper()}.csv"]
    for path in candidates:
        if path.exists():
            return path
    normalized_target = symbol.replace(".", "").replace("-", "").upper()
    for path in ohlcv_dir.glob("*.csv"):
        candidate = path.stem.replace(".", "").replace("-", "").upper()
        if candidate == normalized_target:
            return path
    return None


def _resolve_sample_ohlcv_dir() -> Path:
    package_root = Path(__file__).resolve().parent
    sample_dir = package_root / "sample_data" / "ohlcv"
    if sample_dir.exists():
        return sample_dir
    repo_root = package_root.parents[2]
    return repo_root / "tests" / "data" / "ohlcv"


def _normalize_ohlcv(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, bool, bool, int, bool, int, list[str]]:
    columns = {col.lower().strip(): col for col in df.columns}
    missing = [col for col in REQUIRED_COLUMNS if col not in columns]
    if missing:
        normalized = pd.DataFrame(columns=list(REQUIRED_COLUMNS) + list(OPTIONAL_COLUMNS))
        return normalized, True, True, 0, False, 0, missing

    volume_col = columns.get("volume") or columns.get("vol")
    adj_close_col = (
        columns.get("adj close")
        or columns.get("adj_close")
        or columns.get("adjusted close")
        or columns.get("adjusted_close")
    )

    normalized = pd.DataFrame(
        {
            "date": pd.to_datetime(df[columns["date"]], errors="coerce"),
            "open": pd.to_numeric(df[columns["open"]], errors="coerce"),
            "high": pd.to_numeric(df[columns["high"]], errors="coerce"),
            "low": pd.to_numeric(df[columns["low"]], errors="coerce"),
            "close": pd.to_numeric(df[columns["close"]], errors="coerce"),
        }
    )
    if adj_close_col:
        normalized["adj_close"] = pd.to_numeric(df[adj_close_col], errors="coerce")
    if volume_col:
        normalized["volume"] = pd.to_numeric(df[volume_col], errors="coerce")
    else:
        normalized["volume"] = pd.NA

    missing_dates = int(normalized["date"].isna().sum())
    normalized = normalized.dropna(subset=["date"])
    duplicates = int(normalized.duplicated(subset=["date"]).sum())
    normalized = normalized.drop_duplicates(subset=["date"])
    non_monotonic = not normalized["date"].is_monotonic_increasing
    normalized = normalized.sort_values("date").reset_index(drop=True)
    missing_volume = volume_col is None or normalized["volume"].isna().all()
    missing_data = normalized.empty
    return normalized, missing_volume, missing_data, duplicates, non_monotonic, missing_dates, []
