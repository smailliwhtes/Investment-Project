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
    quality: dict[str, object]


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
            quality=_empty_quality(symbol),
        )
    df = pd.read_csv(path)
    normalized, missing_volume, missing_data = _normalize_ohlcv(df)
    return OhlcvResult(
        symbol=symbol,
        frame=normalized,
        source_path=path,
        missing_volume=missing_volume,
        missing_data=missing_data,
        quality=_quality_metrics(symbol, normalized),
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


def _normalize_ohlcv(df: pd.DataFrame) -> tuple[pd.DataFrame, bool, bool]:
    columns = {col.lower().strip(): col for col in df.columns}
    missing = [col for col in REQUIRED_COLUMNS if col not in columns]
    if missing:
        normalized = pd.DataFrame(columns=list(REQUIRED_COLUMNS) + list(OPTIONAL_COLUMNS))
        return normalized, True, True

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

    normalized = normalized.dropna(subset=["date"]).drop_duplicates(subset=["date"])
    normalized = normalized.sort_values("date").reset_index(drop=True)
    missing_volume = volume_col is None or normalized["volume"].isna().all()
    missing_data = normalized.empty
    return normalized, missing_volume, missing_data


def _empty_quality(symbol: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "last_date": "",
        "n_rows": 0,
        "missing_days": 0,
        "zero_volume_fraction": pd.NA,
        "bad_ohlc_count": 0,
    }


def _quality_metrics(symbol: str, frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return _empty_quality(symbol)
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna().sort_values()
    last_date = "" if dates.empty else dates.iloc[-1].date().isoformat()
    missing_days = 0
    if len(dates) >= 2:
        full = pd.bdate_range(dates.iloc[0], dates.iloc[-1])
        missing_days = max(0, len(full) - len(pd.Index(dates.dt.normalize().unique())))
    volume = pd.to_numeric(frame.get("volume"), errors="coerce")
    zero_volume_fraction = pd.NA
    if volume is not None and not volume.isna().all():
        lookback = int(min(60, len(volume)))
        if lookback > 0:
            tail = volume.tail(lookback)
            zero_volume_fraction = float((tail == 0).mean())

    o = pd.to_numeric(frame.get("open"), errors="coerce")
    h = pd.to_numeric(frame.get("high"), errors="coerce")
    l = pd.to_numeric(frame.get("low"), errors="coerce")
    c = pd.to_numeric(frame.get("close"), errors="coerce")
    bad = ((o <= 0) | (h <= 0) | (l <= 0) | (c <= 0) | (h < l)).fillna(True)
    return {
        "symbol": symbol,
        "last_date": last_date,
        "n_rows": int(len(frame)),
        "missing_days": int(missing_days),
        "zero_volume_fraction": zero_volume_fraction,
        "bad_ohlc_count": int(bad.sum()),
    }
