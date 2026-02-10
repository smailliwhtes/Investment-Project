from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FeatureResult:
    symbol: str
    features: dict[str, Any]


def compute_features(symbol: str, frame: pd.DataFrame, config: dict[str, Any]) -> FeatureResult:
    if frame.empty:
        return FeatureResult(symbol=symbol, features=_empty_features(symbol))

    close = frame["close"].to_numpy(dtype=float)
    volume = frame.get("volume", pd.Series([np.nan] * len(frame))).to_numpy(dtype=float)
    dates = pd.to_datetime(frame["date"], errors="coerce")
    as_of_date = dates.max().date().isoformat() if not dates.isna().all() else ""
    history_days = len(close)
    returns = np.diff(np.log(np.clip(close, 1e-12, None)))

    feature_values = {
        "symbol": symbol,
        "as_of_date": as_of_date,
        "history_days": history_days,
        "return_1m": _safe_return(close, 21),
        "return_3m": _safe_return(close, 63),
        "return_6m": _safe_return(close, 126),
        "return_12m": _safe_return(close, 252),
        "sma20": _sma(close, 20),
        "sma50": _sma(close, 50),
        "sma200": _sma(close, 200),
        "close_to_sma20": _ratio(close, 20),
        "close_to_sma50": _ratio(close, 50),
        "close_to_sma200": _ratio(close, 200),
        "pct_days_above_sma200_6m": _pct_days_above_sma200(close, window=126),
        "volatility_20d": _ann_vol(returns, 20),
        "volatility_60d": _ann_vol(returns, 60),
        "downside_volatility": _downside_vol(returns, 60),
        "worst_5d_return_6m": _worst_5d_return(close, window=126),
        "max_drawdown_6m": _max_drawdown(close, window=126),
        "adv20_usd": _adv20_usd(close, volume),
        "zero_volume_fraction_60d": _zero_volume_fraction(volume, window=60),
        "missing_data": frame.isna().any(axis=None),
        "stale_data": _is_stale(dates, config),
        "split_suspect": _split_suspect(returns),
        "volume_missing": np.isnan(volume).all(),
    }
    return FeatureResult(symbol=symbol, features=feature_values)


def _empty_features(symbol: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "as_of_date": "",
        "history_days": 0,
        "return_1m": np.nan,
        "return_3m": np.nan,
        "return_6m": np.nan,
        "return_12m": np.nan,
        "sma20": np.nan,
        "sma50": np.nan,
        "sma200": np.nan,
        "close_to_sma20": np.nan,
        "close_to_sma50": np.nan,
        "close_to_sma200": np.nan,
        "pct_days_above_sma200_6m": np.nan,
        "volatility_20d": np.nan,
        "volatility_60d": np.nan,
        "downside_volatility": np.nan,
        "worst_5d_return_6m": np.nan,
        "max_drawdown_6m": np.nan,
        "adv20_usd": np.nan,
        "zero_volume_fraction_60d": np.nan,
        "missing_data": True,
        "stale_data": True,
        "split_suspect": False,
        "volume_missing": True,
    }


def _safe_return(close: np.ndarray, days: int) -> float:
    if len(close) <= days:
        return float("nan")
    start = close[-days - 1]
    end = close[-1]
    if start <= 0 or end <= 0:
        return float("nan")
    return float(end / start - 1.0)


def _sma(close: np.ndarray, days: int) -> float:
    if len(close) < days:
        return float("nan")
    return float(np.nanmean(close[-days:]))


def _ratio(close: np.ndarray, days: int) -> float:
    if len(close) < days:
        return float("nan")
    sma = _sma(close, days)
    if sma <= 0 or math.isnan(sma):
        return float("nan")
    return float(close[-1] / sma)


def _pct_days_above_sma200(close: np.ndarray, window: int) -> float:
    if len(close) < 200 or len(close) < window:
        return float("nan")
    rolling = pd.Series(close).rolling(200).mean().to_numpy()
    recent = close[-window:]
    recent_sma = rolling[-window:]
    if len(recent_sma) != len(recent):
        return float("nan")
    return float(np.nanmean(recent > recent_sma))


def _ann_vol(returns: np.ndarray, window: int) -> float:
    if len(returns) < window:
        return float("nan")
    return float(np.nanstd(returns[-window:], ddof=1) * math.sqrt(252.0))


def _downside_vol(returns: np.ndarray, window: int) -> float:
    if len(returns) < window:
        return float("nan")
    sample = returns[-window:]
    downside = sample[sample < 0]
    if len(downside) < 2:
        return 0.0
    return float(np.nanstd(downside, ddof=1) * math.sqrt(252.0))


def _worst_5d_return(close: np.ndarray, window: int) -> float:
    if len(close) < window or len(close) < 6:
        return float("nan")
    subset = close[-window:]
    r5 = subset[5:] / subset[:-5] - 1.0
    return float(np.nanmin(r5)) if len(r5) else float("nan")


def _max_drawdown(close: np.ndarray, window: int) -> float:
    if len(close) < 2:
        return float("nan")
    subset = close[-window:] if len(close) >= window else close
    peak = np.maximum.accumulate(subset)
    drawdown = subset / peak - 1.0
    return float(np.nanmin(drawdown))


def _adv20_usd(close: np.ndarray, volume: np.ndarray) -> float:
    if len(close) < 20 or len(volume) < 20 or np.isnan(volume).all():
        return float("nan")
    return float(np.nanmean(volume[-20:] * close[-20:]))


def _zero_volume_fraction(volume: np.ndarray, window: int) -> float:
    if len(volume) < window or np.isnan(volume).all():
        return float("nan")
    subset = volume[-window:]
    return float(np.mean(subset == 0))


def _is_stale(dates: pd.Series, config: dict[str, Any]) -> bool:
    if dates.isna().all():
        return True
    max_lag = int(config.get("gates", {}).get("max_lag_days", 5))
    latest = dates.max()
    if pd.isna(latest):
        return True
    now = pd.Timestamp.utcnow()
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    if latest.tzinfo is None:
        latest = latest.tz_localize("UTC")
    else:
        latest = latest.tz_convert("UTC")
    lag = (now - latest).days
    return lag > max_lag


def _split_suspect(returns: np.ndarray) -> bool:
    if len(returns) == 0:
        return False
    return bool(np.nanmax(np.abs(returns)) > 0.5)
