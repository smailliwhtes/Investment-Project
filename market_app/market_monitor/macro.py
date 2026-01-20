from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SilverMacro:
    df: pd.DataFrame
    features: dict[str, float]


def load_silver_series(path: Path) -> SilverMacro | None:
    if not path.exists():
        return None
    df = pd.read_csv(path)
    columns = {c.lower(): c for c in df.columns}
    date_col = columns.get("date") or columns.get("timestamp") or columns.get("time")
    price_col = columns.get("price") or columns.get("close") or columns.get("value")
    if not date_col or not price_col:
        return None
    series = pd.DataFrame(
        {
            "Date": pd.to_datetime(df[date_col], errors="coerce"),
            "Price": pd.to_numeric(df[price_col], errors="coerce"),
        }
    ).dropna(subset=["Date", "Price"])
    series = series.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    features = compute_silver_features(series)
    return SilverMacro(df=series.reset_index(drop=True), features=features)


def compute_silver_features(df: pd.DataFrame) -> dict[str, float]:
    price = df["Price"].to_numpy(dtype=float)
    n = len(price)
    returns = np.diff(np.log(np.clip(price, 1e-12, None)))
    ret_1m = _safe_return(price, 21)
    ret_3m = _safe_return(price, 63)
    vol_3m = _ann_vol(returns, 63)
    max_dd = _max_drawdown(price, 126)
    slope, r2, quality = _trend_quality(price, 126)
    return {
        "silver_ret_1m": ret_1m,
        "silver_ret_3m": ret_3m,
        "silver_vol_3m": vol_3m,
        "silver_max_drawdown_6m": max_dd,
        "silver_trend_slope_6m": slope,
        "silver_trend_r2_6m": r2,
        "silver_trend_quality_6m": quality,
        "silver_history_days": float(n),
    }


def _safe_return(price: np.ndarray, days: int) -> float:
    if len(price) <= days or price[-days - 1] <= 0:
        return np.nan
    return float(price[-1] / price[-days - 1] - 1.0)


def _ann_vol(returns: np.ndarray, window: int) -> float:
    if len(returns) < window:
        return np.nan
    return float(np.nanstd(returns[-window:], ddof=1) * np.sqrt(252.0))


def _max_drawdown(price: np.ndarray, window: int) -> float:
    if len(price) < 2:
        return np.nan
    window = min(window, len(price))
    c = price[-window:]
    peak = np.maximum.accumulate(c)
    dd = c / peak - 1.0
    return float(np.nanmin(dd))


def _trend_quality(price: np.ndarray, window: int) -> tuple[float, float, float]:
    if len(price) < window:
        return np.nan, np.nan, np.nan
    y = np.log(np.clip(price[-window:], 1e-12, None))
    x = np.arange(window, dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    y_hat = slope * x + intercept
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
    quality = float(slope * (r2 if not np.isnan(r2) else 0.0))
    return float(slope), float(r2), quality
