import math
from typing import Dict

import numpy as np
import pandas as pd


def compute_features(df: pd.DataFrame) -> Dict[str, float]:
    close = df["Close"].to_numpy(dtype=float)
    volume = df.get("Volume", pd.Series([np.nan] * len(df))).to_numpy(dtype=float)
    n = len(close)

    def safe_return(days: int) -> float:
        if n <= days or close[-days - 1] <= 0:
            return np.nan
        return float(close[-1] / close[-days - 1] - 1.0)

    def sma(days: int) -> float:
        if n < days:
            return np.nan
        return float(np.nanmean(close[-days:]))

    returns = np.diff(np.log(np.clip(close, 1e-12, None)))

    def ann_vol(window: int) -> float:
        if len(returns) < window:
            return np.nan
        return float(np.nanstd(returns[-window:], ddof=1) * math.sqrt(252.0))

    vol20 = ann_vol(20)
    vol60 = ann_vol(60)
    if len(returns) >= 20:
        neg = returns[-20:][returns[-20:] < 0]
        downside = float(np.nanstd(neg, ddof=1) * math.sqrt(252.0)) if len(neg) >= 2 else 0.0
    else:
        downside = np.nan

    if n >= 6:
        r5 = (close[5:] / close[:-5]) - 1.0
        worst_5d = float(np.nanmin(r5)) if len(r5) else np.nan
    else:
        worst_5d = np.nan

    dd_window = min(n, 126)
    if dd_window >= 2:
        c = close[-dd_window:]
        peak = np.maximum.accumulate(c)
        dd = (c / peak) - 1.0
        max_dd = float(np.nanmin(dd))
    else:
        max_dd = np.nan

    sma20 = sma(20)
    sma50 = sma(50)
    sma200 = sma(200)
    close_last = float(close[-1]) if n else np.nan

    pct_days_above_sma200 = np.nan
    if n >= 200:
        roll200 = pd.Series(close).rolling(200).mean().to_numpy()
        above = (close[-200:] > roll200[-200:]).astype(float)
        pct_days_above_sma200 = float(np.nanmean(above))

    adv20_dollar = np.nan
    if n >= 20:
        adv20 = float(np.nanmean(volume[-20:]))
        adv20_dollar = adv20 * close_last

    zero_volume_frac = float(np.mean(volume == 0)) if n else np.nan
    missing_frac = float(np.mean(np.isnan(close))) if n else np.nan

    return {
        "ret_1m": safe_return(21),
        "ret_3m": safe_return(63),
        "ret_6m": safe_return(126),
        "ret_12m": safe_return(252),
        "sma20_ratio": (close_last / sma20 - 1.0) if sma20 and not np.isnan(sma20) else np.nan,
        "sma50_ratio": (close_last / sma50 - 1.0) if sma50 and not np.isnan(sma50) else np.nan,
        "sma200_ratio": (close_last / sma200 - 1.0) if sma200 and not np.isnan(sma200) else np.nan,
        "pct_days_above_sma200": pct_days_above_sma200,
        "vol20_ann": vol20,
        "vol60_ann": vol60,
        "downside_vol_ann": downside,
        "worst_5d_return": worst_5d,
        "max_drawdown_6m": max_dd,
        "adv20_dollar": adv20_dollar,
        "history_days": float(n),
        "missing_frac": missing_frac,
        "zero_volume_frac": zero_volume_frac,
    }
