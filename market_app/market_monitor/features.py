import math

import numpy as np
import pandas as pd


def _safe_return(close: np.ndarray, days: int, end_offset: int = 0) -> float:
    n = len(close)
    start_idx = n - days - 1 - end_offset
    end_idx = n - 1 - end_offset
    if start_idx < 0 or end_idx <= 0:
        return np.nan
    if close[start_idx] <= 0 or close[end_idx] <= 0:
        return np.nan
    return float(close[end_idx] / close[start_idx] - 1.0)


def _sma(close: np.ndarray, days: int) -> float:
    if len(close) < days:
        return np.nan
    return float(np.nanmean(close[-days:]))


def _ann_vol(returns: np.ndarray, window: int) -> float:
    if len(returns) < window:
        return np.nan
    return float(np.nanstd(returns[-window:], ddof=1) * math.sqrt(252.0))


def _trend_quality(close: np.ndarray, window: int) -> tuple[float, float, float]:
    if len(close) < window:
        return np.nan, np.nan, np.nan
    y = np.log(np.clip(close[-window:], 1e-12, None))
    x = np.arange(window, dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    y_hat = slope * x + intercept
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
    quality = float(slope * (r2 if not np.isnan(r2) else 0.0))
    return float(slope), float(r2), quality


def _ulcer_index(close: np.ndarray, window: int) -> float:
    if len(close) < window:
        return np.nan
    subset = close[-window:]
    peak = np.maximum.accumulate(subset)
    dd = (subset / peak) - 1.0
    return float(np.sqrt(np.mean(dd**2)))


def _drawdown_duration(close: np.ndarray, window: int) -> float:
    if len(close) < window:
        return np.nan
    subset = close[-window:]
    peak = np.maximum.accumulate(subset)
    dd = subset / peak - 1.0
    duration = 0
    max_duration = 0
    for value in dd:
        if value < 0:
            duration += 1
            max_duration = max(max_duration, duration)
        else:
            duration = 0
    return float(max_duration)


def compute_features(df: pd.DataFrame) -> dict[str, float]:
    df = df.copy()
    close = df["Close"].to_numpy(dtype=float)
    volume_series = df.get("Volume", pd.Series([np.nan] * len(df)))
    volume = volume_series.to_numpy(dtype=float)
    open_px = df.get("Open", pd.Series([np.nan] * len(df))).to_numpy(dtype=float)
    high = df.get("High", pd.Series([np.nan] * len(df))).to_numpy(dtype=float)
    low = df.get("Low", pd.Series([np.nan] * len(df))).to_numpy(dtype=float)
    n = len(close)

    returns = np.diff(np.log(np.clip(close, 1e-12, None)))
    vol20 = _ann_vol(returns, 20)
    vol60 = _ann_vol(returns, 60)
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

    sma20 = _sma(close, 20)
    sma50 = _sma(close, 50)
    sma200 = _sma(close, 200)
    close_last = float(close[-1]) if n else np.nan

    pct_days_above_sma200 = np.nan
    if n >= 200:
        roll200 = pd.Series(close).rolling(200).mean().to_numpy()
        above = (close[-200:] > roll200[-200:]).astype(float)
        pct_days_above_sma200 = float(np.nanmean(above))

    volume_available = bool(np.isfinite(volume).any())
    adv20_dollar = np.nan
    adv20 = np.nan
    if n >= 20 and volume_available:
        adv20 = float(np.nanmean(volume[-20:]))
        adv20_dollar = adv20 * close_last

    zero_volume_frac = float(np.mean(volume == 0)) if n and volume_available else np.nan
    missing_frac = float(np.mean(np.isnan(close))) if n else np.nan

    trend3 = _trend_quality(close, 63)
    trend6 = _trend_quality(close, 126)
    trend12 = _trend_quality(close, 252)

    high_63 = float(np.nanmax(close[-63:])) if n >= 63 else np.nan
    high_252 = float(np.nanmax(close[-252:])) if n >= 252 else np.nan
    dist_63 = close_last / high_63 - 1.0 if n >= 63 and high_63 > 0 else np.nan
    dist_252 = close_last / high_252 - 1.0 if n >= 252 and high_252 > 0 else np.nan

    mom_12_1 = _safe_return(close, 252, end_offset=21)
    mom_6_1 = _safe_return(close, 126, end_offset=21)
    accel_1m_vs_6m = np.nan
    ret_1m = _safe_return(close, 21)
    ret_6m = _safe_return(close, 126)
    if ret_1m is not None and ret_6m is not None and not np.isnan(ret_1m) and not np.isnan(ret_6m):
        accel_1m_vs_6m = float(ret_1m - ret_6m / 6.0)

    vol_of_vol = np.nan
    if len(returns) >= 80:
        roll = pd.Series(returns).rolling(20).std()
        vol_of_vol = float(roll.rolling(60).std().iloc[-1] * math.sqrt(252.0))

    cvar_60 = np.nan
    if len(returns) >= 60:
        window_returns = returns[-60:]
        cutoff = np.nanquantile(window_returns, 0.05)
        cvar_60 = float(window_returns[window_returns <= cutoff].mean())

    ulcer_60 = _ulcer_index(close, 60)
    drawdown_duration_60 = _drawdown_duration(close, 60)

    true_range = None
    if n >= 2:
        prev_close = np.roll(close, 1)
        tr = np.maximum.reduce(
            [
                high - low,
                np.abs(high - prev_close),
                np.abs(low - prev_close),
            ]
        )
        true_range = tr
    atr14 = float(np.nanmean(true_range[-14:])) if n >= 14 and true_range is not None else np.nan
    gap_atr = (
        float((open_px[-1] - close[-2]) / atr14)
        if n >= 2 and atr14 and not np.isnan(atr14)
        else np.nan
    )
    range_expansion = (
        float((high[-1] - low[-1]) / atr14) if n >= 1 and atr14 and not np.isnan(atr14) else np.nan
    )
    big_day_freq = np.nan
    if n >= 20 and atr14 and not np.isnan(atr14):
        ranges = (high[-20:] - low[-20:]) / atr14
        big_day_freq = float(np.mean(ranges > 1.5))
    close_to_high = (
        float((close_last - low[-1]) / (high[-1] - low[-1]))
        if n >= 1 and high[-1] > low[-1]
        else np.nan
    )

    volume_z = np.nan
    if volume_available and n >= 20:
        med = float(np.nanmedian(volume[-60:]))
        mad = float(np.nanmedian(np.abs(volume[-60:] - med))) if n >= 60 else float(
            np.nanmedian(np.abs(volume[-20:] - med))
        )
        if mad > 0:
            volume_z = float((volume[-1] - med) / (1.4826 * mad))

    if "Date" in df.columns:
        dates = pd.to_datetime(df["Date"], errors="coerce")
        if not dates.isna().all():
            expected = pd.bdate_range(dates.min(), dates.max())
            missing_day_rate = float(1.0 - len(dates.dropna()) / len(expected)) if len(expected) else np.nan
        else:
            missing_day_rate = np.nan
    else:
        missing_day_rate = np.nan

    stale_flag = False
    if n >= 5:
        stale_flag = bool(np.allclose(close[-5:], close[-1], equal_nan=False))

    max_abs_return = float(np.nanmax(np.abs(np.diff(close) / close[:-1]))) if n >= 2 else np.nan
    corp_action_suspect = bool(max_abs_return > 0.5) if not np.isnan(max_abs_return) else False

    return {
        "ret_1m": ret_1m,
        "ret_3m": _safe_return(close, 63),
        "ret_6m": ret_6m,
        "ret_12m": _safe_return(close, 252),
        "mom_12_1": mom_12_1,
        "mom_6_1": mom_6_1,
        "accel_1m_vs_6m": accel_1m_vs_6m,
        "sma20_ratio": (close_last / sma20 - 1.0) if sma20 and not np.isnan(sma20) else np.nan,
        "sma50_ratio": (close_last / sma50 - 1.0) if sma50 and not np.isnan(sma50) else np.nan,
        "sma200_ratio": (close_last / sma200 - 1.0) if sma200 and not np.isnan(sma200) else np.nan,
        "pct_days_above_sma200": pct_days_above_sma200,
        "trend_slope_3m": trend3[0],
        "trend_r2_3m": trend3[1],
        "trend_quality_3m": trend3[2],
        "trend_slope_6m": trend6[0],
        "trend_r2_6m": trend6[1],
        "trend_quality_6m": trend6[2],
        "trend_slope_12m": trend12[0],
        "trend_r2_12m": trend12[1],
        "trend_quality_12m": trend12[2],
        "distance_to_63d_high": dist_63,
        "distance_to_52w_high": dist_252,
        "vol20_ann": vol20,
        "vol60_ann": vol60,
        "vol_of_vol": vol_of_vol,
        "downside_vol_ann": downside,
        "worst_5d_return": worst_5d,
        "max_drawdown_6m": max_dd,
        "cvar_60d": cvar_60,
        "ulcer_index_60d": ulcer_60,
        "drawdown_duration_60d": drawdown_duration_60,
        "gap_atr": gap_atr,
        "range_expansion": range_expansion,
        "big_day_freq": big_day_freq,
        "close_to_high": close_to_high,
        "volume_z": volume_z,
        "adv20_dollar": adv20_dollar,
        "adv20_volume": adv20,
        "volume_available": float(volume_available),
        "history_days": float(n),
        "missing_frac": missing_frac,
        "missing_day_rate": missing_day_rate,
        "zero_volume_frac": zero_volume_frac,
        "stale_price_flag": float(stale_flag),
        "corp_action_suspect": float(corp_action_suspect),
    }
