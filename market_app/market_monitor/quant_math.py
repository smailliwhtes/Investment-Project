from __future__ import annotations

import math
from typing import Iterable

import numpy as np

TRADING_DAYS_PER_YEAR = 252


def _to_1d(values: Iterable[float] | np.ndarray) -> np.ndarray:
    arr = np.asarray(list(values) if not isinstance(values, np.ndarray) else values, dtype=float)
    if arr.ndim == 0:
        arr = arr.reshape(1)
    return arr.reshape(-1)


def simple_returns(prices: Iterable[float] | np.ndarray) -> np.ndarray:
    px = _to_1d(prices)
    if px.size < 2:
        return np.array([], dtype=float)
    denom = px[:-1]
    numer = px[1:]
    valid = (denom > 0) & np.isfinite(denom) & np.isfinite(numer)
    out = np.full(px.size - 1, np.nan, dtype=float)
    out[valid] = (numer[valid] / denom[valid]) - 1.0
    return out


def log_returns(prices: Iterable[float] | np.ndarray) -> np.ndarray:
    px = _to_1d(prices)
    if px.size < 2:
        return np.array([], dtype=float)
    clipped = np.clip(px, 1e-12, None)
    return np.diff(np.log(clipped))


def annualized_volatility(
    returns: Iterable[float] | np.ndarray,
    *,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> float:
    arr = _to_1d(returns)
    arr = arr[np.isfinite(arr)]
    if arr.size < 2:
        return float("nan")
    return float(np.std(arr, ddof=1) * math.sqrt(periods_per_year))


def downside_volatility(
    returns: Iterable[float] | np.ndarray,
    *,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> float:
    arr = _to_1d(returns)
    arr = arr[np.isfinite(arr)]
    downside = arr[arr < 0]
    if downside.size < 2:
        return 0.0 if arr.size >= 2 else float("nan")
    return float(np.std(downside, ddof=1) * math.sqrt(periods_per_year))


def max_drawdown_from_prices(prices: Iterable[float] | np.ndarray) -> float:
    px = _to_1d(prices)
    px = px[np.isfinite(px)]
    if px.size < 2:
        return float("nan")
    peaks = np.maximum.accumulate(px)
    drawdowns = (px / peaks) - 1.0
    return float(np.min(drawdowns))


def max_drawdown_from_returns(returns: Iterable[float] | np.ndarray) -> float:
    arr = _to_1d(returns)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return float("nan")
    equity = np.cumprod(1.0 + arr)
    return max_drawdown_from_prices(equity)


def cvar(
    returns: Iterable[float] | np.ndarray,
    *,
    alpha: float = 0.95,
) -> float:
    arr = _to_1d(returns)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return float("nan")
    threshold = float(np.quantile(arr, 1.0 - alpha))
    tail = arr[arr <= threshold]
    if tail.size == 0:
        return float("nan")
    return float(np.mean(tail))


def wilder_rsi(prices: Iterable[float] | np.ndarray, *, period: int = 14) -> float:
    px = _to_1d(prices)
    if px.size <= period:
        return float("nan")

    delta = np.diff(px)
    gains = np.clip(delta, 0.0, None)
    losses = np.clip(-delta, 0.0, None)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])

    for idx in range(period, gains.size):
        avg_gain = ((avg_gain * (period - 1)) + gains[idx]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[idx]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def wilder_atr(
    high: Iterable[float] | np.ndarray,
    low: Iterable[float] | np.ndarray,
    close: Iterable[float] | np.ndarray,
    *,
    period: int = 14,
) -> float:
    hi = _to_1d(high)
    lo = _to_1d(low)
    cl = _to_1d(close)
    n = min(hi.size, lo.size, cl.size)
    if n <= period:
        return float("nan")

    hi = hi[:n]
    lo = lo[:n]
    cl = cl[:n]

    prev_close = np.roll(cl, 1)
    prev_close[0] = cl[0]
    tr = np.maximum.reduce([hi - lo, np.abs(hi - prev_close), np.abs(lo - prev_close)])

    atr = float(np.mean(tr[1 : period + 1]))
    for idx in range(period + 1, n):
        atr = ((atr * (period - 1)) + float(tr[idx])) / period
    return float(atr)


def trend_slope_r2(prices: Iterable[float] | np.ndarray, *, log_space: bool = True) -> tuple[float, float]:
    px = _to_1d(prices)
    px = px[np.isfinite(px)]
    if px.size < 3:
        return float("nan"), float("nan")

    y = np.log(np.clip(px, 1e-12, None)) if log_space else px
    x = np.arange(y.size, dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    y_hat = slope * x + intercept
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else float("nan")
    return float(slope), float(r2)


def beta(asset_returns: Iterable[float] | np.ndarray, benchmark_returns: Iterable[float] | np.ndarray) -> float:
    a = _to_1d(asset_returns)
    b = _to_1d(benchmark_returns)
    n = min(a.size, b.size)
    if n < 2:
        return float("nan")
    a = a[:n]
    b = b[:n]
    mask = np.isfinite(a) & np.isfinite(b)
    if np.sum(mask) < 2:
        return float("nan")
    a = a[mask]
    b = b[mask]
    var_b = float(np.var(b, ddof=1))
    if var_b <= 0:
        return float("nan")
    cov = float(np.cov(a, b, ddof=1)[0, 1])
    return cov / var_b


def sharpe_ratio(
    returns: Iterable[float] | np.ndarray,
    *,
    risk_free_rate_annual: float = 0.0,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> float:
    arr = _to_1d(returns)
    arr = arr[np.isfinite(arr)]
    if arr.size < 2:
        return float("nan")
    rf_per_period = risk_free_rate_annual / periods_per_year
    excess = arr - rf_per_period
    denom = np.std(excess, ddof=1)
    if denom <= 0:
        return float("nan")
    return float(np.mean(excess) / denom * math.sqrt(periods_per_year))


def sortino_ratio(
    returns: Iterable[float] | np.ndarray,
    *,
    risk_free_rate_annual: float = 0.0,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> float:
    arr = _to_1d(returns)
    arr = arr[np.isfinite(arr)]
    if arr.size < 2:
        return float("nan")
    rf_per_period = risk_free_rate_annual / periods_per_year
    excess = arr - rf_per_period
    downside = excess[excess < 0]
    if downside.size < 2:
        return float("nan")
    denom = np.std(downside, ddof=1)
    if denom <= 0:
        return float("nan")
    return float(np.mean(excess) / denom * math.sqrt(periods_per_year))


def information_ratio(
    portfolio_returns: Iterable[float] | np.ndarray,
    benchmark_returns: Iterable[float] | np.ndarray,
    *,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
) -> float:
    p = _to_1d(portfolio_returns)
    b = _to_1d(benchmark_returns)
    n = min(p.size, b.size)
    if n < 2:
        return float("nan")
    active = p[:n] - b[:n]
    active = active[np.isfinite(active)]
    if active.size < 2:
        return float("nan")
    denom = np.std(active, ddof=1)
    if denom <= 0:
        return float("nan")
    return float(np.mean(active) / denom * math.sqrt(periods_per_year))


def brier_score(y_true: Iterable[int] | np.ndarray, y_prob: Iterable[float] | np.ndarray) -> float:
    y = _to_1d(y_true)
    p = _to_1d(y_prob)
    n = min(y.size, p.size)
    if n == 0:
        return float("nan")
    y = y[:n]
    p = p[:n]
    mask = np.isfinite(y) & np.isfinite(p)
    if not np.any(mask):
        return float("nan")
    y = y[mask]
    p = np.clip(p[mask], 0.0, 1.0)
    return float(np.mean((y - p) ** 2))


def binary_log_loss(
    y_true: Iterable[int] | np.ndarray,
    y_prob: Iterable[float] | np.ndarray,
    *,
    eps: float = 1e-12,
) -> float:
    y = _to_1d(y_true)
    p = _to_1d(y_prob)
    n = min(y.size, p.size)
    if n == 0:
        return float("nan")
    y = y[:n]
    p = p[:n]
    mask = np.isfinite(y) & np.isfinite(p)
    if not np.any(mask):
        return float("nan")
    y = y[mask]
    p = np.clip(p[mask], eps, 1.0 - eps)
    return float(-np.mean((y * np.log(p)) + ((1.0 - y) * np.log(1.0 - p))))


def expected_calibration_error(
    y_true: Iterable[int] | np.ndarray,
    y_prob: Iterable[float] | np.ndarray,
    *,
    n_bins: int = 10,
) -> float:
    y = _to_1d(y_true)
    p = _to_1d(y_prob)
    n = min(y.size, p.size)
    if n == 0:
        return float("nan")

    y = y[:n]
    p = p[:n]
    mask = np.isfinite(y) & np.isfinite(p)
    if not np.any(mask):
        return float("nan")

    y = y[mask]
    p = np.clip(p[mask], 0.0, 1.0)
    bins = np.linspace(0.0, 1.0, n_bins + 1)

    ece = 0.0
    for idx in range(n_bins):
        low = bins[idx]
        high = bins[idx + 1]
        if idx == n_bins - 1:
            in_bin = (p >= low) & (p <= high)
        else:
            in_bin = (p >= low) & (p < high)

        if not np.any(in_bin):
            continue

        conf = float(np.mean(p[in_bin]))
        acc = float(np.mean(y[in_bin]))
        weight = float(np.mean(in_bin))
        ece += abs(acc - conf) * weight

    return float(ece)
