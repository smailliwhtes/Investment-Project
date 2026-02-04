from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from market_monitor.features.io import read_ohlcv
from market_monitor.features.compute_daily_features import _rolling_vol, _sma


@dataclass(frozen=True)
class RegimeResult:
    regime_label: str
    benchmark_symbol: str | None
    benchmark_trend: float | None
    benchmark_vol: float | None
    cross_asset_hint: str | None
    issues: list[str]


def _trend_50(close: pd.Series) -> float | None:
    sma50 = _sma(close, 50)
    if sma50 is None or np.isnan(sma50):
        return np.nan
    return float(close.iloc[-1] / sma50 - 1.0)


def compute_regime(
    *,
    ohlcv_dir: Path,
    benchmarks: list[str],
    asof_date: str,
) -> RegimeResult:
    issues: list[str] = []
    benchmark_symbol = None
    benchmark_trend = np.nan
    benchmark_vol = np.nan
    cross_asset_hint = None

    available = []
    for symbol in benchmarks:
        path = ohlcv_dir / f"{symbol}.csv"
        if path.exists():
            available.append(symbol)
    if not available:
        issues.append("No benchmark symbols available in normalized OHLCV directory.")
        return RegimeResult(
            regime_label="unknown",
            benchmark_symbol=None,
            benchmark_trend=None,
            benchmark_vol=None,
            cross_asset_hint=None,
            issues=issues,
        )

    benchmark_symbol = available[0]
    df = read_ohlcv(ohlcv_dir / f"{benchmark_symbol}.csv")
    df = df[df["date"] <= pd.to_datetime(asof_date)]
    if df.empty or len(df) < 50:
        issues.append("Insufficient benchmark history for regime calculation.")
        return RegimeResult(
            regime_label="unknown",
            benchmark_symbol=benchmark_symbol,
            benchmark_trend=None,
            benchmark_vol=None,
            cross_asset_hint=None,
            issues=issues,
        )

    close = df["close"].astype(float)
    benchmark_trend = _trend_50(close)
    returns = close.pct_change().dropna()
    benchmark_vol = _rolling_vol(returns, 20)

    regime_label = "unknown"
    if benchmark_trend is not None and not np.isnan(benchmark_trend):
        regime_label = "risk_on" if benchmark_trend >= 0 else "risk_off"

    if len(available) >= 2 and "TLT" in available and benchmark_symbol != "TLT":
        tlt_df = read_ohlcv(ohlcv_dir / "TLT.csv")
        tlt_df = tlt_df[tlt_df["date"] <= pd.to_datetime(asof_date)]
        if len(tlt_df) >= 50:
            tlt_trend = _trend_50(tlt_df["close"].astype(float))
            if tlt_trend is not None and not np.isnan(tlt_trend):
                cross_asset_hint = f"{benchmark_symbol} trend {benchmark_trend:.2%} vs TLT {tlt_trend:.2%}"

    return RegimeResult(
        regime_label=regime_label,
        benchmark_symbol=benchmark_symbol,
        benchmark_trend=None if benchmark_trend is None or np.isnan(benchmark_trend) else float(benchmark_trend),
        benchmark_vol=None if benchmark_vol is None or np.isnan(benchmark_vol) else float(benchmark_vol),
        cross_asset_hint=cross_asset_hint,
        issues=issues,
    )
