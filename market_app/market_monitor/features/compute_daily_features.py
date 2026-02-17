from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from market_monitor.features.io import read_ohlcv, write_features, build_features_manifest
from market_monitor.features.schema import FEATURE_COLUMNS


def _pct_return(series: pd.Series, days: int) -> float | None:
    if len(series) <= days:
        return np.nan
    return float(series.iloc[-1] / series.iloc[-1 - days] - 1.0)


def _rolling_vol(returns: pd.Series, window: int) -> float | None:
    if len(returns) < window:
        return np.nan
    return float(returns.tail(window).std())


def _atr(df: pd.DataFrame, window: int) -> float | None:
    if len(df) <= window:
        return np.nan
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return float(tr.tail(window).mean())


def _rsi(series: pd.Series, window: int) -> float | None:
    if len(series) <= window:
        return np.nan
    delta = series.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.tail(window).mean()
    avg_loss = losses.tail(window).mean()
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def _sma(series: pd.Series, window: int) -> float | None:
    if len(series) < window:
        return np.nan
    return float(series.tail(window).mean())


def _volume_z(volume: pd.Series, window: int) -> float | None:
    if len(volume) < window:
        return np.nan
    tail = volume.tail(window)
    mean = tail.mean()
    std = tail.std()
    if std == 0 or np.isnan(std):
        return np.nan
    return float((tail.iloc[-1] - mean) / std)


def _max_drawdown(series: pd.Series, window: int) -> float | None:
    if len(series) < 2:
        return np.nan
    tail = series.tail(window) if len(series) >= window else series
    running_max = tail.cummax()
    drawdown = (tail / running_max) - 1.0
    return float(drawdown.min())


def compute_features_for_symbol(symbol: str, df: pd.DataFrame, asof_date: str) -> dict:
    df = df.copy()
    df = df[df["date"] <= pd.to_datetime(asof_date)].sort_values("date")
    if df.empty:
        return {"symbol": symbol, "asof_date": asof_date}

    close = df["close"].astype(float)
    returns = close.pct_change().dropna()
    volume = df["volume"].astype(float) if "volume" in df.columns else pd.Series(dtype=float)

    history_days = len(df)
    last_close = float(close.iloc[-1])

    sma_20 = _sma(close, 20)
    sma_50 = _sma(close, 50)
    trend_50 = np.nan
    if sma_50 and not np.isnan(sma_50):
        trend_50 = float(last_close / sma_50 - 1.0)

    avg_dollar_vol = np.nan
    if "volume" in df.columns and len(volume) >= 20:
        avg_dollar_vol = float((close.tail(20).reset_index(drop=True) * volume.tail(20).reset_index(drop=True)).mean())

    return {
        "symbol": symbol,
        "asof_date": asof_date,
        "returns_1d": _pct_return(close, 1),
        "returns_5d": _pct_return(close, 5),
        "returns_20d": _pct_return(close, 20),
        "vol_20d": _rolling_vol(returns, 20),
        "atr_14": _atr(df, 14),
        "rsi_14": _rsi(close, 14),
        "sma_20": sma_20,
        "sma_50": sma_50,
        "trend_50": trend_50,
        "volume_z_20": _volume_z(volume, 20) if "volume" in df.columns else np.nan,
        "max_drawdown_252": _max_drawdown(close, 252),
        "avg_dollar_vol": avg_dollar_vol,
        "history_days": history_days,
        "last_close": last_close,
    }


def compute_daily_features(
    *,
    ohlcv_dir: Path,
    out_dir: Path,
    asof_date: str,
    workers: int = 1,
) -> dict:
    paths = sorted(ohlcv_dir.glob("*.csv"))
    # Skip non-OHLCV files (e.g. conversion_errors.csv, ohlcv_manifest.csv)
    _SKIP_STEMS = {"conversion_errors", "ohlcv_manifest"}
    paths = [p for p in paths if p.stem.lower() not in _SKIP_STEMS]

    def _compute(path: Path) -> dict:
        symbol = path.stem.upper()
        df = read_ohlcv(path)
        return compute_features_for_symbol(symbol, df, asof_date)

    rows: list[dict] = []
    if workers <= 1 or len(paths) <= 1:
        for path in paths:
            rows.append(_compute(path))
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_compute, path): path for path in paths}
            for future in concurrent.futures.as_completed(futures):
                rows.append(future.result())

    rows = sorted(rows, key=lambda row: row.get("symbol") or "")

    output_path = out_dir / "features_by_symbol.csv"
    write_features(output_path, rows)

    ohlcv_manifest = ohlcv_dir / "ohlcv_manifest.json"
    manifest = build_features_manifest(
        output_dir=out_dir,
        ohlcv_manifest_path=ohlcv_manifest if ohlcv_manifest.exists() else None,
        feature_rows_path=output_path,
    )
    return {
        "features_path": output_path,
        "manifest": manifest,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compute deterministic daily features.")
    parser.add_argument("--ohlcv-dir", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--asof", required=True)
    parser.add_argument("--workers", type=int, default=1)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    compute_daily_features(
        ohlcv_dir=Path(args.ohlcv_dir),
        out_dir=Path(args.out_dir),
        asof_date=args.asof,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
