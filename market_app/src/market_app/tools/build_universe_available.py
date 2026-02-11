from __future__ import annotations

from pathlib import Path

import pandas as pd


def detect_symbol_column(df: pd.DataFrame) -> str:
    for candidate in ("symbol", "Symbol", "ticker", "Ticker"):
        if candidate in df.columns:
            return candidate
    return str(df.columns[0])


def cached_symbols(ohlcv_dir: Path) -> set[str]:
    return {p.stem.upper() for p in sorted(ohlcv_dir.glob("*.csv"))}


def build_universe_available(ohlcv_dir: Path, universe_in: Path, out_dir: Path) -> tuple[Path, Path]:
    if not ohlcv_dir.exists():
        raise FileNotFoundError(f"OHLCV directory does not exist: {ohlcv_dir}")
    if not universe_in.exists():
        raise FileNotFoundError(f"Universe file does not exist: {universe_in}")

    out_dir.mkdir(parents=True, exist_ok=True)
    universe_df = pd.read_csv(universe_in)
    if universe_df.empty:
        raise ValueError(f"Universe file is empty: {universe_in}")

    symbol_col = detect_symbol_column(universe_df)
    normalized = universe_df[symbol_col].astype(str).str.strip().str.upper()
    universe_df = universe_df.assign(_symbol_normalized=normalized)
    present_mask = universe_df["_symbol_normalized"].isin(cached_symbols(ohlcv_dir))

    available_df = universe_df.loc[present_mask].drop(columns=["_symbol_normalized"])
    missing_df = universe_df.loc[~present_mask].drop(columns=["_symbol_normalized"])
    available_df = available_df.sort_values(symbol_col, kind="mergesort").reset_index(drop=True)
    missing_df = missing_df.sort_values(symbol_col, kind="mergesort").reset_index(drop=True)

    available_path = out_dir / "universe.csv"
    missing_path = out_dir / "universe_missing.csv"
    available_df.to_csv(available_path, index=False)
    missing_df.to_csv(missing_path, index=False)
    return available_path, missing_path
