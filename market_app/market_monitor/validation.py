from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from market_monitor.features.io import read_ohlcv
from market_monitor.ohlcv_utils import REQUIRED_COLUMNS
from market_monitor.scoring.gates import GATE_CODES
from market_monitor.universe import read_watchlist

REQUIRED_WATCHLIST_COLUMNS = {"symbol", "theme_bucket", "asset_type"}


@dataclass(frozen=True)
class ValidationResult:
    errors: list[str]
    warnings: list[str]
    missing_symbols: list[str]
    per_symbol_reasons: dict[str, list[str]]
    exogenous_gaps: list[str]
    benchmark_missing: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


def _ensure_upper(values: Iterable[str]) -> list[str]:
    return [value.strip().upper() for value in values if value and value.strip()]


def validate_watchlist(watchlist_path: Path) -> tuple[pd.DataFrame, list[str]]:
    errors: list[str] = []
    if not watchlist_path.exists():
        return pd.DataFrame(), [f"Watchlist is empty or missing at {watchlist_path}."]
    if watchlist_path.suffix.lower() != ".csv":
        watchlist = read_watchlist(watchlist_path)
        if watchlist.empty:
            errors.append(f"Watchlist is empty or missing at {watchlist_path}.")
        return watchlist, errors

    df = pd.read_csv(watchlist_path)
    headers = {col.lower(): col for col in df.columns}
    missing = REQUIRED_WATCHLIST_COLUMNS - set(headers.keys())
    if missing:
        errors.append(f"Watchlist missing required columns: {sorted(missing)}.")
        return pd.DataFrame(), errors
    df = df.rename(columns={headers["symbol"]: "symbol"})
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df = df[df["symbol"] != ""]
    if df.empty:
        errors.append(f"Watchlist is empty or missing at {watchlist_path}.")
        return df, errors
    duplicates = df["symbol"].duplicated()
    if duplicates.any():
        dupes = df.loc[duplicates, "symbol"].tolist()
        errors.append(f"Watchlist has duplicate symbols: {', '.join(sorted(set(dupes)))}.")
    return df, errors


def _validate_ohlcv_symbol(
    symbol: str, ohlcv_daily_dir: Path, asof_date: str, min_history_days: int
) -> tuple[list[str], int]:
    issues: list[str] = []
    path = ohlcv_daily_dir / f"{symbol}.csv"
    if not path.exists():
        return [GATE_CODES["missing_ohlcv"]], 0
    df = read_ohlcv(path)
    if df.empty:
        return [GATE_CODES["missing_ohlcv"]], 0
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        issues.append(f"Missing columns: {', '.join(missing_columns)}")
    df = df[df["date"] <= pd.to_datetime(asof_date)]
    history_days = len(df)
    if history_days < min_history_days:
        issues.append(GATE_CODES["history_lt_min"])
    return issues, history_days


def _exogenous_coverage(exogenous_dir: Path, asof_date: str) -> tuple[bool, list[str]]:
    if not exogenous_dir.exists():
        return False, [f"Exogenous daily dir missing: {exogenous_dir}"]
    day_partition = exogenous_dir / f"day={asof_date}" / "part-00000.csv"
    if day_partition.exists():
        return True, []
    csv_candidates = list(exogenous_dir.glob("*.csv"))
    if not csv_candidates:
        return False, [f"Exogenous daily dir has no CSVs: {exogenous_dir}"]
    df = pd.read_csv(csv_candidates[0])
    day_col = None
    for col in df.columns:
        if col.lower() in {"day", "date"}:
            day_col = col
            break
    if day_col:
        if df[df[day_col] == asof_date].empty:
            return False, [f"Exogenous cache missing asof date {asof_date}."]
        return True, []
    return False, [f"Exogenous cache missing day/date column in {csv_candidates[0].name}."]


def validate_data(
    *,
    watchlist_path: Path,
    ohlcv_daily_dir: Path,
    exogenous_daily_dir: Path,
    asof_date: str,
    min_history_days: int,
    benchmark_symbols: list[str],
    exogenous_enabled: bool = False,
) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []
    missing_symbols: list[str] = []
    per_symbol_reasons: dict[str, list[str]] = {}
    exogenous_gaps: list[str] = []
    benchmark_missing: list[str] = []

    watchlist_df, watchlist_errors = validate_watchlist(watchlist_path)
    errors.extend(watchlist_errors)

    symbols = _ensure_upper(watchlist_df["symbol"].tolist()) if not watchlist_df.empty else []

    if not ohlcv_daily_dir.exists():
        errors.append(f"OHLCV daily dir missing: {ohlcv_daily_dir}")
    else:
        for symbol in symbols:
            issues, _ = _validate_ohlcv_symbol(symbol, ohlcv_daily_dir, asof_date, min_history_days)
            if issues:
                per_symbol_reasons[symbol] = issues
                if GATE_CODES["missing_ohlcv"] in issues:
                    missing_symbols.append(symbol)
                if GATE_CODES["history_lt_min"] in issues:
                    errors.append(f"{symbol} has insufficient history (<{min_history_days} days).")
                for issue in issues:
                    if issue.startswith("Missing columns"):
                        errors.append(f"{symbol} OHLCV schema issue: {issue}.")

        if missing_symbols:
            errors.append(f"Missing OHLCV files for symbols: {', '.join(sorted(missing_symbols))}.")

    if exogenous_enabled and exogenous_daily_dir:
        ok, gaps = _exogenous_coverage(exogenous_daily_dir, asof_date)
        if not ok:
            errors.extend(gaps)
            exogenous_gaps.extend(gaps)

    if benchmark_symbols:
        for symbol in benchmark_symbols:
            path = ohlcv_daily_dir / f"{symbol}.csv"
            if not path.exists():
                benchmark_missing.append(symbol)
        if benchmark_missing:
            warnings.append(
                f"Benchmark symbols missing in OHLCV directory: {', '.join(sorted(benchmark_missing))}."
            )

    return ValidationResult(
        errors=errors,
        warnings=warnings,
        missing_symbols=sorted(set(missing_symbols)),
        per_symbol_reasons=per_symbol_reasons,
        exogenous_gaps=exogenous_gaps,
        benchmark_missing=benchmark_missing,
    )
