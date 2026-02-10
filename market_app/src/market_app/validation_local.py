from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from market_app.local_config import ConfigResult
from market_app.ohlcv_local import load_ohlcv, resolve_ohlcv_dir
from market_app.symbols_local import load_symbols


@dataclass(frozen=True)
class ValidationReport:
    payload: dict[str, Any]
    human_summary: str
    exit_code: int


def _resolve_anchor_date(config: dict[str, Any]) -> date | None:
    as_of_date = config.get("run", {}).get("as_of_date")
    if not as_of_date:
        return None
    parsed = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def run_validation(config_result: ConfigResult) -> ValidationReport:
    config = config_result.config
    symbols_dir = Path(config["paths"].get("symbols_dir", "") or "")
    ohlcv_dir = Path(config["paths"].get("ohlcv_dir", "") or "")

    symbol_result = load_symbols(symbols_dir, config, logger=_NullLogger())
    ohlcv_dir = resolve_ohlcv_dir(ohlcv_dir, logger=_NullLogger())

    missing_symbols: list[str] = []
    duplicate_dates: list[str] = []
    non_monotonic: list[str] = []
    missing_date_rows: list[str] = []
    missing_volume_symbols: list[str] = []
    stale_symbols: list[str] = []
    insufficient_history: list[str] = []
    column_mismatches: dict[str, list[str]] = {}

    anchor_date = _resolve_anchor_date(config)
    min_history_days = int(config.get("gates", {}).get("min_history_days", 252))
    max_lag_days = int(config.get("gates", {}).get("max_lag_days", 5))

    for symbol in symbol_result.symbols["symbol"].tolist():
        ohlcv = load_ohlcv(symbol, ohlcv_dir)
        if ohlcv.missing_data or ohlcv.frame.empty:
            if ohlcv.missing_required_columns:
                column_mismatches[symbol] = ohlcv.missing_required_columns
            missing_symbols.append(symbol)
            continue
        if ohlcv.duplicate_dates:
            duplicate_dates.append(symbol)
        if ohlcv.non_monotonic_dates:
            non_monotonic.append(symbol)
        if ohlcv.missing_date_rows:
            missing_date_rows.append(symbol)
        if ohlcv.missing_volume:
            missing_volume_symbols.append(symbol)
        history_days = len(ohlcv.frame)
        if history_days < min_history_days:
            insufficient_history.append(symbol)
        if anchor_date:
            latest = pd.to_datetime(ohlcv.frame["date"], errors="coerce").max()
            if not pd.isna(latest):
                lag = (anchor_date - latest.date()).days
                if lag > max_lag_days:
                    stale_symbols.append(symbol)

    payload = {
        "schema_version": 1,
        "symbols": {
            "total": int(len(symbol_result.symbols)),
            "missing_ohlcv": sorted(missing_symbols),
            "malformed_rows": symbol_result.errors,
        },
        "ohlcv": {
            "column_mismatches": column_mismatches,
            "duplicate_dates": sorted(duplicate_dates),
            "non_monotonic_dates": sorted(non_monotonic),
            "missing_date_rows": sorted(missing_date_rows),
            "missing_volume": sorted(missing_volume_symbols),
            "missing_volume_rate": float(
                len(missing_volume_symbols) / max(len(symbol_result.symbols), 1)
            ),
            "insufficient_history": sorted(insufficient_history),
            "stale_symbols": sorted(stale_symbols),
        },
        "config": {
            "as_of_date": config.get("run", {}).get("as_of_date"),
            "min_history_days": min_history_days,
            "max_lag_days": max_lag_days,
        },
    }

    issues = sum(
        [
            len(missing_symbols),
            len(duplicate_dates),
            len(non_monotonic),
            len(missing_date_rows),
            len(missing_volume_symbols),
            len(insufficient_history),
            len(stale_symbols),
            len(symbol_result.errors),
            len(column_mismatches),
        ]
    )
    summary_lines = [
        "[validate] Market App offline data validation",
        f"- symbols_total: {payload['symbols']['total']}",
        f"- missing_ohlcv: {len(missing_symbols)}",
        f"- malformed_symbol_rows: {len(symbol_result.errors)}",
        f"- column_mismatches: {len(column_mismatches)}",
        f"- duplicate_dates: {len(duplicate_dates)}",
        f"- non_monotonic_dates: {len(non_monotonic)}",
        f"- missing_date_rows: {len(missing_date_rows)}",
        f"- missing_volume: {len(missing_volume_symbols)}",
        f"- insufficient_history: {len(insufficient_history)}",
        f"- stale_symbols: {len(stale_symbols)}",
    ]
    exit_code = 0 if issues == 0 else 2
    return ValidationReport(
        payload=payload,
        human_summary="\n".join(summary_lines),
        exit_code=exit_code,
    )


class _NullLogger:
    def warning(self, *args, **kwargs) -> None:
        return None

    def info(self, *args, **kwargs) -> None:
        return None
