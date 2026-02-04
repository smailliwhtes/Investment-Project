from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np

from market_monitor.scoring.schema import GateDecision


@dataclass(frozen=True)
class GateConfig:
    minimum_history_days: int
    price_floor: float
    average_dollar_volume_floor: float | None
    max_vol_20d_cap: float | None


GATE_CODES = {
    "missing_ohlcv": "MISSING_OHLC",
    "history_lt_min": "HISTORY_LT_MIN",
    "price_lt_floor": "PRICE_LT_FLOOR",
    "liquidity_lt_floor": "LIQUIDITY_LT_FLOOR",
    "volatility_gt_cap": "VOLATILITY_GT_CAP",
}


RISK_FLAGS = {
    "volume_missing": "volume_missing",
    "high_volatility": "high_volatility",
    "deep_drawdown": "deep_drawdown",
}


def _is_nan(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and np.isnan(value))


def apply_gates(
    *,
    has_ohlcv: bool,
    history_days: int,
    last_close: float | None,
    avg_dollar_vol: float | None,
    vol_20d: float | None,
    max_drawdown_252: float | None,
    config: GateConfig,
) -> GateDecision:
    failed: list[str] = []
    metrics = {
        "history_days": float(history_days),
        "last_close": last_close,
        "avg_dollar_vol": avg_dollar_vol,
        "vol_20d": vol_20d,
        "max_drawdown_252": max_drawdown_252,
    }
    if not has_ohlcv:
        failed.append(GATE_CODES["missing_ohlcv"])

    if history_days < config.minimum_history_days:
        failed.append(GATE_CODES["history_lt_min"])

    if last_close is None or _is_nan(last_close) or last_close < config.price_floor:
        failed.append(GATE_CODES["price_lt_floor"])

    if config.average_dollar_volume_floor is not None and not _is_nan(avg_dollar_vol):
        if avg_dollar_vol < config.average_dollar_volume_floor:
            failed.append(GATE_CODES["liquidity_lt_floor"])

    if config.max_vol_20d_cap is not None and not _is_nan(vol_20d):
        if vol_20d > config.max_vol_20d_cap:
            failed.append(GATE_CODES["volatility_gt_cap"])

    risk_flags: list[str] = []
    if _is_nan(avg_dollar_vol):
        risk_flags.append(RISK_FLAGS["volume_missing"])
    if not _is_nan(vol_20d) and config.max_vol_20d_cap is not None and vol_20d > config.max_vol_20d_cap:
        risk_flags.append(RISK_FLAGS["high_volatility"])
    if not _is_nan(max_drawdown_252) and max_drawdown_252 < -0.4:
        risk_flags.append(RISK_FLAGS["deep_drawdown"])

    return GateDecision(passed=len(failed) == 0, failed_gates=failed, metrics=metrics, risk_flags=risk_flags)


def join_pipe(values: Iterable[str]) -> str:
    cleaned = [value for value in values if value]
    return "|".join(cleaned)
