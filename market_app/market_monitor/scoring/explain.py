from __future__ import annotations

from typing import Iterable

import numpy as np

from market_monitor.scoring.schema import GateDecision, ScoreResult


def _is_nan(value: float | None) -> bool:
    return value is None or (isinstance(value, float) and np.isnan(value))


def _format_pct(value: float | None) -> str:
    if _is_nan(value):
        return "NA"
    return f"{value * 100:.1f}%"


def _format_float(value: float | None) -> str:
    if _is_nan(value):
        return "NA"
    return f"{value:.2f}"


def _pick_exogenous_spike(exogenous: dict[str, float | str | None] | None) -> str | None:
    if not exogenous:
        return None
    candidates = []
    for key, value in exogenous.items():
        if not isinstance(value, (int, float)):
            continue
        if "z" in key.lower() and not np.isnan(value) and abs(value) >= 2.0:
            candidates.append((abs(value), key, float(value)))
    if not candidates:
        return None
    _, key, value = sorted(candidates, reverse=True)[0]
    return f"exogenous spike: {key} z-score {value:.2f}"


def build_explanations(
    *,
    symbol: str,
    gate: GateDecision,
    score: ScoreResult,
    returns_20d: float | None,
    vol_20d: float | None,
    trend_50: float | None,
    rsi_14: float | None,
    avg_dollar_vol: float | None,
    regime_label: str,
    exogenous: dict[str, float | str | None] | None = None,
) -> list[str]:
    reasons: list[str] = []
    if gate.failed_gates:
        reasons.append("Gate failure: " + ", ".join(gate.failed_gates))

    if not _is_nan(returns_20d):
        if returns_20d > 0.05:
            reasons.append(f"20d momentum positive ({_format_pct(returns_20d)})")
        elif returns_20d < -0.05:
            reasons.append(f"20d momentum weak ({_format_pct(returns_20d)})")

    if not _is_nan(trend_50):
        if trend_50 > 0:
            reasons.append(f"Price above 50d average ({_format_pct(trend_50)})")
        else:
            reasons.append(f"Price below 50d average ({_format_pct(trend_50)})")

    if not _is_nan(vol_20d):
        reasons.append(f"20d volatility {vol_20d:.2f}")

    if not _is_nan(rsi_14):
        if rsi_14 >= 70:
            reasons.append("RSI indicates stretched momentum")
        elif rsi_14 <= 30:
            reasons.append("RSI indicates oversold pressure")

    if not _is_nan(avg_dollar_vol):
        reasons.append(f"Avg dollar volume {avg_dollar_vol:,.0f}")

    if regime_label == "risk_off":
        reasons.append("Regime risk_off penalty applied")
    elif regime_label == "risk_on":
        reasons.append("Regime risk_on support")

    exo_reason = _pick_exogenous_spike(exogenous)
    if exo_reason:
        reasons.append(exo_reason)

    components_sorted = sorted(score.components.items(), key=lambda item: abs(item[1]), reverse=True)
    for name, value in components_sorted[:2]:
        if name == "momentum" and abs(value) >= 0.3:
            reasons.append("Momentum component contributed materially")
        if name == "stability" and abs(value) >= 0.3:
            reasons.append("Stability component influenced score")
        if name == "liquidity" and abs(value) >= 0.3:
            reasons.append("Liquidity component influenced score")

    unique_reasons = []
    seen = set()
    for reason in reasons:
        if reason and reason not in seen:
            unique_reasons.append(reason)
            seen.add(reason)

    if len(unique_reasons) < 3:
        unique_reasons.append(f"Priority score set to {score.score}")

    return unique_reasons[:6]
