from __future__ import annotations

from dataclasses import dataclass


FEATURE_METRICS = [
    "returns_20d",
    "vol_20d",
    "trend_50",
    "rsi_14",
    "avg_dollar_vol",
]


@dataclass(frozen=True)
class GateDecision:
    passed: bool
    failed_gates: list[str]
    metrics: dict[str, float | None]
    risk_flags: list[str]


@dataclass(frozen=True)
class ScoreResult:
    score: int
    components: dict[str, float]
    regime_adjustment: float
