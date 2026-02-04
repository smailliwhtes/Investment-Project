from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np

from market_monitor.scoring.schema import ScoreResult


@dataclass(frozen=True)
class ScoreConfig:
    base_score: float
    weight_momentum: float
    weight_trend: float
    weight_stability: float
    weight_liquidity: float
    regime_risk_off_penalty: float
    regime_risk_on_bonus: float
    vol_target: float
    liquidity_target: float


def _safe(value: float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    return float(value)


def _clip(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


def _round_half_away_from_zero(value: float) -> int:
    if value >= 0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


def compute_score(
    *,
    returns_20d: float | None,
    trend_50: float | None,
    vol_20d: float | None,
    avg_dollar_vol: float | None,
    regime_label: str,
    config: ScoreConfig,
) -> ScoreResult:
    momentum = _safe(returns_20d) or 0.0
    trend = _safe(trend_50) or 0.0

    momentum_component = _clip(momentum * 5.0, -1.0, 1.0)
    trend_component = _clip(trend * 5.0, -1.0, 1.0)

    vol_value = _safe(vol_20d)
    if vol_value is None:
        stability_component = 0.0
    else:
        stability_component = _clip(1.0 - (vol_value / config.vol_target), -1.0, 1.0)

    adv_value = _safe(avg_dollar_vol)
    if adv_value is None or adv_value <= 0:
        liquidity_component = 0.0
    else:
        liquidity_component = _clip(math.log10(adv_value / config.liquidity_target), -1.0, 1.0)

    regime_adjustment = 0.0
    if regime_label == "risk_off":
        regime_adjustment = -abs(config.regime_risk_off_penalty)
    elif regime_label == "risk_on":
        regime_adjustment = abs(config.regime_risk_on_bonus)

    raw_score = (
        config.base_score
        + config.weight_momentum * momentum_component
        + config.weight_trend * trend_component
        + config.weight_stability * stability_component
        + config.weight_liquidity * liquidity_component
        + regime_adjustment
    )

    score = _round_half_away_from_zero(raw_score)
    score = int(_clip(score, 1.0, 10.0))

    return ScoreResult(
        score=score,
        components={
            "momentum": momentum_component,
            "trend": trend_component,
            "stability": stability_component,
            "liquidity": liquidity_component,
        },
        regime_adjustment=regime_adjustment,
    )
