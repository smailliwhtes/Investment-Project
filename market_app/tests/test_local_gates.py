from __future__ import annotations

import pandas as pd

from market_app.scoring_local import apply_gates, build_risk_flags


def test_gates_and_risk_flags() -> None:
    config = {
        "gates": {
            "min_history_days": 252,
            "min_adv20_usd": 1_000_000,
            "price_floor": 1.0,
        },
        "risk_thresholds": {
            "extreme_volatility": 0.6,
            "deep_drawdown": -0.4,
            "tail_risk": -0.2,
        },
    }
    features = pd.DataFrame(
        [
            {
                "symbol": "LOW",
                "history_days": 100,
                "adv20_usd": 10_000,
                "sma20": 0.5,
                "close_to_sma20": 1.0,
                "volatility_60d": 0.8,
                "max_drawdown_6m": -0.5,
                "worst_5d_return_6m": -0.3,
                "stale_data": True,
                "theme_uncertain": True,
                "volume_missing": True,
            }
        ]
    )
    eligible = apply_gates(features, config).eligible
    assert bool(eligible.loc[0, "eligible"]) is False
    flags, levels = build_risk_flags(features, config)
    assert "adv20_below_min" in flags.iloc[0]
    assert levels.iloc[0] == "RED"
