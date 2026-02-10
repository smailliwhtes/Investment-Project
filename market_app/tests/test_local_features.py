from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from market_app.features_local import compute_features


def test_feature_calc_constant_series() -> None:
    dates = pd.bdate_range(date(2023, 1, 2), periods=260)
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "volume": 1000,
        }
    )
    result = compute_features("CONST", frame, {"gates": {"max_lag_days": 5}})
    features = result.features
    assert features["volatility_20d"] == 0.0
    assert features["max_drawdown_6m"] == 0.0
