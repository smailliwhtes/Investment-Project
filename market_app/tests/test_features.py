import numpy as np
import pandas as pd
import pytest

from market_monitor.features import compute_features


def test_compute_features_basic():
    df = pd.read_csv("tests/fixtures/ohlcv.csv")
    features = compute_features(df)
    assert features["history_days"] > 150
    assert features["ret_1m"] is not None
    assert features["adv20_dollar"] > 0
    assert features["volume_available"] == 1.0
    assert "trend_quality_6m" in features


def test_feature_windows_and_liquidity():
    dates = pd.bdate_range("2022-01-03", periods=300)
    close = np.linspace(100, 200, 300)
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": close * 0.99,
            "High": close * 1.01,
            "Low": close * 0.98,
            "Close": close,
            "Volume": np.full(300, 1_250_000),
        }
    )
    features = compute_features(df)
    assert features["ret_1m"] == pytest.approx(close[-1] / close[-22] - 1.0, rel=1e-6)
    assert features["ret_3m"] == pytest.approx(close[-1] / close[-64] - 1.0, rel=1e-6)
    assert features["ret_6m"] == pytest.approx(close[-1] / close[-127] - 1.0, rel=1e-6)
    assert features["ret_12m"] == pytest.approx(close[-1] / close[-253] - 1.0, rel=1e-6)
    assert features["adv20_dollar"] == pytest.approx(close[-1] * 1_250_000, rel=1e-6)
    assert features["max_drawdown_6m"] == pytest.approx(0.0, abs=1e-12)
    assert features["vol20_ann"] >= 0.0
