import pandas as pd

from market_monitor.features import compute_features


def test_compute_features_basic():
    df = pd.read_csv("tests/fixtures/ohlcv.csv")
    features = compute_features(df)
    assert features["history_days"] > 150
    assert features["ret_1m"] is not None
    assert features["adv20_dollar"] > 0
