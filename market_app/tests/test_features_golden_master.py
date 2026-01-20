from pathlib import Path

import pandas as pd
import pytest

from market_monitor.features import compute_features


def test_golden_master_features_for_aaa() -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "nasdaq_daily" / "AAA.csv"
    df = pd.read_csv(fixture)
    features = compute_features(df)

    assert features["ret_1m"] == pytest.approx(0.07046979865771807, rel=1e-6)
    assert features["ret_3m"] == pytest.approx(0.24609375, rel=1e-6)
    assert features["sma20_ratio"] == pytest.approx(0.030694668820678617, rel=1e-6)
    assert features["sma50_ratio"] == pytest.approx(0.08319185059422751, rel=1e-6)
    assert features["vol20_ann"] == pytest.approx(0.0009843509980651415, rel=1e-6)
    assert features["max_drawdown_6m"] == pytest.approx(0.0, abs=1e-12)
    assert features["adv20_dollar"] == pytest.approx(159_500_000.0, rel=1e-9)
    assert features["zero_volume_frac"] == pytest.approx(0.0, abs=1e-12)
