import numpy as np
import pandas as pd

from tools.run_watchlist import compute_features


def _sample_df(rows: int = 300) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=rows, freq="D")
    close = pd.Series(np.linspace(100, 200, rows))
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": close * 0.99,
            "High": close * 1.01,
            "Low": close * 0.98,
            "Close": close,
            "Volume": np.full(rows, 1_500_000),
        }
    )
    return df


def test_compute_features_with_events():
    df = _sample_df()
    events = pd.DataFrame(
        {
            "date": [df["Date"].iloc[10], df["Date"].iloc[50]],
            "weight": [1.0, 2.0],
        }
    )
    features = compute_features("TEST", df, events=events, event_window=5)
    assert features["forecast_return_21d"] > 0
    assert 0 <= features["rsi_14"] <= 100
    assert features["trend_r2_63d"] > 0
    assert not np.isnan(features["event_impact_mean"])
    assert not np.isnan(features["event_impact_weighted"])
