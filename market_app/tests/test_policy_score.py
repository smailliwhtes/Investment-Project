from __future__ import annotations

import pandas as pd

from market_monitor.policy_score import rank_policy_impacts


class _Provider:
    def load_symbol_data(self, symbol: str):  # noqa: ARG002
        frame = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", periods=30, freq="D"),
                "Close": [100.0 + idx for idx in range(30)],
                "Volume": [1_500_000.0 for _ in range(30)],
            }
        )
        return frame, {}


def test_policy_confidence_requires_symbol_evidence() -> None:
    simulation_summary = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "analog_count": 5,
                "simulation_basis": "empirical",
                "effective_sample_count_20d": 5,
                "median_return_20d": 0.03,
                "q10_return_20d": -0.02,
                "q90_return_20d": 0.05,
            },
            {
                "symbol": "BBB",
                "analog_count": 5,
                "simulation_basis": "synthetic_fallback",
                "effective_sample_count_20d": 0,
                "median_return_20d": 0.03,
                "q10_return_20d": -0.02,
                "q90_return_20d": 0.05,
            },
        ]
    )
    event_study = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "horizon_days": 1,
                "cumulative_abnormal_return": 0.02,
                "window_volatility": 0.01,
            },
            {
                "symbol": "AAA",
                "horizon_days": 20,
                "cumulative_abnormal_return": 0.01,
                "window_volatility": 0.01,
            },
        ]
    )

    ranked = rank_policy_impacts(
        simulation_summary,
        event_study,
        provider=_Provider(),
        as_of_date="2024-01-30",
        average_dollar_volume_floor=1_000_000.0,
        analog_count=5,
        top_n_analogs=5,
    ).set_index("symbol")

    assert ranked.loc["AAA", "confidence_score"] > 0
    assert ranked.loc["BBB", "confidence_score"] == 0
    assert ranked.loc["BBB", "effective_sample_count"] == 0
