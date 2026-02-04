from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from market_monitor.features.join_exogenous import build_joined_features


def _read_partition(root: Path, day: str) -> pd.DataFrame:
    return pd.read_parquet(root / f"day={day}" / "part-00000.parquet")


def test_join_exogenous_lags_no_leakage(tmp_path: Path) -> None:
    market_df = pd.DataFrame(
        {
            "day": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03"],
            "symbol": ["AAA", "BBB", "AAA", "AAA"],
            "market_feature": [1.0, 2.0, 3.0, 4.0],
        }
    )
    gdelt_df = pd.DataFrame(
        {
            "day": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "total_event_count": [10, 20, 30],
            "tone_mean": [0.1, 0.2, 0.3],
        }
    )

    market_path = tmp_path / "market.parquet"
    gdelt_path = tmp_path / "gdelt.parquet"
    market_df.to_parquet(market_path, index=False)
    gdelt_df.to_parquet(gdelt_path, index=False)

    out_dir = tmp_path / "joined"
    result = build_joined_features(
        market_path=market_path,
        gdelt_path=gdelt_path,
        out_dir=out_dir,
        lags=[1],
        rolling_window=2,
        rolling_mean=True,
        rolling_sum=True,
        rolling_min_periods=1,
    )

    day2 = _read_partition(out_dir, "2024-01-02")
    assert len(day2) == 1
    row = day2.iloc[0]
    assert row["symbol"] == "AAA"
    assert row["market_feature"] == 3.0
    assert row["total_event_count_lag_1"] == 10
    assert row["total_event_count_roll2_sum"] == 30
    assert row["total_event_count_roll2_mean"] == 15
    assert row["tone_mean_lag_1"] == 0.1
    assert "total_event_count" not in day2.columns

    day1 = _read_partition(out_dir, "2024-01-01")
    assert day1["total_event_count_lag_1"].isna().all()
    assert day1["total_event_count_roll2_sum"].iloc[0] == 10

    manifest = json.loads((out_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["coverage"]["min_day"] == "2024-01-01"
    assert manifest["coverage"]["max_day"] == "2024-01-03"
    assert manifest["row_counts"]["total_rows"] == 4
    assert result.partitions == 3
