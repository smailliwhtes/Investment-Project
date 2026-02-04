from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from market_monitor.features.join_exogenous import build_joined_features


def _write_daily_cache(root: Path) -> None:
    daily_root = root / "daily_features"
    day_dir = daily_root / "day=2024-01-01"
    day_dir.mkdir(parents=True)
    pd.DataFrame({"day": ["2024-01-01"], "mentions": [10], "tone_mean": [0.2]}).to_csv(
        day_dir / "part-00000.csv", index=False
    )
    day_dir = daily_root / "day=2024-01-02"
    day_dir.mkdir(parents=True)
    pd.DataFrame({"day": ["2024-01-02"], "mentions": [20], "tone_mean": [0.3]}).to_csv(
        day_dir / "part-00000.csv", index=False
    )
    manifest = {
        "schema_version": 1,
        "coverage": {"min_day": "2024-01-01", "max_day": "2024-01-02", "n_days": 2},
        "row_counts": {"total_rows": 2, "rows_per_day": {"2024-01-01": 1, "2024-01-02": 1}},
        "schema": {"columns": ["day", "mentions", "tone_mean"], "dtypes": {}},
        "content_hash": "test",
    }
    (daily_root / "features_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )


def test_join_exogenous_with_doctor_cache(tmp_path: Path) -> None:
    gdelt_root = tmp_path / "gdelt"
    _write_daily_cache(gdelt_root)

    market = pd.DataFrame(
        {
            "day": ["2024-01-01", "2024-01-02"],
            "symbol": ["AAA", "AAA"],
            "market_feature": [1.0, 2.0],
        }
    )
    market_path = tmp_path / "market.csv"
    market.to_csv(market_path, index=False)

    out_dir = tmp_path / "joined"
    result = build_joined_features(
        market_path=market_path,
        gdelt_path=gdelt_root / "daily_features",
        out_dir=out_dir,
        lags=[1],
        rolling_window=2,
        rolling_mean=True,
        rolling_sum=True,
        output_format="csv",
    )

    day2 = pd.read_csv(out_dir / "day=2024-01-02" / "part-00000.csv")
    assert day2.loc[0, "mentions_lag_1"] == 10
    assert day2.loc[0, "mentions_roll2_sum"] == 30
    assert "mentions" not in day2.columns
    assert result.partitions == 2
