from pathlib import Path

import pandas as pd

from market_monitor.run_watchlist import _load_exogenous_features


def test_load_exogenous_features_prefers_join_ready_csv(tmp_path: Path) -> None:
    join_ready = tmp_path / "gdelt_daily_join_ready.csv"
    join_ready_df = pd.DataFrame(
        [
            {"day": "2026-03-08", "tone_mean_lag_1": 1.23, "mentions_roll7_sum": 10.0},
            {"day": "2026-03-07", "tone_mean_lag_1": 0.50, "mentions_roll7_sum": 8.0},
        ]
    )
    join_ready_df.to_csv(join_ready, index=False)

    # This should be ignored when join-ready exists.
    (tmp_path / "aaa_other.csv").write_text("day,tone_mean_lag_1\n2026-03-08,999\n", encoding="utf-8")

    row, meta = _load_exogenous_features(tmp_path, "2026-03-08", include_raw=True)

    assert meta["coverage"] == 1
    assert row["tone_mean_lag_1"] == 1.23
    assert row["mentions_roll7_sum"] == 10.0


def test_load_exogenous_features_accepts_direct_csv_path(tmp_path: Path) -> None:
    csv_path = tmp_path / "gdelt_daily_join_ready.csv"
    pd.DataFrame(
        [
            {"date": "2026-03-08", "signal_lag_3": 3.14, "signal_roll7_mean": 2.72},
            {"date": "2026-03-07", "signal_lag_3": 1.00, "signal_roll7_mean": 0.90},
        ]
    ).to_csv(csv_path, index=False)

    row, meta = _load_exogenous_features(csv_path, "2026-03-08", include_raw=False)

    assert meta["coverage"] == 1
    assert "date" not in row
    assert row["signal_lag_3"] == 3.14
    assert row["signal_roll7_mean"] == 2.72


def test_load_exogenous_features_accepts_parquet_partition(tmp_path: Path) -> None:
    try:
        partition_dir = tmp_path / "day=2026-03-08"
        partition_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {"day": "2026-03-08", "signal_lag_1": 1.5, "signal_roll7_sum": 4.0},
            ]
        ).to_parquet(partition_dir / "part-00000.parquet", index=False)
    except ImportError:
        return

    row, meta = _load_exogenous_features(tmp_path, "2026-03-08", include_raw=False)

    assert meta["coverage"] == 1
    assert row["signal_lag_1"] == 1.5
    assert row["signal_roll7_sum"] == 4.0
