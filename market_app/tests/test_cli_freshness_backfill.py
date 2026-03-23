from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from market_monitor.cli import _ensure_scored_freshness_columns


def test_ensure_scored_freshness_backfills_from_ohlcv(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "r1"
    run_dir.mkdir(parents=True)

    scored = pd.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "score": [1.2, 0.9],
            "rank": [1, 2],
        }
    )
    scored.to_csv(run_dir / "scored.csv", index=False)

    pd.DataFrame({"symbol": ["AAA", "BBB"], "asof_date": ["2025-01-31", "2025-01-31"]}).to_csv(
        run_dir / "results.csv", index=False
    )

    ohlcv_dir = tmp_path / "ohlcv_daily"
    ohlcv_dir.mkdir()

    pd.DataFrame(
        {
            "date": ["2025-01-28", "2025-01-30"],
            "open": [10, 11],
            "high": [11, 12],
            "low": [9, 10],
            "close": [10.5, 11.5],
            "volume": [1000, 1100],
        }
    ).to_csv(ohlcv_dir / "AAA.csv", index=False)

    pd.DataFrame(
        {
            "date": ["2025-01-20", "2025-01-27"],
            "open": [20, 21],
            "high": [21, 22],
            "low": [19, 20],
            "close": [20.5, 21.5],
            "volume": [2000, 2100],
        }
    ).to_csv(ohlcv_dir / "BBB.csv", index=False)

    snapshot = {
        "paths": {
            "ohlcv_daily_dir": str(ohlcv_dir),
        }
    }
    (run_dir / "config_snapshot.yaml").write_text(yaml.safe_dump(snapshot), encoding="utf-8")

    _ensure_scored_freshness_columns(run_dir)

    merged = pd.read_csv(run_dir / "scored.csv")
    assert "last_date" in merged.columns
    assert "lag_days" in merged.columns

    aaa = merged.loc[merged["symbol"] == "AAA"].iloc[0]
    bbb = merged.loc[merged["symbol"] == "BBB"].iloc[0]

    assert aaa["last_date"] == "2025-01-30"
    assert int(aaa["lag_days"]) == 1
    assert bbb["last_date"] == "2025-01-27"
    assert int(bbb["lag_days"]) == 4


def test_ensure_scored_freshness_backfills_from_paths_ohlcv_dir(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "r2"
    run_dir.mkdir(parents=True)

    pd.DataFrame({"symbol": ["AAA"], "score": [1.0], "rank": [1]}).to_csv(
        run_dir / "scored.csv", index=False
    )
    pd.DataFrame({"symbol": ["AAA"], "asof_date": ["2025-01-31"]}).to_csv(
        run_dir / "results.csv", index=False
    )

    ohlcv_dir = tmp_path / "ohlcv_from_paths_ohlcv_dir"
    ohlcv_dir.mkdir()
    pd.DataFrame(
        {
            "date": ["2025-01-29", "2025-01-30"],
            "open": [10, 11],
            "high": [11, 12],
            "low": [9, 10],
            "close": [10.5, 11.5],
            "volume": [1000, 1100],
        }
    ).to_csv(ohlcv_dir / "AAA.csv", index=False)

    snapshot = {"paths": {"ohlcv_dir": str(ohlcv_dir)}}
    (run_dir / "config_snapshot.yaml").write_text(yaml.safe_dump(snapshot), encoding="utf-8")

    _ensure_scored_freshness_columns(run_dir)

    merged = pd.read_csv(run_dir / "scored.csv")
    row = merged.iloc[0]
    assert row["last_date"] == "2025-01-30"
    assert int(row["lag_days"]) == 1


def test_ensure_scored_freshness_backfills_from_parquet_ohlcv(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "r3"
    run_dir.mkdir(parents=True)

    pd.DataFrame({"symbol": ["AAA"], "score": [1.0], "rank": [1]}).to_csv(
        run_dir / "scored.csv", index=False
    )
    pd.DataFrame({"symbol": ["AAA"], "asof_date": ["2025-01-31"]}).to_csv(
        run_dir / "results.csv", index=False
    )

    ohlcv_dir = tmp_path / "ohlcv_parquet"
    ohlcv_dir.mkdir()
    try:
        pd.DataFrame(
            {
                "date": ["2025-01-29", "2025-01-30"],
                "open": [10, 11],
                "high": [11, 12],
                "low": [9, 10],
                "close": [10.5, 11.5],
                "volume": [1000, 1100],
            }
        ).to_parquet(ohlcv_dir / "AAA.parquet", index=False)
    except ImportError:
        pytest.skip("Parquet engine not available.")

    snapshot = {"paths": {"ohlcv_daily_dir": str(ohlcv_dir)}}
    (run_dir / "config_snapshot.yaml").write_text(yaml.safe_dump(snapshot), encoding="utf-8")

    _ensure_scored_freshness_columns(run_dir)

    merged = pd.read_csv(run_dir / "scored.csv")
    row = merged.iloc[0]
    assert row["last_date"] == "2025-01-30"
    assert int(row["lag_days"]) == 1

