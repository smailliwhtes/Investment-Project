from __future__ import annotations

from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest

from market_monitor.features.io import read_ohlcv, resolve_ohlcv_path, write_ohlcv


def test_read_ohlcv_csv_parquet_parity(tmp_path: Path) -> None:
    source = pd.DataFrame(
        {
            "Date": ["2025-01-03", "2025-01-02", "2025-01-02"],
            "Open": [11.0, 10.0, 10.5],
            "High": [12.0, 11.0, 11.5],
            "Low": [10.0, 9.0, 9.5],
            "Close": [11.5, 10.5, 10.75],
            "Volume": [1100, 1000, 1200],
        }
    )
    csv_path = tmp_path / "AAPL.csv"
    parquet_path = tmp_path / "AAPL.parquet"
    source.to_csv(csv_path, index=False)
    try:
        source.to_parquet(parquet_path, index=False)
    except ImportError:
        pytest.skip("Parquet engine not available.")

    csv_df = read_ohlcv(csv_path)
    parquet_df = read_ohlcv(parquet_path)

    pdt.assert_frame_equal(csv_df, parquet_df)
    assert csv_df["date"].dt.strftime("%Y-%m-%d").tolist() == ["2025-01-02", "2025-01-03"]
    assert float(csv_df.iloc[0]["close"]) == pytest.approx(10.75)


def test_resolve_ohlcv_path_prefers_parquet_and_normalized_symbol(tmp_path: Path) -> None:
    csv_path = tmp_path / "BRK-B.csv"
    parquet_path = tmp_path / "BRKB.parquet"
    csv_path.write_text("date,open,high,low,close,volume\n2025-01-02,1,1,1,1,1\n", encoding="utf-8")
    try:
        pd.DataFrame(
            [{"date": "2025-01-02", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}]
        ).to_parquet(parquet_path, index=False)
    except ImportError:
        pytest.skip("Parquet engine not available.")

    resolved = resolve_ohlcv_path("BRK.B", tmp_path)
    assert resolved == parquet_path


def test_write_ohlcv_parquet_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "SPY.parquet"
    frame = pd.DataFrame(
        {
            "date": ["2025-01-02", "2025-01-03"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [1000, 1100],
        }
    )
    try:
        write_ohlcv(path, frame)
    except ImportError:
        pytest.skip("Parquet engine not available.")

    written = read_ohlcv(path)
    assert written["date"].dt.strftime("%Y-%m-%d").tolist() == ["2025-01-02", "2025-01-03"]
    assert written["close"].tolist() == [100.5, 101.5]
