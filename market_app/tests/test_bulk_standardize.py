from pathlib import Path

import pandas as pd

from market_monitor.bulk.standardize import standardize_ohlcv_csv, standardize_timeseries_csv


def test_standardize_ohlcv_csv(tmp_path: Path):
    raw_path = tmp_path / "raw.csv"
    raw_path.write_text(
        "date,open,high,low,close,volume\n2024-01-01,1,2,0.5,1.5,100\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "out.csv"

    result = standardize_ohlcv_csv(raw_path, output_path)

    df = pd.read_csv(output_path)
    assert result.rows == 1
    assert list(df.columns) == ["Date", "Open", "High", "Low", "Close", "Volume"]


def test_standardize_timeseries_csv(tmp_path: Path):
    raw_path = tmp_path / "gov.csv"
    raw_path.write_text(
        "DATE,VALUE\n2024-01-01,3.5\n2024-01-02,3.6\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "out.csv"

    result = standardize_timeseries_csv(raw_path, output_path)

    df = pd.read_csv(output_path)
    assert result.rows == 2
    assert list(df.columns) == ["Date", "Value"]
