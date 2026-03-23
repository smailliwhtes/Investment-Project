from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from market_app.ohlcv_local import load_ohlcv


def test_ohlcv_loader_missing_volume(tmp_path: Path) -> None:
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    content = "\n".join(
        [
            "date,open,high,low,close",
            "2024-01-02,10,11,9,10.5",
            "2024-01-03,10.5,12,10,11.5",
        ]
    )
    (ohlcv_dir / "TEST.csv").write_text(content, encoding="utf-8")
    result = load_ohlcv("TEST", ohlcv_dir)
    assert result.missing_volume is True
    assert "volume" in result.frame.columns


def test_ohlcv_loader_reads_parquet(tmp_path: Path) -> None:
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    try:
        pd.DataFrame(
            {
                "Date": ["2024-01-03", "2024-01-02"],
                "Open": [10.5, 10.0],
                "High": [12.0, 11.0],
                "Low": [10.0, 9.0],
                "Close": [11.5, 10.5],
                "Volume": [1100, 1000],
            }
        ).to_parquet(ohlcv_dir / "TEST.parquet", index=False)
    except ImportError:
        pytest.skip("Parquet engine not available.")

    result = load_ohlcv("TEST", ohlcv_dir)

    assert result.source_path is not None
    assert result.source_path.suffix == ".parquet"
    assert result.missing_data is False
    assert result.frame["date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-01-03"]
