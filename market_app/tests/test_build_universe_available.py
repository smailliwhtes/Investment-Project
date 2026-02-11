from __future__ import annotations

from pathlib import Path

import pandas as pd

from market_app.tools.build_universe_available import build_universe_available


def test_build_universe_available_outputs_present_and_missing(tmp_path: Path) -> None:
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()
    (ohlcv_dir / "AAPL.csv").write_text("date,open,high,low,close,volume\n2026-01-01,1,1,1,1,1\n", encoding="utf-8")

    universe_in = tmp_path / "universe.csv"
    universe_in.write_text("Ticker,Name\nAAPL,Apple\nMSFT,Microsoft\n", encoding="utf-8")

    out_dir = tmp_path / "out"
    available_path, missing_path = build_universe_available(ohlcv_dir, universe_in, out_dir)

    available = pd.read_csv(available_path)
    missing = pd.read_csv(missing_path)

    assert available["Ticker"].tolist() == ["AAPL"]
    assert missing["Ticker"].tolist() == ["MSFT"]
