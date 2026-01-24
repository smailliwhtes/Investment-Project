from __future__ import annotations

import csv
from pathlib import Path


def test_fixture_ohlcv_files_have_enough_rows() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_dir = repo_root / "tests" / "fixtures" / "ohlcv"
    expected_header = ["Date", "Open", "High", "Low", "Close", "Volume"]

    for symbol in ["AAA", "BBB", "SPY"]:
        path = fixture_dir / f"{symbol}.csv"
        assert path.exists()
        with path.open(encoding="utf-8") as handle:
            reader = csv.reader(handle)
            header = next(reader)
            assert header == expected_header
            rows = sum(1 for _ in reader)
        assert rows >= 300
