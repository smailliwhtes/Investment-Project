import json
from pathlib import Path

import pandas as pd

from market_monitor.gdelt.utils import build_content_hash
from market_monitor.ohlcv_doctor import normalize_directory


def test_ohlcv_doctor_golden(tmp_path: Path) -> None:
    raw_dir = Path(__file__).resolve().parent / "fixtures" / "ohlcv_raw"
    out_dir = tmp_path / "ohlcv_daily"

    result = normalize_directory(
        raw_dir=raw_dir,
        out_dir=out_dir,
        date_col=None,
        delimiter=None,
        symbol_from_filename=True,
        coerce=True,
        strict=False,
        streaming=True,
        chunk_rows=2,
    )

    manifest_path = result["manifest_path"]
    assert manifest_path.exists()

    golden_dir = Path(__file__).resolve().parent / "fixtures" / "ohlcv_daily_goldens"
    for symbol in ["AAA", "BBB", "SPY"]:
        actual = pd.read_csv(out_dir / f"{symbol}.csv")
        expected = pd.read_csv(golden_dir / f"{symbol}.csv")
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "content_hash" in manifest
    assert manifest["content_hash"] == build_content_hash({"symbols": manifest["symbols"]})
