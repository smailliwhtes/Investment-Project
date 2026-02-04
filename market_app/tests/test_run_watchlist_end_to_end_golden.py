import json
from pathlib import Path

import pandas as pd
import yaml

from market_monitor.run_watchlist import run_watchlist


def test_run_watchlist_end_to_end_golden(tmp_path: Path) -> None:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    watchlist_path = fixtures_dir / "watchlists" / "watchlist_tiny.csv"
    ohlcv_daily_dir = fixtures_dir / "ohlcv_daily"
    exogenous_dir = fixtures_dir / "exogenous" / "daily_features"

    outputs_dir = tmp_path / "outputs"
    config_path = tmp_path / "config.yaml"
    config_payload = {
        "paths": {
            "watchlist_file": str(watchlist_path),
            "outputs_dir": str(outputs_dir),
            "ohlcv_daily_dir": str(ohlcv_daily_dir),
            "exogenous_daily_dir": str(exogenous_dir),
        },
        "pipeline": {
            "auto_normalize_ohlcv": False,
            "include_raw_exogenous_same_day": False,
            "asof_default": "2025-01-31",
            "benchmarks": ["SPY"],
        },
        "scoring": {
            "minimum_history_days": 200,
            "price_floor": 1.0,
            "average_dollar_volume_floor": 1000000.0,
            "max_vol_20d_cap": None,
        },
    }
    config_path.write_text(yaml.safe_dump(config_payload), encoding="utf-8")

    args = type(
        "obj",
        (),
        {
            "config": str(config_path),
            "watchlist": str(watchlist_path),
            "asof": "2025-01-31",
            "run_id": "golden",
            "ohlcv_raw_dir": None,
            "ohlcv_daily_dir": str(ohlcv_daily_dir),
            "exogenous_daily_dir": str(exogenous_dir),
            "outputs_dir": str(outputs_dir),
            "include_raw_gdelt": False,
        },
    )

    manifest = run_watchlist(args)
    results_path = outputs_dir / "golden" / "results.csv"
    assert results_path.exists()

    results_df = pd.read_csv(results_path)
    assert not results_df.empty
    assert list(results_df.columns[:4]) == ["symbol", "asof_date", "theme_bucket", "asset_type"]

    manifest_path = outputs_dir / "golden" / "run_manifest.json"
    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_data["run_id"] == "golden"
    assert manifest_data["determinism_fingerprint"] == manifest["determinism_fingerprint"]

    expected_hash = "19b56652b9a88a1565167e5b94bf4e48286014976e139c738340251c5ac62b8f"
    assert manifest_data["determinism_fingerprint"] == expected_hash
