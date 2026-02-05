from pathlib import Path

import yaml

from market_monitor.run_watchlist import run_watchlist


def test_determinism_twice_same_bytes(tmp_path: Path) -> None:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    watchlist_path = fixtures_dir / "watchlists" / "watchlist_tiny.csv"
    ohlcv_daily_dir = fixtures_dir / "ohlcv_daily"
    exogenous_dir = fixtures_dir / "exogenous" / "daily_features"

    outputs_dir_a = tmp_path / "out_a"
    outputs_dir_b = tmp_path / "out_b"
    config_path = tmp_path / "config.yaml"
    config_payload = {
        "paths": {
            "watchlist_file": str(watchlist_path),
            "outputs_dir": str(outputs_dir_a),
            "ohlcv_daily_dir": str(ohlcv_daily_dir),
            "exogenous_daily_dir": str(exogenous_dir),
        },
        "pipeline": {
            "auto_normalize_ohlcv": False,
            "asof_default": "2025-01-31",
        },
        "scoring": {
            "minimum_history_days": 200,
            "price_floor": 1.0,
            "average_dollar_volume_floor": 1000000.0,
        },
    }
    config_path.write_text(yaml.safe_dump(config_payload), encoding="utf-8")

    def _run(outputs_dir: Path) -> str:
        args = type(
            "obj",
            (),
            {
                "config": str(config_path),
                "watchlist": str(watchlist_path),
                "asof": "2025-01-31",
                "run_id": "repeatable",
                "ohlcv_raw_dir": None,
                "ohlcv_daily_dir": str(ohlcv_daily_dir),
                "exogenous_daily_dir": str(exogenous_dir),
                "outputs_dir": str(outputs_dir),
                "include_raw_gdelt": False,
                "log_level": "INFO",
                "workers": 1,
                "profile": False,
            },
        )
        manifest = run_watchlist(args)
        return manifest["determinism_fingerprint"]

    hash_a = _run(outputs_dir_a)
    hash_b = _run(outputs_dir_b)

    assert hash_a == hash_b
