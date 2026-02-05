from pathlib import Path

import yaml

from market_monitor.hash_utils import hash_file
from market_monitor.run_watchlist import run_watchlist


def _run_with_workers(tmp_path: Path, workers: int) -> str:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    watchlist_path = fixtures_dir / "watchlists" / "watchlist_tiny.csv"
    ohlcv_daily_dir = fixtures_dir / "ohlcv_daily"
    exogenous_dir = fixtures_dir / "exogenous" / "daily_features"

    outputs_dir = tmp_path / f"out_{workers}"
    config_path = tmp_path / f"config_{workers}.yaml"
    config_payload = {
        "paths": {
            "watchlist_file": str(watchlist_path),
            "outputs_dir": str(outputs_dir),
            "ohlcv_daily_dir": str(ohlcv_daily_dir),
            "exogenous_daily_dir": str(exogenous_dir),
        },
        "pipeline": {
            "auto_normalize_ohlcv": False,
            "asof_default": "2025-01-31",
            "benchmarks": ["SPY"],
        },
        "scoring": {
            "minimum_history_days": 200,
            "price_floor": 1.0,
            "average_dollar_volume_floor": 1000000.0,
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
            "run_id": f"workers_{workers}",
            "ohlcv_raw_dir": None,
            "ohlcv_daily_dir": str(ohlcv_daily_dir),
            "exogenous_daily_dir": str(exogenous_dir),
            "outputs_dir": str(outputs_dir),
            "include_raw_gdelt": False,
            "log_level": "INFO",
            "workers": workers,
            "profile": False,
        },
    )
    run_watchlist(args)
    return hash_file(outputs_dir / f"workers_{workers}" / "results.csv")


def test_parallel_determinism(tmp_path: Path) -> None:
    hash_one = _run_with_workers(tmp_path, 1)
    hash_four = _run_with_workers(tmp_path, 4)
    assert hash_one == hash_four
