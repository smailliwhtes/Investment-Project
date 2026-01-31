from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pandas as pd
import yaml

from market_monitor.cli import run_pipeline


def test_watchlist_smoke_pipeline(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    watchlist_path = repo_root / "watchlists" / "watchlist_smoke.csv"
    fixture_path = repo_root / "tests" / "fixtures" / "ohlcv.csv"

    data_dir = tmp_path / "ohlcv"
    data_dir.mkdir(parents=True, exist_ok=True)
    fixture_df = pd.read_csv(fixture_path)
    for symbol in ["SPY", "TLT", "GLD"]:
        fixture_df.to_csv(data_dir / f"{symbol}.csv", index=False)

    outputs_dir = tmp_path / "outputs"
    cache_dir = tmp_path / "cache"
    logs_dir = tmp_path / "logs"

    config_payload = {
        "data": {
            "offline_mode": True,
            "provider": "nasdaq_daily",
            "paths": {"nasdaq_daily_dir": str(data_dir)},
        },
        "paths": {
            "watchlist_file": str(watchlist_path),
            "outputs_dir": str(outputs_dir),
            "cache_dir": str(cache_dir),
            "logs_dir": str(logs_dir),
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config_payload), encoding="utf-8")

    args = Namespace(
        config=str(config_path),
        provider=None,
        price_min=None,
        price_max=None,
        history_min_days=None,
        outdir=str(outputs_dir),
        cache_dir=str(cache_dir),
        max_workers=None,
        mode="watchlist",
        watchlist=str(watchlist_path),
        themes=None,
        batch_size=None,
        batch_cursor_file=None,
        log_level="INFO",
        offline=True,
    )

    result = run_pipeline(args)

    assert result == 0
    assert (outputs_dir / "run_manifest.json").exists()
    assert list(outputs_dir.glob("features_*.csv"))
    assert list(outputs_dir.glob("scored_*.csv"))
