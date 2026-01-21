from __future__ import annotations

import socket
from argparse import Namespace
from pathlib import Path

import yaml

from market_monitor.cli import run_pipeline


def test_offline_watchlist_blocks_socket(tmp_path: Path, monkeypatch) -> None:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "ohlcv"
    watchlist = Path(__file__).resolve().parent / "fixtures" / "watchlist.txt"
    outputs_dir = tmp_path / "outputs"
    cache_dir = tmp_path / "cache"
    logs_dir = tmp_path / "logs"

    config_payload = {
        "data": {
            "offline_mode": True,
            "provider": "nasdaq_daily",
            "paths": {"nasdaq_daily_dir": str(fixtures_dir)},
        },
        "paths": {
            "watchlist_file": str(watchlist),
            "outputs_dir": str(outputs_dir),
            "cache_dir": str(cache_dir),
            "logs_dir": str(logs_dir),
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config_payload), encoding="utf-8")

    def _blocked_socket(*args, **kwargs):
        raise AssertionError("Network socket usage is blocked in offline mode.")

    monkeypatch.setattr(socket, "socket", _blocked_socket)

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
        watchlist=str(watchlist),
        themes=None,
        batch_size=None,
        batch_cursor_file=None,
        log_level="INFO",
    )
    result = run_pipeline(args)
    assert result == 0
