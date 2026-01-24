from __future__ import annotations

from pathlib import Path

from market_monitor.config_schema import load_config
from market_monitor.data_paths import resolve_data_paths
from market_monitor.paths import resolve_path


def test_config_paths_resolve_relative_to_config(monkeypatch, tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "tests" / "fixtures" / "minimal_config.yaml"
    config = load_config(config_path).config
    base_dir = config_path.parent

    monkeypatch.chdir(tmp_path)

    watchlist_path = resolve_path(base_dir, config["paths"]["watchlist_file"])
    nasdaq_dir = resolve_data_paths(config, base_dir).nasdaq_daily_dir

    assert watchlist_path == (base_dir / "watchlist.txt").resolve()
    assert nasdaq_dir == (base_dir / "ohlcv").resolve()
