from __future__ import annotations

import os
import subprocess
import sys
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


def test_data_roots_env_override(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    ohlcv_dir = tmp_path / "ohlcv"
    outputs_dir = tmp_path / "outputs"
    monkeypatch.setenv("MARKET_APP_OHLCV_DIR", str(ohlcv_dir))
    monkeypatch.setenv("MARKET_APP_OUTPUTS_DIR", str(outputs_dir))

    config = load_config(config_path).config

    assert config["data_roots"]["ohlcv_dir"] == str(ohlcv_dir)
    assert config["paths"]["outputs_dir"] == str(outputs_dir)


def test_check_watchlist_requires_ohlcv_dir(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    env = dict(**os.environ)
    env.pop("MARKET_APP_OHLCV_DIR", None)
    env.pop("MARKET_APP_NASDAQ_DAILY_DIR", None)
    env.pop("NASDAQ_DAILY_DIR", None)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor.tools.check_watchlist_ohlcv",
            "--config",
            str(config_path),
        ],
        capture_output=True,
        text=True,
        env=env,
    )

    output = (result.stdout + result.stderr).lower()
    assert result.returncode != 0
    assert "data_roots.ohlcv_dir" in output
    assert "market_app_ohlcv_dir" in output
