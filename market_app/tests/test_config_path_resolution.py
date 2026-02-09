from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from market_monitor.config_schema import load_config
from market_monitor.data_paths import resolve_data_paths
from market_monitor.paths import resolve_path


def test_load_config_accepts_str_absolute(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    config = load_config(str(config_path)).config

    assert config["paths"]["watchlist_file"]


def test_load_config_accepts_path_absolute(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")

    config = load_config(config_path).config

    assert config["paths"]["watchlist_file"]


def test_load_config_env_override(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("paths:\n  watchlist_file: env_watchlist.csv\n", encoding="utf-8")

    other_path = tmp_path / "other.yaml"
    other_path.write_text("paths:\n  watchlist_file: other_watchlist.csv\n", encoding="utf-8")

    monkeypatch.setenv("MARKET_APP_CONFIG", str(config_path))

    config = load_config(other_path).config

    assert config["paths"]["watchlist_file"] == "env_watchlist.csv"


def test_load_config_relative_resolves_repo_root(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    nested_dir = repo_root / "nested" / "child"
    nested_dir.mkdir(parents=True)

    config_path = repo_root / "config.yaml"
    config_path.write_text("paths:\n  watchlist_file: repo_watchlist.csv\n", encoding="utf-8")

    monkeypatch.chdir(nested_dir)

    config = load_config("config.yaml").config

    assert config["paths"]["watchlist_file"] == "repo_watchlist.csv"


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


def test_config_paths_ignore_env_override_when_config_sets_ohlcv(monkeypatch, tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "tests" / "fixtures" / "minimal_config.yaml"
    config = load_config(config_path).config
    base_dir = config_path.parent

    monkeypatch.setenv("MARKET_APP_NASDAQ_DAILY_DIR", str(tmp_path / "external_ohlcv"))

    resolved = resolve_data_paths(config, base_dir).nasdaq_daily_dir
    assert resolved == (base_dir / "ohlcv").resolve()


def test_env_override_applies_when_config_omits_ohlcv(monkeypatch, tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    env_ohlcv = tmp_path / "env_ohlcv"
    monkeypatch.setenv("MARKET_APP_NASDAQ_DAILY_DIR", str(env_ohlcv))

    config = load_config(config_path).config
    base_dir = config_path.parent
    resolved = resolve_data_paths(config, base_dir).nasdaq_daily_dir

    assert resolved == env_ohlcv.resolve()


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
