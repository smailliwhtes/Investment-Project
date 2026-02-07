from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml


def _write_temp_config(tmp_path: Path, fixture_config: Path, fixture_root: Path) -> Path:
    config = yaml.safe_load(fixture_config.read_text(encoding="utf-8"))
    config["paths"]["output_dir"] = str(tmp_path / "outputs" / "runs")
    config["paths"]["data_dir"] = str(fixture_root)
    config["paths"]["watchlist_file"] = str(fixture_root / "watchlist.txt")
    config["paths"]["nasdaq_daily_dir"] = str(fixture_root / "ohlcv")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def test_market_app_cli_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "market_app.cli", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_market_app_doctor_passes(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_config = repo_root / "tests" / "fixtures" / "blueprint_config.yaml"

    fixture_root = repo_root / "tests" / "fixtures"
    config_path = _write_temp_config(tmp_path, fixture_config, fixture_root)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_app.cli",
            "doctor",
            "--config",
            str(config_path),
            "--offline",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    assert "Summary: PASS" in result.stdout
