from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml


def test_cli_help() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "market_monitor", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0


def test_cli_preflight_minimal_config(tmp_path: Path) -> None:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "minimal_config.yaml"
    fixture_dir = fixture_path.parent
    payload = yaml.safe_load(fixture_path.read_text(encoding="utf-8"))

    payload["paths"]["outputs_dir"] = str(tmp_path / "outputs")
    payload["paths"]["cache_dir"] = str(tmp_path / "cache")
    payload["paths"]["logs_dir"] = str(tmp_path / "logs")
    payload["paths"]["watchlist_file"] = str(fixture_dir / "watchlist.txt")
    payload["data"]["paths"]["nasdaq_daily_dir"] = str(fixture_dir / "ohlcv")

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")

    outdir = tmp_path / "run"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "preflight",
            "--config",
            str(config_path),
            "--outdir",
            str(outdir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
