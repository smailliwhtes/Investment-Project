from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml


def test_local_cli_smoke(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config" / "config.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["paths"]["output_dir"] = str(tmp_path / "outputs" / "runs")
    config["paths"]["symbols_dir"] = str(repo_root / "tests" / "data" / "symbols")
    config["paths"]["ohlcv_dir"] = str(repo_root / "tests" / "data" / "ohlcv")
    temp_config = tmp_path / "config.yaml"
    temp_config.write_text(yaml.safe_dump(config), encoding="utf-8")

    run_id = "smoke_local"
    cmd = [
        sys.executable,
        "-m",
        "market_app.cli",
        "--config",
        str(temp_config),
        "--offline",
        "--run-id",
        run_id,
        "--top-n",
        "3",
    ]
    subprocess.check_call(cmd)

    run_dir = tmp_path / "outputs" / "runs" / run_id
    expected = [
        "universe.csv",
        "classified.csv",
        "features.csv",
        "eligible.csv",
        "scored.csv",
        "report.md",
        "manifest.json",
        "run.log",
    ]
    for name in expected:
        path = run_dir / name
        assert path.exists()
        assert path.stat().st_size > 0
