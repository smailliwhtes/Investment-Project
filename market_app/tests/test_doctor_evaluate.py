from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


def test_doctor_runs_minimal_config() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "tests" / "fixtures" / "minimal_config.yaml"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "doctor",
            "--config",
            str(config_path),
            "--offline",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    assert result.returncode == 0, result.stderr or result.stdout

    base_dir = config_path.parent
    outputs_dir = base_dir / "outputs"
    cache_dir = base_dir / "data" / "cache"
    if outputs_dir.exists():
        shutil.rmtree(outputs_dir)
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    data_dir = base_dir / "data"
    if data_dir.exists() and not any(data_dir.iterdir()):
        data_dir.rmdir()


def test_evaluate_outdir_independent_of_inputs(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "tests" / "fixtures" / "minimal_config.yaml"
    outdir = tmp_path / "eval_outputs"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "evaluate",
            "--config",
            str(config_path),
            "--outdir",
            str(outdir),
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    combined = (result.stdout or "") + (result.stderr or "")
    assert result.returncode in {0, 2}, combined
    assert "No symbols available" not in combined
