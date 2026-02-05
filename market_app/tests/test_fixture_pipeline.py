from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


def test_run_and_evaluate_with_fixtures(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "tests" / "fixtures" / "minimal_config.yaml"
    outdir = tmp_path / "market_audit"

    run_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "run-legacy",
            "--config",
            str(config_path),
            "--mode",
            "watchlist",
            "--outdir",
            str(outdir),
            "--offline",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    assert run_result.returncode == 0, run_result.stderr or run_result.stdout

    eligible_files = list(outdir.glob("eligible_*.csv"))
    scored_files = list(outdir.glob("scored_*.csv"))
    assert eligible_files, "eligible outputs missing"
    assert scored_files, "scored outputs missing"

    eligible_df = pd.read_csv(eligible_files[0])
    scored_df = pd.read_csv(scored_files[0])
    assert not eligible_df.empty
    assert not scored_df.empty

    eval_result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "evaluate",
            "--config",
            str(config_path),
            "--outdir",
            str(outdir),
            "--offline",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    combined = (eval_result.stdout or "") + (eval_result.stderr or "")
    assert eval_result.returncode == 0, combined
    assert "panel is empty" not in combined.lower()
