from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path


ARTIFACTS = [
    "universe.csv",
    "features.csv",
    "eligible.csv",
    "scored.csv",
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def test_mini_dataset_offline_run_is_deterministic() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = repo_root / "tests" / "data" / "mini_dataset" / "config.yaml"
    run_root = repo_root / "tests" / "data" / "mini_dataset" / "outputs" / "runs"
    run1 = run_root / "it_det_1"
    run2 = run_root / "it_det_2"

    cmd = [sys.executable, "-m", "market_app.cli", "--config", str(config), "--offline"]
    subprocess.check_call(cmd + ["--run-id", run1.name], cwd=repo_root)
    subprocess.check_call(cmd + ["--run-id", run2.name], cwd=repo_root)

    hashes_1 = {_name: _sha256(run1 / _name) for _name in ARTIFACTS}
    hashes_2 = {_name: _sha256(run2 / _name) for _name in ARTIFACTS}
    assert hashes_1 == hashes_2
