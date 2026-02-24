from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd


def _run_checker(run_dir: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "../scripts/check_staleness_contract.py",
            "--run-dir",
            str(run_dir),
        ],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        capture_output=True,
        check=False,
    )


def test_contract_accepts_equivalent_lag_days_and_last_date_formats(tmp_path: Path) -> None:
    scored = pd.DataFrame(
        [
            {"symbol": " alfa ", "last_date": "2025-01-31", "lag_days": 399},
            {"symbol": "beta", "last_date": "2025-01-30", "lag_days": 2},
        ]
    )
    dq = pd.DataFrame(
        [
            {"symbol": "ALFA", "last_date": "2025-01-31T00:00:00", "lag_days": 399.0},
            {"symbol": "BETA", "last_date": "2025-01-30 13:10:00", "lag_days": 2.0},
        ]
    )
    scored.to_csv(tmp_path / "scored.csv", index=False)
    dq.to_csv(tmp_path / "data_quality.csv", index=False)

    result = _run_checker(tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_contract_fails_with_missing_symbols_message(tmp_path: Path) -> None:
    pd.DataFrame(
        [{"symbol": "ALFA", "last_date": "2025-01-31", "lag_days": 1}]
    ).to_csv(tmp_path / "scored.csv", index=False)
    pd.DataFrame(
        [{"symbol": "BETA", "last_date": "2025-01-31", "lag_days": 1}]
    ).to_csv(tmp_path / "data_quality.csv", index=False)

    result = _run_checker(tmp_path)
    assert result.returncode != 0
    assert "symbol universe mismatch" in result.stdout
    assert "missing in data_quality.csv" in result.stdout


def test_contract_fails_on_duplicate_symbol_with_clear_message(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {"symbol": "ALFA", "last_date": "2025-01-31", "lag_days": 1},
            {"symbol": "ALFA", "last_date": "2025-01-31", "lag_days": 1},
        ]
    ).to_csv(tmp_path / "scored.csv", index=False)
    pd.DataFrame(
        [{"symbol": "ALFA", "last_date": "2025-01-31", "lag_days": 1}]
    ).to_csv(tmp_path / "data_quality.csv", index=False)

    result = _run_checker(tmp_path)
    assert result.returncode != 0
    assert "duplicate symbols" in result.stdout


def test_contract_fails_on_real_mismatch_with_counts(tmp_path: Path) -> None:
    pd.DataFrame(
        [{"symbol": "ALFA", "last_date": "2025-01-31", "lag_days": 2}]
    ).to_csv(tmp_path / "scored.csv", index=False)
    pd.DataFrame(
        [{"symbol": "ALFA", "last_date": "2025-01-30", "lag_days": 3}]
    ).to_csv(tmp_path / "data_quality.csv", index=False)

    result = _run_checker(tmp_path)
    assert result.returncode != 0
    assert "mismatched_last_date=1" in result.stdout
    assert "mismatched_lag_days=1" in result.stdout
