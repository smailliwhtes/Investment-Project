from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import yaml


def _write_watchlist_run_config(tmp_path: Path) -> tuple[Path, Path]:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    watchlist_path = fixtures_dir / "watchlists" / "watchlist_tiny.csv"
    config_path = tmp_path / "config.yaml"
    payload = {
        "paths": {
            "watchlist_file": str(watchlist_path),
            "outputs_dir": str(tmp_path / "runs"),
            "ohlcv_daily_dir": str(fixtures_dir / "ohlcv_daily"),
            "exogenous_daily_dir": str(fixtures_dir / "exogenous" / "daily_features"),
        },
        "pipeline": {
            "auto_normalize_ohlcv": False,
            "include_raw_exogenous_same_day": False,
            "asof_default": "2025-01-31",
            "benchmarks": ["SPY"],
        },
        "scoring": {
            "minimum_history_days": 200,
            "price_floor": 1.0,
            "average_dollar_volume_floor": 1000000.0,
            "max_vol_20d_cap": None,
        },
    }
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return config_path, watchlist_path


def test_validate_config_json_shape(tmp_path: Path) -> None:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "minimal_config.yaml"
    valid = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "validate-config",
            "--config",
            str(fixture_path),
            "--format",
            "json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert valid.returncode == 0, valid.stderr or valid.stdout
    payload = json.loads(valid.stdout)
    assert payload["valid"] is True
    assert payload["errors"] == []

    invalid = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "validate-config",
            "--config",
            str(tmp_path / "missing.yaml"),
            "--format",
            "json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert invalid.returncode == 2
    payload = json.loads(invalid.stdout)
    assert payload["valid"] is False
    assert payload["errors"]


def test_contract_run_out_dir_emits_required_artifacts(tmp_path: Path) -> None:
    config_path, watchlist_path = _write_watchlist_run_config(tmp_path)
    out_dir = tmp_path / "runs" / "contract_run"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "run",
            "--config",
            str(config_path),
            "--watchlist",
            str(watchlist_path),
            "--out-dir",
            str(out_dir),
            "--offline",
            "--progress-jsonl",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    for name in (
        "scored.csv",
        "eligible.csv",
        "report.md",
        "run_manifest.json",
        "config_snapshot.yaml",
        "ui_engine.log",
    ):
        assert (out_dir / name).exists(), name
    assert (out_dir / "logs" / "engine.log").exists()

    scored = pd.read_csv(out_dir / "scored.csv")
    assert "last_date" in scored.columns
    assert "lag_days" in scored.columns
    progress_lines = [line for line in result.stdout.splitlines() if line.strip().startswith("{")]
    assert progress_lines
    assert any(json.loads(line).get("type") == "artifact_emitted" for line in progress_lines)


def test_diff_runs_json(tmp_path: Path) -> None:
    run_a = tmp_path / "run_a"
    run_b = tmp_path / "run_b"
    run_a.mkdir()
    run_b.mkdir()
    pd.DataFrame(
        {
            "symbol": ["AAA", "BBB"],
            "score": [5.0, 3.0],
            "rank": [1, 2],
            "flags_count": [0, 1],
        }
    ).to_csv(run_a / "scored.csv", index=False)
    pd.DataFrame(
        {
            "symbol": ["AAA", "CCC"],
            "score": [4.5, 6.0],
            "rank": [2, 1],
            "flags_count": [2, 0],
        }
    ).to_csv(run_b / "scored.csv", index=False)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "diff-runs",
            "--run-a",
            str(run_a),
            "--run-b",
            str(run_b),
            "--format",
            "json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["summary"]["n_new"] == 1
    assert payload["summary"]["n_removed"] == 1
