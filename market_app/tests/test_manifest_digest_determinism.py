from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import yaml

from market_app.cli import _execute_run, parse_args
from market_monitor.determinism import canonical_bytes


def _write_temp_config(
    tmp_path: Path, fixture_config: Path, watchlist_path: Path, data_dir: Path
) -> Path:
    config = yaml.safe_load(fixture_config.read_text(encoding="utf-8"))
    config["paths"]["output_dir"] = str(tmp_path / "outputs" / "runs")
    config["paths"]["watchlist_file"] = str(watchlist_path)
    config["paths"]["data_dir"] = str(data_dir)
    config["paths"]["nasdaq_daily_dir"] = str(data_dir / "ohlcv")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config_path


def _copy_watchlists(tmp_path: Path, source: Path) -> None:
    target = tmp_path / "watchlists.yaml"
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")


def test_manifest_digest_determinism_across_run_ids(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_config = repo_root / "tests" / "fixtures" / "blueprint_config.yaml"
    watchlists = repo_root / "tests" / "fixtures" / "watchlists.yaml"
    watchlist_path = repo_root / "tests" / "fixtures" / "watchlist.txt"

    config_path = _write_temp_config(
        tmp_path, fixture_config, watchlist_path, repo_root / "tests" / "fixtures"
    )
    _copy_watchlists(tmp_path, watchlists)

    args = parse_args(
        [
            "run",
            "--config",
            str(config_path),
            "--runs-dir",
            str(tmp_path / "outputs" / "runs"),
            "--offline",
            "--as-of-date",
            "2025-01-31",
            "--now-utc",
            "2025-01-31T00:00:00+00:00",
        ]
    )

    run_dir_a = _execute_run(args, run_id_override="determinism_a")
    run_dir_b = _execute_run(args, run_id_override="determinism_b")

    manifest_a = canonical_bytes(run_dir_a / "manifest.json")
    manifest_b = canonical_bytes(run_dir_b / "manifest.json")
    digest_a = canonical_bytes(run_dir_a / "digest.json")
    digest_b = canonical_bytes(run_dir_b / "digest.json")

    assert manifest_a == manifest_b
    assert digest_a == digest_b

    manifest_payload = json.loads((run_dir_a / "manifest.json").read_text(encoding="utf-8"))
    digest_payload = json.loads((run_dir_a / "digest.json").read_text(encoding="utf-8"))
    assert "manifest.json" not in manifest_payload.get("outputs", {})
    assert "digest.json" not in manifest_payload.get("outputs", {})
    assert "manifest.json" not in digest_payload.get("outputs", {})
    assert "digest.json" not in digest_payload.get("outputs", {})


def test_determinism_check_cli_passes_with_empty_allowlist(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_config = repo_root / "tests" / "fixtures" / "blueprint_config.yaml"
    watchlists = repo_root / "tests" / "fixtures" / "watchlists.yaml"
    watchlist_path = repo_root / "tests" / "fixtures" / "watchlist.txt"

    config_path = _write_temp_config(
        tmp_path, fixture_config, watchlist_path, repo_root / "tests" / "fixtures"
    )
    _copy_watchlists(tmp_path, watchlists)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_app.cli",
            "determinism-check",
            "--config",
            str(config_path),
            "--runs-dir",
            str(tmp_path / "outputs" / "runs"),
            "--offline",
            "--as-of-date",
            "2025-01-31",
            "--now-utc",
            "2025-01-31T00:00:00+00:00",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
