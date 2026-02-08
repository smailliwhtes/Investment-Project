from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from market_app.cli import _execute_run, _run_determinism_check, parse_args


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


def test_determinism_check_pass(tmp_path: Path) -> None:
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
            "determinism-check",
            "--config",
            str(config_path),
            "--runs-dir",
            str(tmp_path / "outputs" / "runs"),
            "--offline",
            "--as-of-date",
            "2025-01-31",
            "--run-id",
            "det_check",
        ]
    )

    exit_code = _run_determinism_check(args)
    assert exit_code == 0

    diff_dir = tmp_path / "outputs" / "runs" / "determinism_check" / "det_check"
    assert (diff_dir / "diff_summary.json").exists()


def test_determinism_check_detects_diff(tmp_path: Path) -> None:
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
            "determinism-check",
            "--config",
            str(config_path),
            "--runs-dir",
            str(tmp_path / "outputs" / "runs"),
            "--offline",
            "--as-of-date",
            "2025-01-31",
            "--run-id",
            "det_check_fail",
        ]
    )

    def _run_with_diff(run_args, *, run_id_override=None, as_of_date=None, now_utc=None):
        run_dir = _execute_run(
            run_args,
            run_id_override=run_id_override,
            as_of_date=as_of_date,
            now_utc=now_utc,
        )
        if run_id_override and run_id_override.endswith("_2"):
            eligible_path = run_dir / "eligible.csv"
            df = pd.read_csv(eligible_path)
            df["nondeterministic"] = run_id_override
            df.to_csv(eligible_path, index=False, lineterminator="\n")
        return run_dir

    exit_code = _run_determinism_check(args, run_fn=_run_with_diff)
    assert exit_code == 2

    diff_dir = tmp_path / "outputs" / "runs" / "determinism_check" / "det_check_fail"
    assert (diff_dir / "diff_summary.json").exists()
