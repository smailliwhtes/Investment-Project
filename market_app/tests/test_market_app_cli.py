from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import yaml


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


def test_market_app_cli_contract(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_config = repo_root / "tests" / "fixtures" / "blueprint_config.yaml"
    watchlists = repo_root / "tests" / "fixtures" / "watchlists.yaml"
    watchlist_path = repo_root / "tests" / "fixtures" / "watchlist.txt"

    config_path = _write_temp_config(
        tmp_path, fixture_config, watchlist_path, repo_root / "tests" / "fixtures"
    )
    _copy_watchlists(tmp_path, watchlists)

    run_id = "test_run"
    cmd = [
        sys.executable,
        "-m",
        "market_app.cli",
        "--config",
        str(config_path),
        "--offline",
        "--run_id",
        run_id,
        "--top_n",
        "5",
        "--conservative",
    ]
    subprocess.check_call(cmd)

    run_dir = tmp_path / "outputs" / "runs" / run_id
    expected = [
        "universe.csv",
        "classified.csv",
        "features.csv",
        "eligible.csv",
        "scored.csv",
        "regime.json",
        "report.md",
        "manifest.json",
    ]
    for name in expected:
        path = run_dir / name
        assert path.exists()
        assert path.stat().st_size > 0

    regime = json.loads((run_dir / "regime.json").read_text(encoding="utf-8"))
    assert "regime_label" in regime
    assert "indicators" in regime

    classified = pd.read_csv(run_dir / "classified.csv")
    evidence = json.loads(classified.loc[0, "evidence"])
    assert isinstance(evidence, dict)

    scored = pd.read_csv(run_dir / "scored.csv")
    assert "forward_outcome_summary" in scored.columns


def test_market_app_variant_changes_score(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    fixture_config = repo_root / "tests" / "fixtures" / "blueprint_config.yaml"
    watchlists = repo_root / "tests" / "fixtures" / "watchlists.yaml"
    watchlist_path = repo_root / "tests" / "fixtures" / "watchlist.txt"

    config_path = _write_temp_config(
        tmp_path, fixture_config, watchlist_path, repo_root / "tests" / "fixtures"
    )
    _copy_watchlists(tmp_path, watchlists)

    cmd_cons = [
        sys.executable,
        "-m",
        "market_app.cli",
        "--config",
        str(config_path),
        "--offline",
        "--run_id",
        "cons_run",
        "--top_n",
        "5",
        "--conservative",
    ]
    subprocess.check_call(cmd_cons)

    cmd_opp = [
        sys.executable,
        "-m",
        "market_app.cli",
        "--config",
        str(config_path),
        "--offline",
        "--run_id",
        "opp_run",
        "--top_n",
        "5",
        "--opportunistic",
    ]
    subprocess.check_call(cmd_opp)

    cons = pd.read_csv(tmp_path / "outputs" / "runs" / "cons_run" / "scored.csv")
    opp = pd.read_csv(tmp_path / "outputs" / "runs" / "opp_run" / "scored.csv")

    assert not cons["total_score"].equals(opp["total_score"])
