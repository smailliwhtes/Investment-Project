from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from market_monitor.cli import main


def _write_fred_series(path: Path, rows: list[tuple[str, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["date", "value"]).to_csv(path, index=False)


def _write_policy_inputs(tmp_path: Path) -> dict[str, Path]:
    data_dir = tmp_path / "data"
    events_path = data_dir / "policy_events.jsonl"
    gdelt_path = data_dir / "policy_gdelt_daily_features.csv"
    holdings_path = data_dir / "etf_holdings.csv"
    fred_dir = data_dir / "fred_cache"
    sources_path = tmp_path / "policy_sources.yaml"
    scenarios_path = tmp_path / "policy_scenarios.yaml"

    events = [
        {
            "event_id": "evt_tariff_20240220",
            "event_type": "tariff",
            "source": "fixture",
            "agency": "ustr",
            "event_date": "2024-02-20",
            "title": "Tariff proposal",
            "summary": "Synthetic tariff proposal.",
            "sectors": ["industrials", "supply_chain"],
            "tickers": ["AAA", "BBB"],
            "countries": ["USA", "CHN"],
            "severity": 0.70,
        },
        {
            "event_id": "evt_tariff_20240617",
            "event_type": "tariff",
            "source": "fixture",
            "agency": "ustr",
            "event_date": "2024-06-17",
            "title": "Tariff implementation",
            "summary": "Synthetic tariff implementation.",
            "sectors": ["industrials", "metals"],
            "tickers": ["AAA"],
            "countries": ["USA", "CHN"],
            "severity": 0.78,
        },
        {
            "event_id": "evt_tariff_20241004",
            "event_type": "tariff",
            "source": "fixture",
            "agency": "ustr",
            "event_date": "2024-10-04",
            "title": "Tariff escalation",
            "summary": "Synthetic tariff escalation.",
            "sectors": ["industrials", "supply_chain"],
            "tickers": ["BBB"],
            "countries": ["USA", "CHN"],
            "severity": 0.82,
        },
        {
            "event_id": "evt_sanction_20240909",
            "event_type": "sanction",
            "source": "fixture",
            "agency": "treasury",
            "event_date": "2024-09-09",
            "title": "Sanctions package",
            "summary": "Synthetic sanctions package.",
            "sectors": ["energy"],
            "tickers": ["GLD"],
            "countries": ["USA", "RUS"],
            "severity": 0.55,
        },
    ]
    events_path.parent.mkdir(parents=True, exist_ok=True)
    with events_path.open("w", encoding="utf-8", newline="\n") as handle:
        for event in events:
            handle.write(json.dumps(event, sort_keys=True))
            handle.write("\n")

    pd.DataFrame(
        [
            {"Date": "2024-01-31", "conflict_event_count_total": 2, "energy_stress_score": 0.30},
            {"Date": "2024-02-20", "conflict_event_count_total": 3, "energy_stress_score": 0.35},
            {"Date": "2024-06-17", "conflict_event_count_total": 4, "energy_stress_score": 0.45},
            {"Date": "2024-10-04", "conflict_event_count_total": 6, "energy_stress_score": 0.72},
            {"Date": "2025-01-31", "conflict_event_count_total": 5, "energy_stress_score": 0.61},
        ]
    ).to_csv(gdelt_path, index=False)

    pd.DataFrame(
        [
            {"as_of_date": "2025-01-31", "etf_symbol": "XLI", "constituent_symbol": "AAA", "weight": 0.55, "sector": "industrials", "theme": "supply_chain"},
            {"as_of_date": "2025-01-31", "etf_symbol": "XLI", "constituent_symbol": "BBB", "weight": 0.45, "sector": "industrials", "theme": "metals"},
            {"as_of_date": "2025-01-31", "etf_symbol": "GLD", "constituent_symbol": "GLD", "weight": 1.00, "sector": "metals", "theme": "commodities"},
        ]
    ).to_csv(holdings_path, index=False)

    fred_rows = [
        ("2024-01-01", 4.75),
        ("2024-02-01", 4.75),
        ("2024-03-01", 4.75),
        ("2024-04-01", 4.75),
        ("2024-05-01", 4.75),
        ("2024-06-01", 4.75),
        ("2024-07-01", 4.75),
        ("2024-08-01", 4.50),
        ("2024-09-01", 4.25),
        ("2024-10-01", 4.25),
        ("2024-11-01", 4.25),
        ("2024-12-01", 4.00),
        ("2025-01-01", 4.00),
    ]
    _write_fred_series(fred_dir / "FEDFUNDS.csv", fred_rows)
    _write_fred_series(
        fred_dir / "UNRATE.csv",
        [(date, value) for date, value in zip([row[0] for row in fred_rows], [4.0, 4.0, 3.9, 3.9, 4.0, 4.1, 4.1, 4.2, 4.2, 4.2, 4.1, 4.1, 4.0], strict=True)],
    )
    _write_fred_series(
        fred_dir / "CPIAUCSL.csv",
        [(date, value) for date, value in zip([row[0] for row in fred_rows], [300.0, 300.6, 301.4, 302.1, 302.8, 303.7, 304.5, 305.4, 306.2, 307.0, 307.9, 308.8, 309.6], strict=True)],
    )
    _write_fred_series(
        fred_dir / "INDPRO.csv",
        [(date, value) for date, value in zip([row[0] for row in fred_rows], [100.0, 100.2, 100.5, 100.7, 101.0, 101.2, 101.5, 101.7, 102.0, 102.3, 102.5, 102.8, 103.0], strict=True)],
    )

    sources_path.write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "policy_events": {"type": "jsonl", "path": str(events_path)},
                    "fred_cache": {"type": "fred_cache", "path": str(fred_dir)},
                    "gdelt_daily_features": {"type": "csv", "path": str(gdelt_path)},
                    "etf_holdings": {"type": "csv", "path": str(holdings_path)},
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    scenarios_path.write_text(
        yaml.safe_dump(
            {
                "scenarios": {
                    "tariff-shock": {
                        "description": "Synthetic tariff scenario for deterministic CLI testing.",
                        "event_type": "tariff",
                        "severity": 0.8,
                        "sectors": ["industrials", "supply_chain"],
                        "linked_etfs": ["XLI"],
                        "tickers": ["AAA"],
                        "countries": ["USA", "CHN"],
                        "direction_bias": "negative",
                        "benchmark_symbol": "SPY",
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    return {
        "events": events_path,
        "fred_cache": fred_dir,
        "gdelt_daily": gdelt_path,
        "holdings": holdings_path,
        "sources": sources_path,
        "scenarios": scenarios_path,
    }


def _write_policy_config(tmp_path: Path) -> Path:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    inputs = _write_policy_inputs(tmp_path)
    (tmp_path / "cache").mkdir(parents=True, exist_ok=True)
    config = {
        "data": {
            "offline_mode": True,
            "provider": "nasdaq_daily",
            "paths": {
                "nasdaq_daily_dir": str(fixtures_dir / "ohlcv_daily"),
            },
        },
        "data_roots": {
            "ohlcv_dir": str(fixtures_dir / "ohlcv_daily"),
            "outputs_dir": str(tmp_path / "outputs"),
        },
        "paths": {
            "watchlist_file": str(fixtures_dir / "watchlists" / "watchlist_tiny.csv"),
            "outputs_dir": str(tmp_path / "outputs"),
            "cache_dir": str(tmp_path / "cache"),
            "exogenous_daily_dir": str(fixtures_dir / "exogenous" / "daily_features"),
        },
        "pipeline": {
            "asof_default": "2025-01-31",
            "benchmarks": ["SPY"],
        },
        "policy": {
            "paths": {
                "events_file": str(inputs["events"]),
                "fred_cache_dir": str(inputs["fred_cache"]),
                "gdelt_daily_file": str(inputs["gdelt_daily"]),
                "etf_holdings_file": str(inputs["holdings"]),
                "sources_config": str(inputs["sources"]),
                "scenarios_config": str(inputs["scenarios"]),
            },
            "simulation": {
                "horizons": [1, 5, 20],
                "estimation_lookback_days": 20,
                "top_n_analogs": 3,
                "path_count": 64,
                "seed": 7,
                "benchmark_symbol": "SPY",
                "average_dollar_volume_floor": 100_000.0,
            },
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def test_policy_simulate_cli_writes_expected_artifacts(tmp_path: Path, capsys) -> None:
    config_path = _write_policy_config(tmp_path)
    out_dir = tmp_path / "outputs" / "policy_run"

    exit_code = main(
        [
            "policy",
            "simulate",
            "--config",
            str(config_path),
            "--scenario",
            "tariff-shock",
            "--outdir",
            str(out_dir),
            "--offline",
            "--progress-jsonl",
        ]
    )

    assert exit_code == 0
    for name in (
        "policy_event_study.csv",
        "policy_analogs.csv",
        "policy_scenario_rankings.csv",
        "policy_report.md",
        "policy_manifest.json",
        "policy_summary.json",
    ):
        assert (out_dir / name).exists(), name

    summary = json.loads((out_dir / "policy_summary.json").read_text(encoding="utf-8"))
    assert summary["scenario"] == "tariff-shock"
    assert summary["status"] == "complete"
    assert summary["summary"] == "Policy scenario simulation complete."
    assert summary["output_dir"] == str(out_dir)
    assert len(summary["fields"]) >= 4
    assert summary["top_rankings"]

    manifest = json.loads((out_dir / "policy_manifest.json").read_text(encoding="utf-8"))
    assert manifest["counts"]["historical_events"] >= 3
    assert manifest["counts"]["symbols_ranked"] >= 1

    rankings = pd.read_csv(out_dir / "policy_scenario_rankings.csv")
    assert {"symbol", "rank", "scenario_impact_score"}.issubset(rankings.columns)
    assert not rankings.empty

    report_text = (out_dir / "policy_report.md").read_text(encoding="utf-8")
    assert "Policy Simulator Report" in report_text
    assert "Historical Analogs" in report_text

    stdout_lines = capsys.readouterr().out.splitlines()
    progress_lines = [json.loads(line) for line in stdout_lines if line.startswith('{"ts"')]
    assert any(line["type"] == "artifact_emitted" for line in progress_lines)
