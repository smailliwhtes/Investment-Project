from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import yaml


def _write_corpus_fixture(path: Path) -> None:
    rows = [
        {
            "SQLDATE": "20240102",
            "GlobalEventID": "1",
            "EventCode": "190",
            "EventRootCode": "19",
            "QuadClass": "4",
            "GoldsteinScale": "-5.0",
            "AvgTone": "-2.0",
            "NumMentions": "10",
            "NumSources": "5",
            "NumArticles": "4",
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "RUS",
            "ActionGeo_CountryCode": "USA",
        },
        {
            "SQLDATE": "20240102",
            "GlobalEventID": "2",
            "EventCode": "190",
            "EventRootCode": "19",
            "QuadClass": "4",
            "GoldsteinScale": "-4.0",
            "AvgTone": "-1.5",
            "NumMentions": "8",
            "NumSources": "4",
            "NumArticles": "3",
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "RUS",
            "ActionGeo_CountryCode": "USA",
        },
        {
            "SQLDATE": "20240102",
            "GlobalEventID": "3",
            "EventCode": "190",
            "EventRootCode": "19",
            "QuadClass": "4",
            "GoldsteinScale": "-3.0",
            "AvgTone": "-1.0",
            "NumMentions": "6",
            "NumSources": "3",
            "NumArticles": "2",
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "RUS",
            "ActionGeo_CountryCode": "USA",
        },
        {
            "SQLDATE": "20240103",
            "GlobalEventID": "4",
            "EventCode": "010",
            "EventRootCode": "01",
            "QuadClass": "1",
            "GoldsteinScale": "1.0",
            "AvgTone": "0.5",
            "NumMentions": "2",
            "NumSources": "1",
            "NumArticles": "1",
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "CAN",
            "ActionGeo_CountryCode": "USA",
        },
        {
            "SQLDATE": "20240104",
            "GlobalEventID": "5",
            "EventCode": "020",
            "EventRootCode": "02",
            "QuadClass": "2",
            "GoldsteinScale": "0.5",
            "AvgTone": "0.1",
            "NumMentions": "2",
            "NumSources": "1",
            "NumArticles": "1",
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "MEX",
            "ActionGeo_CountryCode": "USA",
        },
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def test_corpus_build_linked_generates_cause_effect_artifacts(tmp_path: Path) -> None:
    fixtures = Path(__file__).resolve().parent / "fixtures"
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    _write_corpus_fixture(corpus_dir / "events.csv")

    config = {
        "data": {"offline_mode": True, "provider": "nasdaq_daily"},
        "paths": {
            "watchlist_file": str(fixtures / "watchlists" / "watchlist_tiny.csv"),
            "ohlcv_daily_dir": str(fixtures / "ohlcv_daily"),
            "outputs_dir": str(tmp_path / "outputs"),
            "cache_dir": str(tmp_path / "cache"),
        },
        "pipeline": {"benchmarks": ["SPY"]},
        "corpus": {
            "gdelt_conflict_dir": str(corpus_dir),
            "gdelt_events_raw_dir": str(tmp_path / "raw_events"),
            "analogs": {"spike_stddev": 0.0, "forward_days": [1, 5]},
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    out_dir = tmp_path / "outputs" / "linked"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "corpus",
            "build-linked",
            "--config",
            str(config_path),
            "--outdir",
            str(out_dir),
            "--lags",
            "1,3",
            "--rolling-window",
            "2",
            "--rolling-mean",
            "--rolling-sum",
            "--output-format",
            "csv",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout

    payload = json.loads(result.stdout)
    assert Path(payload["manifest_path"]).exists()
    assert Path(payload["summary_path"]).exists()
    assert Path(payload["joined_manifest_path"]).exists()

    assert (out_dir / "market_daily.csv").exists()
    assert (out_dir / "gdelt_daily_features.csv").exists()
    assert (out_dir / "cause_effect_manifest.json").exists()
    assert (out_dir / "cause_effect_summary.json").exists()
    assert (out_dir / "linked_market_gdelt" / "manifest.json").exists()

    summary = json.loads((out_dir / "cause_effect_summary.json").read_text(encoding="utf-8"))
    counts = summary["counts"]
    assert counts["market_rows"] > 0
    assert counts["gdelt_rows"] > 0
    assert counts["joined_rows"] > 0
    assert counts["event_impact_rows"] > 0


def test_corpus_build_linked_requires_feature_selection(tmp_path: Path) -> None:
    fixtures = Path(__file__).resolve().parent / "fixtures"
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    _write_corpus_fixture(corpus_dir / "events.csv")

    config = {
        "data": {"offline_mode": True, "provider": "nasdaq_daily"},
        "paths": {
            "watchlist_file": str(fixtures / "watchlists" / "watchlist_tiny.csv"),
            "ohlcv_daily_dir": str(fixtures / "ohlcv_daily"),
            "outputs_dir": str(tmp_path / "outputs"),
            "cache_dir": str(tmp_path / "cache"),
        },
        "pipeline": {"benchmarks": ["SPY"]},
        "corpus": {
            "gdelt_conflict_dir": str(corpus_dir),
            "gdelt_events_raw_dir": str(tmp_path / "raw_events"),
            "analogs": {"spike_stddev": 0.0, "forward_days": [1, 5]},
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "corpus",
            "build-linked",
            "--config",
            str(config_path),
            "--lags",
            "",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
    assert "No GDELT feature selection" in result.stdout

