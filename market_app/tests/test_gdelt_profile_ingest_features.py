from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from market_monitor.gdelt.features_daily import build_daily_features
from market_monitor.gdelt.ingest import ingest_gdelt
from market_monitor.gdelt.profile import profile_gdelt
from market_monitor.gdelt.utils import EVENTS_HEADER_COLUMNS


def _write_headered_events(path: Path) -> None:
    rows = [
        {
            "SQLDATE": "20200101",
            "GlobalEventID": "1",
            "EventCode": "010",
            "EventBaseCode": "01",
            "EventRootCode": "01",
            "QuadClass": "1",
            "GoldsteinScale": "2.5",
            "AvgTone": "1.0",
            "NumMentions": "3",
            "NumSources": "2",
            "NumArticles": "1",
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "RUS",
            "ActionGeo_CountryCode": "USA",
            "SOURCEURL": "https://example.com/a",
        },
        {
            "SQLDATE": "20200102",
            "GlobalEventID": "2",
            "EventCode": "020",
            "EventBaseCode": "02",
            "EventRootCode": "02",
            "QuadClass": "4",
            "GoldsteinScale": "-1.5",
            "AvgTone": "-0.5",
            "NumMentions": "5",
            "NumSources": "3",
            "NumArticles": "2",
            "Actor1CountryCode": "USA",
            "Actor2CountryCode": "CHN",
            "ActionGeo_CountryCode": "CHN",
            "SOURCEURL": "https://example.com/b",
        },
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_tab_events(path: Path) -> None:
    row = [""] * len(EVENTS_HEADER_COLUMNS)
    row[EVENTS_HEADER_COLUMNS.index("GlobalEventID")] = "3"
    row[EVENTS_HEADER_COLUMNS.index("SQLDATE")] = "20200103"
    row[EVENTS_HEADER_COLUMNS.index("EventCode")] = "030"
    row[EVENTS_HEADER_COLUMNS.index("EventBaseCode")] = "03"
    row[EVENTS_HEADER_COLUMNS.index("EventRootCode")] = "03"
    row[EVENTS_HEADER_COLUMNS.index("QuadClass")] = "2"
    row[EVENTS_HEADER_COLUMNS.index("GoldsteinScale")] = "0.5"
    row[EVENTS_HEADER_COLUMNS.index("AvgTone")] = "0.2"
    row[EVENTS_HEADER_COLUMNS.index("NumMentions")] = "2"
    row[EVENTS_HEADER_COLUMNS.index("NumSources")] = "1"
    row[EVENTS_HEADER_COLUMNS.index("NumArticles")] = "1"
    row[EVENTS_HEADER_COLUMNS.index("Actor1CountryCode")] = "USA"
    row[EVENTS_HEADER_COLUMNS.index("Actor2CountryCode")] = "MEX"
    row[EVENTS_HEADER_COLUMNS.index("ActionGeo_CountryCode")] = "MEX"
    row[EVENTS_HEADER_COLUMNS.index("SOURCEURL")] = "https://example.com/c"
    path.write_text("\t".join(row), encoding="utf-8")


def test_gdelt_profile_detects_date_range(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_headered_events(raw_dir / "events.csv")

    summary = profile_gdelt(raw_dir=raw_dir, file_glob="*.csv", format_hint="events")
    assert summary is not None
    assert summary.min_day == "2020-01-01"
    assert summary.max_day == "2020-01-02"
    assert summary.required_field_present["day"]
    assert summary.required_field_missing["day"] == 0.0


def test_gdelt_ingest_partitions_and_manifest(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_headered_events(raw_dir / "events.csv")
    _write_tab_events(raw_dir / "events_tab.csv")

    out_dir = tmp_path / "gdelt"
    result = ingest_gdelt(
        raw_dir=raw_dir,
        out_dir=out_dir,
        file_glob="*.csv",
        format_hint="events",
        write_format="csv",
    )
    assert result is not None
    assert (out_dir / "events" / "day=2020-01-01" / "part-00000.csv").exists()
    assert (out_dir / "events" / "day=2020-01-03" / "part-00000.csv").exists()
    manifest_path = out_dir / "events" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["coverage"]["min_day"] == "2020-01-01"
    assert manifest["coverage"]["max_day"] == "2020-01-03"
    assert "event_root_code" in manifest["columns"]


def test_gdelt_features_daily_basic(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_headered_events(raw_dir / "events.csv")

    out_dir = tmp_path / "gdelt"
    ingest_gdelt(
        raw_dir=raw_dir,
        out_dir=out_dir,
        file_glob="*.csv",
        format_hint="events",
        write_format="csv",
    )

    output_path = out_dir / "features_daily.csv"
    result = build_daily_features(gdelt_dir=out_dir, out_path=output_path, by_country=False)
    assert result.rows == 2
    df = pd.read_csv(output_path)
    assert "day" in df.columns
    assert df.loc[df["day"] == "2020-01-01", "total_event_count"].iloc[0] == 1
    assert "goldstein_mean" in df.columns
    assert "tone_mean" in df.columns
