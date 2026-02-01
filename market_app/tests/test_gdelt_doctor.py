from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from market_monitor.gdelt.doctor import (
    NEEDS_NORMALIZATION,
    READY_STABLE,
    audit_corpus,
    normalize_corpus,
    verify_cache,
)
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
        }
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


def test_doctor_audit_detects_mixed_dialects(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    headered = raw_dir / "events.csv"
    tabbed = raw_dir / "events_tab.csv"
    _write_headered_events(headered)
    _write_tab_events(tabbed)

    report = audit_corpus(raw_dir=raw_dir, file_glob="*.csv", format_hint="auto")
    assert report.overall_verdict in {READY_STABLE, NEEDS_NORMALIZATION}
    by_path = {Path(item.path).name: item for item in report.files}
    assert by_path["events.csv"].readiness_verdict == READY_STABLE
    assert by_path["events_tab.csv"].readiness_verdict == NEEDS_NORMALIZATION

    payload = report.to_dict()
    assert "files" in payload
    assert "overall_verdict" in payload
    assert payload["files"][0]["required_fields"]["availability"]


def test_doctor_normalize_creates_cache(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_headered_events(raw_dir / "events.csv")
    _write_tab_events(raw_dir / "events_tab.csv")

    gdelt_dir = tmp_path / "gdelt"
    normalize_corpus(
        raw_dir=raw_dir,
        gdelt_dir=gdelt_dir,
        file_glob="*.csv",
        format_hint="events",
        write_format="csv",
    )

    assert (gdelt_dir / "events" / "day=2020-01-01" / "part-00000.csv").exists()
    assert (gdelt_dir / "events" / "day=2020-01-03" / "part-00000.csv").exists()
    manifest_path = gdelt_dir / "events" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["content_hash"]

    ok, issues = verify_cache(gdelt_dir=gdelt_dir)
    assert ok
    assert not issues
