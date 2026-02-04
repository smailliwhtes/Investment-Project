from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from market_monitor.gdelt.doctor import (
    DAILY_FEATURES_PRECOMPUTED,
    NEEDS_NORMALIZATION,
    audit_corpus,
    normalize_corpus,
)


def _write_precomputed_daily(path: Path) -> None:
    df = pd.DataFrame(
        {
            "dt": ["2024-01-01", "2024-01-01", "2024-01-02"],
            "mentions": [10, 20, 30],
            "tone_mean": [0.1, 0.2, 0.3],
        }
    )
    df.to_csv(path, index=False)


def test_doctor_classify_precomputed_daily(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    source = raw_dir / "precomputed.csv"
    _write_precomputed_daily(source)

    report = audit_corpus(raw_dir=raw_dir, file_glob="*.csv", format_hint="auto")
    by_path = {Path(item.path).name: item for item in report.files}
    audit = by_path["precomputed.csv"]
    assert audit.file_type == DAILY_FEATURES_PRECOMPUTED
    assert audit.readiness_verdict == NEEDS_NORMALIZATION
    assert audit.candidate_date_columns
    assert audit.inferred_frequency == "daily"

    gdelt_dir = tmp_path / "gdelt"
    normalize_corpus(
        raw_dir=raw_dir,
        gdelt_dir=gdelt_dir,
        file_glob="*.csv",
        format_hint="auto",
        write_format="csv",
    )

    day_path = gdelt_dir / "daily_features" / "day=2024-01-01" / "part-00000.csv"
    assert day_path.exists()
    day_frame = pd.read_csv(day_path)
    assert day_frame.loc[0, "mentions"] == 15

    manifest_path = gdelt_dir / "daily_features" / "features_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["content_hash"]
    assert manifest["coverage"]["min_day"] == "2024-01-01"
    assert "mentions" in manifest["schema"]["columns"]
