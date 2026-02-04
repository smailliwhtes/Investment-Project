from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from market_monitor.gdelt.doctor import ANNUAL_AGGREGATES, UNUSABLE, audit_corpus, normalize_corpus


def test_doctor_classify_annual_aggregates(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    source = raw_dir / "annual.csv"
    pd.DataFrame({"Year": [2020, 2021], "conflict_count": [5, 7]}).to_csv(
        source, index=False
    )

    report = audit_corpus(raw_dir=raw_dir, file_glob="*.csv", format_hint="auto")
    audit = report.files[0]
    assert audit.file_type == ANNUAL_AGGREGATES
    assert audit.readiness_verdict == UNUSABLE
    assert any("Annual aggregates" in issue for issue in audit.issues)

    with pytest.raises(ValueError, match="annual"):
        normalize_corpus(
            raw_dir=raw_dir,
            gdelt_dir=tmp_path / "gdelt",
            file_glob="*.csv",
            format_hint="auto",
            write_format="csv",
        )
