from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from market_monitor.gdelt import doctor as gdelt_doctor


def test_precomputed_daily_streams_large_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "precomputed_daily.csv"
    path.write_text(
        "Date,mentions,tone_mean\n"
        "2024-01-01,1,0.1\n"
        "2024-01-02,2,0.2\n",
        encoding="utf-8",
    )

    calls: list[int | None] = []
    original_read_csv = pd.read_csv

    def wrapped_read_csv(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs.get("chunksize"))
        return original_read_csv(*args, **kwargs)

    monkeypatch.setattr(pd, "read_csv", wrapped_read_csv)

    result = gdelt_doctor._normalize_precomputed_daily(
        file_paths=[path],
        gdelt_dir=tmp_path / "gdelt",
        date_col=None,
        write_format="csv",
        classification_summary={},
        streaming_threshold_bytes=1,
        streaming_chunk_rows=1,
    )

    assert any(chunksize is not None for chunksize in calls)
    assert result["manifest_path"].exists()
