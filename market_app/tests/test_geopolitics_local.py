from __future__ import annotations

from pathlib import Path

from market_app.geopolitics_local import build_geopolitics_features, lag_geopolitics_features


def test_geopolitics_build_and_lag(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parent / "data" / "geopolitics" / "gdelt_events_sample.csv"
    output_dir = tmp_path / "outputs"
    output_dir.mkdir()

    result = build_geopolitics_features(geopolitics_path=fixture, output_dir=output_dir)
    assert not result.frame.empty
    assert "events_count" in result.frame.columns
    lagged = lag_geopolitics_features(result.frame, lag_days=1)
    assert lagged["day"].iloc[0] > result.frame["day"].iloc[0]
