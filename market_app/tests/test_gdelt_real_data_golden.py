from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from market_monitor.features.join_exogenous import build_joined_features
from market_monitor.gdelt import doctor as gdelt_doctor
from market_monitor.gdelt.doctor import audit_corpus, normalize_corpus
from market_monitor import time_utils


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "gdelt_real"
RAW_DIR = FIXTURE_ROOT / "raw"
EXPECTED_DIR = FIXTURE_ROOT / "expected"
FIXED_UTC = "2024-02-01T00:00:00+00:00"


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _relativize_path(value: str) -> str:
    path = Path(value)
    if path.is_absolute():
        try:
            return path.relative_to(REPO_ROOT).as_posix()
        except ValueError:
            return path.name
    return value


def _canonicalize_manifest(value: object) -> object:
    if isinstance(value, dict):
        cleaned: dict[str, object] = {}
        for key, item in value.items():
            if key in {"path", "market_path", "gdelt_path"} and isinstance(item, str):
                cleaned[key] = _relativize_path(item)
            else:
                cleaned[key] = _canonicalize_manifest(item)
        return cleaned
    if isinstance(value, list):
        return [_canonicalize_manifest(item) for item in value]
    if isinstance(value, str):
        return _relativize_path(value)
    return value


def _assert_frames_equal(left_path: Path, right_path: Path) -> None:
    left = pd.read_csv(left_path)
    right = pd.read_csv(right_path)
    pd.testing.assert_frame_equal(left, right, check_dtype=False)


def test_gdelt_doctor_real_data_golden(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time_utils, "utc_now_iso", lambda: FIXED_UTC)

    report = audit_corpus(raw_dir=RAW_DIR, file_glob="*", format_hint="auto")
    file_types = {Path(item.path).name: item.file_type for item in report.files}
    assert file_types["gdelt_events_sample.tsv"] == gdelt_doctor.EVENTS_RAW
    assert file_types["precomputed_daily_mixed.csv"] == gdelt_doctor.DAILY_FEATURES_PRECOMPUTED

    gdelt_dir = tmp_path / "gdelt"
    normalize_corpus(
        raw_dir=RAW_DIR,
        gdelt_dir=gdelt_dir,
        file_glob="*",
        format_hint="auto",
        write_format="csv",
    )

    expected_daily = EXPECTED_DIR / "daily_features"
    for day in ["2024-01-01", "2024-01-02"]:
        expected_path = expected_daily / f"day={day}" / "part-00000.csv"
        actual_path = gdelt_dir / "daily_features" / f"day={day}" / "part-00000.csv"
        _assert_frames_equal(actual_path, expected_path)

    expected_manifest = _canonicalize_manifest(_load_json(expected_daily / "features_manifest.json"))
    actual_manifest = _canonicalize_manifest(
        _load_json(gdelt_dir / "daily_features" / "features_manifest.json")
    )
    expected_manifest.pop("created_utc", None)
    actual_manifest.pop("created_utc", None)
    assert actual_manifest == expected_manifest


def test_join_exogenous_real_data_golden(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(time_utils, "utc_now_iso", lambda: FIXED_UTC)

    output_dir = tmp_path / "joined"
    result = build_joined_features(
        market_path=FIXTURE_ROOT / "market_daily.csv",
        gdelt_path=EXPECTED_DIR / "daily_features",
        out_dir=output_dir,
        lags=[1],
        output_format="csv",
    )

    expected_joined = EXPECTED_DIR / "joined"
    for day in ["2024-01-02", "2024-01-03"]:
        expected_path = expected_joined / f"day={day}" / "part-00000.csv"
        actual_path = output_dir / f"day={day}" / "part-00000.csv"
        _assert_frames_equal(actual_path, expected_path)

    expected_manifest = _canonicalize_manifest(_load_json(expected_joined / "manifest.json"))
    actual_manifest = _canonicalize_manifest(_load_json(result.manifest_path))
    expected_manifest.pop("created_utc", None)
    actual_manifest.pop("created_utc", None)
    assert actual_manifest == expected_manifest
