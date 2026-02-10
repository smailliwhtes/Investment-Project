from __future__ import annotations

from pathlib import Path

from market_app.local_config import load_config
from market_app.validation_local import run_validation


def test_validation_report_on_fixtures(tmp_path: Path) -> None:
    config_path = tmp_path / "config_validation.yaml"
    config_payload = (
        "schema_version: v2\n"
        "offline: true\n"
        "run:\n"
        "  as_of_date: 2024-01-03\n"
        "paths:\n"
        f"  symbols_dir: {Path(__file__).resolve().parent / 'data' / 'symbols'}\n"
        f"  ohlcv_dir: {Path(__file__).resolve().parent / 'data' / 'ohlcv'}\n"
        f"  output_dir: {tmp_path}\n"
    )
    config_path.write_text(config_payload, encoding="utf-8")
    config_result = load_config(config_path)
    report = run_validation(config_result)
    assert "symbols" in report.payload
