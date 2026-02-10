from __future__ import annotations

from pathlib import Path

from market_app.local_config import load_config
from market_app.ml_local import run_training


def test_ml_training_on_fixtures(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_payload = (
        "schema_version: v2\n"
        "offline: true\n"
        "paths:\n"
        f"  symbols_dir: {Path(__file__).resolve().parent / 'data' / 'symbols'}\n"
        f"  ohlcv_dir: {Path(__file__).resolve().parent / 'data' / 'ohlcv'}\n"
        f"  geopolitics_dir: {Path(__file__).resolve().parent / 'data' / 'geopolitics'}\n"
        f"  output_dir: {tmp_path}\n"
        f"  training_output_dir: {tmp_path / 'training'}\n"
        f"  model_dir: {tmp_path / 'models'}\n"
    )
    config_path.write_text(config_payload, encoding="utf-8")
    config_result = load_config(config_path)
    report = run_training(config_result, asof_end="2024-01-03", run_id="fixture_train")
    assert (report.model_dir / "model_manifest.json").exists()
