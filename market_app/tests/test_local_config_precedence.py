from __future__ import annotations

import os
from pathlib import Path

from market_app.local_config import load_config


def test_local_config_precedence(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "config.yaml"
    config_payload = (
        "schema_version: v2\n"
        "paths:\n"
        f"  ohlcv_dir: {tmp_path / 'yaml_ohlcv'}\n"
    )
    config_path.write_text(config_payload, encoding="utf-8")

    env_dir = tmp_path / "env_ohlcv"
    cli_dir = tmp_path / "cli_ohlcv"
    monkeypatch.setenv("MARKET_APP_OHLCV_DIR", str(env_dir))

    result = load_config(config_path, cli_overrides={"ohlcv_dir": str(cli_dir)})
    assert Path(result.config["paths"]["ohlcv_dir"]).resolve() == cli_dir.resolve()
