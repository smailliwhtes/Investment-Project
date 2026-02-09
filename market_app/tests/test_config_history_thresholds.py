from __future__ import annotations

from pathlib import Path

import pandas as pd

from market_monitor.config_schema import load_config
from market_monitor.providers.base import HistoryProvider, ProviderCapabilities
from market_monitor.staging import stage_pipeline


class FixtureProvider(HistoryProvider):
    name = "fixture"
    capabilities = ProviderCapabilities(True, False, False, "offline")

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        return self.df.tail(days).copy() if days > 0 else self.df.copy()


def test_custom_history_thresholds_affect_stage_pipeline(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "data:",
                "  offline_mode: true",
                "  provider: nasdaq_daily",
                "data_roots:",
                "  ohlcv_dir: ohlcv",
                "paths:",
                "  watchlist_file: watchlist.txt",
                "  cache_dir: data/cache",
                "  logs_dir: outputs/logs",
                "  outputs_dir: outputs",
                "staging:",
                "  stage1_micro_days: 3",
                "  stage2_short_days: 5",
                "  stage3_deep_days: 10",
                "  history_min_days: 6",
            ]
        ),
        encoding="utf-8",
    )

    config = load_config(config_path).config
    df = pd.DataFrame(
        {
            "Date": pd.date_range("2025-01-01", periods=15, freq="B"),
            "Open": 1.0,
            "High": 1.0,
            "Low": 1.0,
            "Close": 1.0,
            "Volume": 1000,
        }
    )
    provider = FixtureProvider(df)
    universe = pd.DataFrame(
        {"symbol": ["TEST"], "name": ["Test"], "security_type": ["COMMON"], "currency": ["USD"]}
    )
    run_meta = {
        "run_id": "test",
        "run_timestamp_utc": "2025-02-01T00:00:00Z",
        "config_hash": "hash",
        "provider_name": "fixture",
    }

    _, _, stage3_df, _ = stage_pipeline(
        universe,
        provider,
        tmp_path,
        0,
        config,
        run_meta,
        logger=type("obj", (), {"info": lambda *args, **kwargs: None})(),
    )

    assert int(stage3_df.loc[0, "history_days"]) == 10
    assert stage3_df.loc[0, "data_status"] == "OK"
