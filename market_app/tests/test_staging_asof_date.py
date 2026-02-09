from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from market_monitor.providers.base import HistoryProvider, ProviderCapabilities
from market_monitor.staging import stage_pipeline


class FixtureProvider(HistoryProvider):
    name = "fixture"
    capabilities = ProviderCapabilities(True, False, False, "offline")

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        return self.df.tail(days).copy() if days > 0 else self.df.copy()


def test_stage_pipeline_uses_symbol_max_date_before_now_utc(tmp_path: Path) -> None:
    dates = pd.date_range("2005-02-25", "2026-01-27", freq="B")
    close = np.linspace(100.0, 200.0, len(dates))
    df = pd.DataFrame(
        {
            "Date": dates,
            "Open": close,
            "High": close,
            "Low": close,
            "Close": close,
            "Volume": 1000,
        }
    )
    provider = FixtureProvider(df)
    universe = pd.DataFrame(
        {"symbol": ["SPY"], "name": ["SPY"], "security_type": ["ETF"], "currency": ["USD"]}
    )
    config = {
        "staging": {
            "stage1_micro_days": 7,
            "stage2_short_days": 60,
            "stage3_deep_days": 600,
            "history_min_days": 252,
        },
        "gates": {"price_min": None, "price_max": None, "risk_flags": {}},
        "themes": {},
        "data": {"max_workers": 1},
    }
    run_meta = {
        "run_id": "test",
        "run_timestamp_utc": "2026-02-08T21:00:00Z",
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
        now_utc="2026-02-08T21:00:00Z",
    )

    assert stage3_df.loc[0, "as_of_date"] == "2026-01-27"
    assert stage3_df.loc[0, "history_days"] == 600
    assert stage3_df.loc[0, "as_of_date_deep"] == "2026-01-27"
