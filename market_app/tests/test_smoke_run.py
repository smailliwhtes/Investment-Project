from pathlib import Path

import pandas as pd

from market_monitor.io import FEATURE_COLUMNS, SCORED_COLUMNS, ELIGIBLE_COLUMNS, write_csv
from market_monitor.report import write_report
from market_monitor.scoring import score_frame
from market_monitor.staging import stage_pipeline
from market_monitor.providers.base import HistoryProvider, ProviderCapabilities


class FixtureProvider(HistoryProvider):
    name = "fixture"
    capabilities = ProviderCapabilities(True, False, False, "offline")

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        return self.df.tail(days).copy()


def test_smoke_pipeline(tmp_path: Path):
    df = pd.read_csv("tests/fixtures/ohlcv.csv")
    provider = FixtureProvider(df)
    universe = pd.DataFrame({"symbol": ["TEST"], "name": ["Test Co"], "security_type": ["COMMON"], "currency": ["USD"]})
    config = {
        "staging": {"stage1_micro_days": 7, "stage2_short_days": 60, "stage3_deep_days": 120, "history_min_days": 60},
        "gates": {"price_max": 10.0, "min_adv20_dollar": 1000, "max_zero_volume_frac": 1.0},
        "themes": {"defense": {"symbols": [], "keywords": []}},
        "data": {"max_workers": 1},
    }
    run_meta = {"run_id": "test", "run_timestamp_utc": "2024-01-01T00:00:00Z", "config_hash": "hash", "provider_name": "fixture"}

    stage1_df, stage2_df, stage3_df, summary = stage_pipeline(
        universe,
        provider,
        tmp_path,
        0,
        config,
        run_meta,
        logger=type("obj", (), {"info": lambda *args, **kwargs: None})(),
    )
    scored = score_frame(stage3_df, {
        "trend": 0.25,
        "momentum": 0.25,
        "liquidity": 0.15,
        "vol_penalty": 0.15,
        "dd_penalty": 0.1,
        "tail_penalty": 0.05,
        "theme_bonus": 0.05,
    })

    outputs_dir = tmp_path / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    write_csv(scored, outputs_dir / "features_test.csv", FEATURE_COLUMNS)
    write_csv(scored, outputs_dir / "scored_test.csv", SCORED_COLUMNS)
    write_csv(scored[["symbol", "name", "eligible", "gate_fail_codes", "notes"]], outputs_dir / "eligible_test.csv", ELIGIBLE_COLUMNS)
    write_report(outputs_dir / "report_test.md", summary, scored)

    assert (outputs_dir / "features_test.csv").exists()
    assert (outputs_dir / "scored_test.csv").exists()
    assert (outputs_dir / "eligible_test.csv").exists()
    assert (outputs_dir / "report_test.md").exists()
