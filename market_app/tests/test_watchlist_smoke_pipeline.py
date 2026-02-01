from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from market_monitor.cli import run_pipeline


EXPECTED_ELIGIBLE_COLUMNS = [
    "symbol",
    "eligible",
    "gate_fail_reasons",
    "theme_bucket",
    "asset_type",
]

EXPECTED_SCORED_COLUMNS = [
    "symbol",
    "score_1to10",
    "risk_flags",
    "explanation",
    "theme_bucket",
    "asset_type",
    "ml_signal",
    "ml_model_id",
    "ml_featureset_id",
]


def _assert_pipe_delimited(series: pd.Series) -> None:
    cleaned = series.fillna("").astype(str)
    assert not cleaned.str.contains(";", regex=False).any()


def _assert_bool_like(series: pd.Series) -> None:
    normalized = series.fillna("").astype(str).str.lower()
    allowed = {"true", "false", "0", "1"}
    assert normalized.isin(allowed).all()


def _assert_score_range(series: pd.Series) -> None:
    numeric = pd.to_numeric(series, errors="coerce")
    assert not numeric.isna().any()
    assert ((numeric % 1 == 0) & numeric.between(1, 10)).all()


def _build_synthetic_ohlcv(symbol: str, rows: int = 300) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=rows, freq="B")
    base = 100 + (sum(ord(letter) for letter in symbol) % 25)
    trend = np.linspace(0, 12, rows)
    close = base + trend
    open_ = close * 0.995
    high = close * 1.01
    low = close * 0.99
    volume = 1_000_000 + np.arange(rows) * 10
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        }
    )


def test_watchlist_smoke_pipeline(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    watchlist_path = repo_root / "watchlists" / "watchlist_smoke.csv"
    data_dir = tmp_path / "ohlcv"
    data_dir.mkdir(parents=True, exist_ok=True)
    for symbol in ["SPY", "TLT", "GLD"]:
        fixture_df = _build_synthetic_ohlcv(symbol)
        fixture_df.to_csv(data_dir / f"{symbol}.csv", index=False)

    outputs_dir = tmp_path / "outputs"
    cache_dir = tmp_path / "cache"
    logs_dir = tmp_path / "logs"

    config_payload = {
        "data": {
            "offline_mode": True,
            "provider": "nasdaq_daily",
            "paths": {"nasdaq_daily_dir": str(data_dir)},
        },
        "paths": {
            "watchlist_file": str(watchlist_path),
            "outputs_dir": str(outputs_dir),
            "cache_dir": str(cache_dir),
            "logs_dir": str(logs_dir),
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config_payload), encoding="utf-8")

    args = Namespace(
        config=str(config_path),
        provider=None,
        price_min=None,
        price_max=None,
        history_min_days=None,
        outdir=str(outputs_dir),
        cache_dir=str(cache_dir),
        max_workers=None,
        mode="watchlist",
        watchlist=str(watchlist_path),
        themes=None,
        batch_size=None,
        batch_cursor_file=None,
        log_level="INFO",
        offline=True,
    )

    result = run_pipeline(args)

    assert result == 0
    assert (outputs_dir / "run_manifest.json").exists()
    assert list(outputs_dir.glob("features_*.csv"))
    eligible_files = list(outputs_dir.glob("eligible_*.csv"))
    scored_files = list(outputs_dir.glob("scored_*.csv"))
    assert eligible_files
    assert scored_files

    eligible_df = pd.read_csv(eligible_files[0])
    scored_df = pd.read_csv(scored_files[0])

    assert list(eligible_df.columns) == EXPECTED_ELIGIBLE_COLUMNS
    assert list(scored_df.columns) == EXPECTED_SCORED_COLUMNS
    _assert_bool_like(eligible_df["eligible"])
    _assert_pipe_delimited(eligible_df["gate_fail_reasons"])
    _assert_score_range(scored_df["score_1to10"])
    _assert_pipe_delimited(scored_df["risk_flags"])
