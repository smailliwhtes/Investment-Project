from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
import yaml


def _write_watchlist_csv(path: Path) -> None:
    frame = pd.DataFrame(
        [
            {"symbol": "ALFA", "name": "Alpha Robotics", "exchange": "NASDAQ", "asset_type": "COMMON"},
            {"symbol": "BETA", "name": "Beta Materials", "exchange": "NASDAQ", "asset_type": "COMMON"},
        ]
    )
    frame.to_csv(path, index=False)


def _write_symbols_csv(path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "symbol": "ALFA",
                "name": "Alpha Robotics",
                "exchange": "NASDAQ",
                "asset_type": "COMMON",
                "is_etf": False,
                "is_test_issue": False,
            },
            {
                "symbol": "BETA",
                "name": "Beta Materials",
                "exchange": "NASDAQ",
                "asset_type": "COMMON",
                "is_etf": False,
                "is_test_issue": False,
            },
        ]
    )
    frame.to_csv(path, index=False)


def _write_ohlcv_csv(path: Path, *, start: str, periods: int, base_price: float, volume: int) -> None:
    dates = pd.bdate_range(start=start, periods=periods)
    close = pd.Series([base_price + i * 0.2 for i in range(len(dates))], dtype="float64")
    frame = pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": close - 0.3,
            "high": close + 0.4,
            "low": close - 0.5,
            "close": close,
            "volume": volume,
        }
    )
    frame.to_csv(path, index=False)


def _write_corpus_fixture(path: Path) -> None:
    frame = pd.DataFrame(
        [
            {"date": "2025-01-28", "theme": "defense", "sentiment": 0.25},
            {"date": "2025-01-29", "theme": "tech", "sentiment": 0.10},
            {"date": "2025-01-30", "theme": "metals", "sentiment": -0.05},
        ]
    )
    frame.to_csv(path, index=False)


def test_offline_e2e_market_and_corpus(tmp_path: Path) -> None:
    app_root = Path(__file__).resolve().parents[1] / "market_app"
    symbols_dir = tmp_path / "symbols"
    ohlcv_dir = tmp_path / "ohlcv"
    corpus_dir = tmp_path / "corpus"
    output_dir = tmp_path / "outputs"

    symbols_dir.mkdir(parents=True, exist_ok=True)
    ohlcv_dir.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)

    watchlist_path = tmp_path / "watchlist.csv"
    _write_watchlist_csv(watchlist_path)
    _write_symbols_csv(symbols_dir / "symbols.csv")
    _write_ohlcv_csv(ohlcv_dir / "ALFA.csv", start="2024-11-01", periods=75, base_price=10.0, volume=250_000)
    _write_ohlcv_csv(ohlcv_dir / "BETA.csv", start="2024-11-01", periods=75, base_price=22.0, volume=210_000)
    _write_corpus_fixture(corpus_dir / "gdelt_fixture.csv")

    config = {
        "schema_version": "v2",
        "offline": True,
        "online": False,
        "paths": {
            "symbols_dir": str(symbols_dir),
            "ohlcv_dir": str(ohlcv_dir),
            "output_dir": str(output_dir),
            "watchlists_file": str(watchlist_path),
            "corpus_dir": str(corpus_dir),
        },
        "run": {"top_n": 5},
        "gates": {
            "min_history_days": 30,
            "min_adv20_usd": 100_000,
            "price_floor": 1.0,
            "zero_volume_max_frac": 1.0,
            "max_lag_days": 10,
        },
        "corpus": {"enabled": True, "required": True},
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    run_id = "offline_e2e"
    cmd = [
        sys.executable,
        "-m",
        "market_app.cli",
        "run",
        "--config",
        str(config_path),
        "--offline",
        "--run-id",
        run_id,
        "--as-of-date",
        "2025-01-31",
    ]
    subprocess.check_call(cmd, cwd=app_root)

    run_dir = output_dir / run_id
    assert (run_dir / "eligible.csv").exists()
    assert (run_dir / "scored.csv").exists()
    assert (run_dir / "report.md").exists()

    scored = pd.read_csv(run_dir / "scored.csv")
    data_quality = pd.read_csv(run_dir / "data_quality.csv")
    assert "last_date" in scored.columns
    assert "lag_days" in scored.columns

    merged = scored[["symbol", "last_date", "lag_days"]].merge(
        data_quality[["symbol", "last_date", "lag_days"]],
        on="symbol",
        how="left",
        suffixes=("_scored", "_dq"),
    )
    assert not merged[["last_date_dq", "lag_days_dq"]].isna().any().any(), (
        "Every scored symbol must have deterministic staleness source rows in data_quality.csv"
    )
    assert (merged["last_date_scored"].astype(str) == merged["last_date_dq"].astype(str)).all()
    assert (
        pd.to_numeric(merged["lag_days_scored"], errors="raise")
        == pd.to_numeric(merged["lag_days_dq"], errors="raise")
    ).all()

    forecast_like = [
        col
        for col in scored.columns
        if "forecast" in col.lower() or "forward" in col.lower()
    ]
    assert forecast_like, "Expected at least one forecast-like column in scored.csv"

    assert (run_dir / "corpus_features.csv").exists()
