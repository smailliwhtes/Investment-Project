from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from market_app.features_local import compute_features
from market_app.local_config import load_config
from market_app.offline_pipeline import run_offline_pipeline


def _config(tmp_path: Path, symbols_dir: Path, ohlcv_dir: Path, *, as_of: str = "2026-02-10") -> Path:
    base = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "config.yaml").read_text(encoding="utf-8"))
    base["paths"]["symbols_dir"] = str(symbols_dir)
    base["paths"]["ohlcv_dir"] = str(ohlcv_dir)
    base["paths"]["output_dir"] = str(tmp_path / "outputs" / "runs")
    base["paths"]["logging_config"] = ""
    base["as_of_date"] = as_of
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml.safe_dump(base), encoding="utf-8")
    return cfg


def test_last_date_and_lag_days_merged_into_scored(tmp_path: Path) -> None:
    symbols_dir = tmp_path / "symbols"
    ohlcv_dir = tmp_path / "ohlcv"
    symbols_dir.mkdir()
    ohlcv_dir.mkdir()
    (symbols_dir / "universe.csv").write_text("symbol,name\nSPY,Spy\nAAA,AAA\n", encoding="utf-8")
    (ohlcv_dir / "SPY.csv").write_text("date,open,high,low,close,volume\n2026-02-10,10,11,9,10,1000\n", encoding="utf-8")
    (ohlcv_dir / "AAA.csv").write_text("date,open,high,low,close,volume\n2026-02-07,10,11,9,10,1000\n", encoding="utf-8")

    cfg_result = load_config(_config(tmp_path, symbols_dir, ohlcv_dir))
    logger = logging.getLogger("contract")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    out = run_offline_pipeline(cfg_result, run_id="contract", logger=logger)

    dq = pd.read_csv(out / "data_quality.csv")
    scored = pd.read_csv(out / "scored.csv")
    merged = scored.merge(dq[["symbol", "last_date", "lag_days"]], on="symbol", suffixes=("_scored", "_dq"))
    assert (merged["last_date_scored"] == merged["last_date_dq"]).all()
    assert (merged["lag_days_scored"].astype("Int64") == merged["lag_days_dq"].astype("Int64")).all()
    assert "lag_bin" in scored.columns


def test_feature_correctness_constant_series() -> None:
    dates = pd.bdate_range("2024-01-02", periods=260)
    frame = pd.DataFrame({"date": dates, "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1000})
    feat = compute_features("CONST", frame, {})
    assert feat.features["volatility_20d"] == 0.0
    assert feat.features["max_drawdown_6m"] == 0.0


def test_smoke_run_writes_required_outputs(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[1]
    cfg_result = load_config(
        _config(
            tmp_path,
            repo / "tests" / "data" / "mini_dataset" / "symbols",
            repo / "tests" / "data" / "mini_dataset" / "ohlcv",
            as_of="2025-01-31",
        )
    )
    logger = logging.getLogger("smoke_contract")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    out = run_offline_pipeline(cfg_result, run_id="smoke_contract", logger=logger)
    required = [
        "universe.csv",
        "data_quality.csv",
        "features.csv",
        "eligible.csv",
        "ineligible.csv",
        "scored.csv",
        "report.md",
        "manifest.json",
    ]
    for name in required:
        assert (out / name).exists(), name
