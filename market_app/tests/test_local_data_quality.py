from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from market_app.local_config import load_config
from market_app.offline_pipeline import run_offline_pipeline


def _write_config(tmp_path: Path, symbols_dir: Path, ohlcv_dir: Path) -> Path:
    base = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "config.yaml").read_text(encoding="utf-8"))
    base["paths"]["symbols_dir"] = str(symbols_dir)
    base["paths"]["ohlcv_dir"] = str(ohlcv_dir)
    base["paths"]["output_dir"] = str(tmp_path / "outputs" / "runs")
    base["paths"]["logging_config"] = ""
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml.safe_dump(base), encoding="utf-8")
    return cfg


def test_stale_data_uses_spy_as_of_and_lag_math(tmp_path: Path) -> None:
    symbols_dir = tmp_path / "symbols"
    ohlcv_dir = tmp_path / "ohlcv"
    symbols_dir.mkdir()
    ohlcv_dir.mkdir()
    (symbols_dir / "universe.csv").write_text("symbol,name\nSPY,Spy\nAAA,AAA\nOLD,OLD\n", encoding="utf-8")
    (ohlcv_dir / "SPY.csv").write_text(
        "date,open,high,low,close,volume\n2026-02-10,1,1,1,1,100\n",
        encoding="utf-8",
    )
    (ohlcv_dir / "AAA.csv").write_text(
        "date,open,high,low,close,volume\n2026-02-10,1,1,1,1,100\n",
        encoding="utf-8",
    )
    (ohlcv_dir / "OLD.csv").write_text(
        "date,open,high,low,close,volume\n2026-01-27,1,1,1,1,100\n",
        encoding="utf-8",
    )

    cfg_path = _write_config(tmp_path, symbols_dir, ohlcv_dir)
    cfg_result = load_config(cfg_path)
    cfg_result.config["gates"]["max_lag_days"] = 5

    logger = logging.getLogger("test_local_data_quality")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    run_dir = run_offline_pipeline(cfg_result, run_id="dq_test", logger=logger)
    dq = pd.read_csv(run_dir / "data_quality.csv")
    scored = pd.read_csv(run_dir / "scored.csv")

    assert set(dq["symbol"]) == {"SPY", "AAA", "OLD"}

    aaa = dq.loc[dq["symbol"] == "AAA"].iloc[0]
    old = dq.loc[dq["symbol"] == "OLD"].iloc[0]
    assert aaa["as_of_date"] == "2026-02-10"
    assert int(aaa["lag_days"]) == 0
    assert bool(aaa["stale_data"]) is False
    assert int(old["lag_days"]) == 14
    assert bool(old["stale_data"]) is True

    assert "last_date" in scored.columns
    assert "lag_days" in scored.columns
    old_scored = scored.loc[scored["symbol"] == "OLD"].iloc[0]
    assert old_scored["last_date"] == "2026-01-27"
    assert int(old_scored["lag_days"]) == 14


def test_scored_deterministic_across_runs(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    cfg_path = _write_config(tmp_path, repo_root / "tests" / "data" / "mini_dataset" / "symbols", repo_root / "tests" / "data" / "mini_dataset" / "ohlcv")
    cfg_result = load_config(cfg_path)

    logger = logging.getLogger("test_local_data_quality_det")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    run_a = run_offline_pipeline(cfg_result, run_id="det_a", logger=logger)
    run_b = run_offline_pipeline(cfg_result, run_id="det_b", logger=logger)
    a = (run_a / "scored.csv").read_bytes()
    b = (run_b / "scored.csv").read_bytes()
    assert a == b


def test_pipeline_honors_explicit_config_paths_over_env(tmp_path: Path, monkeypatch) -> None:
    symbols_dir = tmp_path / "symbols"
    ohlcv_dir = tmp_path / "ohlcv"
    bad_symbols_dir = tmp_path / "bad_symbols"
    bad_ohlcv_dir = tmp_path / "bad_ohlcv"
    symbols_dir.mkdir()
    ohlcv_dir.mkdir()
    bad_symbols_dir.mkdir()
    bad_ohlcv_dir.mkdir()

    (symbols_dir / "universe.csv").write_text("symbol,name\nSPY,Spy\nAAA,AAA\nOLD,OLD\n", encoding="utf-8")
    (bad_symbols_dir / "universe.csv").write_text("symbol,name\nZZZ,ZZZ\n", encoding="utf-8")

    (ohlcv_dir / "SPY.csv").write_text("date,open,high,low,close,volume\n2026-02-10,1,1,1,1,100\n", encoding="utf-8")
    (ohlcv_dir / "AAA.csv").write_text("date,open,high,low,close,volume\n2026-02-10,1,1,1,1,100\n", encoding="utf-8")
    (ohlcv_dir / "OLD.csv").write_text("date,open,high,low,close,volume\n2026-01-27,1,1,1,1,100\n", encoding="utf-8")
    (bad_ohlcv_dir / "ZZZ.csv").write_text("date,open,high,low,close,volume\n2026-02-10,1,1,1,1,100\n", encoding="utf-8")

    monkeypatch.setenv("MARKET_APP_SYMBOLS_DIR", str(bad_symbols_dir))
    monkeypatch.setenv("MARKET_APP_OHLCV_DIR", str(bad_ohlcv_dir))

    cfg_path = _write_config(tmp_path, symbols_dir, ohlcv_dir)
    cfg_result = load_config(cfg_path)

    logger = logging.getLogger("test_local_data_quality_paths")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    run_dir = run_offline_pipeline(cfg_result, run_id="dq_paths", logger=logger)
    universe = pd.read_csv(run_dir / "universe.csv")
    dq = pd.read_csv(run_dir / "data_quality.csv")

    assert set(universe["symbol"]) == {"SPY", "AAA", "OLD"}
    assert set(dq["symbol"]) == {"SPY", "AAA", "OLD"}
