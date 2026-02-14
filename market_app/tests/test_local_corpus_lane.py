from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from market_app.local_config import load_config
from market_app.offline_pipeline import run_offline_pipeline


def test_local_corpus_lane_writes_features(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-01", "2025-01-02"],
            "theme": ["defense", "tech", "metals"],
            "sentiment": [0.2, 0.1, -0.1],
        }
    ).to_csv(corpus_dir / "gdelt_local.csv", index=False)

    config = yaml.safe_load((repo_root / "config" / "config.yaml").read_text(encoding="utf-8"))
    config["paths"]["symbols_dir"] = str(repo_root / "tests" / "data" / "mini_dataset" / "symbols")
    config["paths"]["ohlcv_dir"] = str(repo_root / "tests" / "data" / "mini_dataset" / "ohlcv")
    config["paths"]["output_dir"] = str(tmp_path / "outputs" / "runs")
    config["paths"]["logging_config"] = ""
    config["paths"]["corpus_dir"] = str(corpus_dir)
    cfg = tmp_path / "config.yaml"
    cfg.write_text(yaml.safe_dump(config), encoding="utf-8")

    cfg_result = load_config(cfg)
    logger = logging.getLogger("test_local_corpus_lane")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    run_dir = run_offline_pipeline(cfg_result, run_id="corpus_lane", logger=logger)
    assert (run_dir / "corpus_features.csv").exists()
    report = (run_dir / "report.md").read_text(encoding="utf-8")
    assert "Context Summary" in report
