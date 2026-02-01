from __future__ import annotations

from pathlib import Path
import json

import pandas as pd

from market_monitor.ml import dataset as ml_dataset
from market_monitor.ml import predict as ml_predict
from market_monitor.ml import train_xgb as ml_train
from market_monitor.pipeline import _attach_ml_predictions


def _write_joined_dataset(tmp_path: Path, *, include_raw_exogenous: bool) -> Path:
    days = pd.date_range("2024-01-01", periods=10, freq="D")
    symbols = ["AAA", "BBB"]
    rows = []
    for day_idx, day in enumerate(days):
        for symbol in symbols:
            base = 100 + day_idx
            rows.append(
                {
                    "day": day,
                    "symbol": symbol,
                    "close": base,
                    "ret_1d": 0.01 * day_idx,
                    "gdelt_mentions_lag_1": float(day_idx),
                    "gdelt_mentions": float(day_idx) if include_raw_exogenous else None,
                }
            )
    frame = pd.DataFrame(rows)
    if not include_raw_exogenous:
        frame = frame.drop(columns=["gdelt_mentions"])

    joined_dir = tmp_path / "joined"
    joined_dir.mkdir(parents=True, exist_ok=True)

    for day in frame["day"].dt.strftime("%Y-%m-%d").unique():
        day_dir = joined_dir / f"day={day}"
        day_dir.mkdir(parents=True, exist_ok=True)
        day_frame = frame[frame["day"].dt.strftime("%Y-%m-%d") == day].copy()
        day_frame["day"] = day
        day_frame.to_parquet(day_dir / "part-00000.parquet", index=False)

    market_path = tmp_path / "market" / "market.parquet"
    market_path.parent.mkdir(parents=True, exist_ok=True)
    market_frame = frame[["day", "symbol", "close", "ret_1d"]].copy()
    market_frame.to_parquet(market_path, index=False)

    gdelt_path = tmp_path / "gdelt" / "gdelt.csv"
    gdelt_path.parent.mkdir(parents=True, exist_ok=True)
    gdelt_frame = frame[["day", "gdelt_mentions_lag_1"]].copy()
    gdelt_frame["day"] = gdelt_frame["day"].dt.strftime("%Y-%m-%d")
    gdelt_frame.to_csv(gdelt_path, index=False)

    manifest = {
        "schema_version": 1,
        "inputs": {
            "market_path": str(market_path),
            "gdelt_path": str(gdelt_path),
        },
        "columns": list(frame.columns),
        "content_hash": "test_hash",
    }
    (joined_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return joined_dir


def test_ml_dataset_leakage_guard(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=True)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)
    assert "gdelt_mentions" in dataset.excluded_exogenous
    assert "gdelt_mentions" not in dataset.features


def test_ml_train_predict_and_merge(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)

    artifacts, pipeline = ml_train.train_model(
        dataset,
        folds=2,
        gap=0,
        model_type="sklearn_gb",
        random_seed=7,
        model_params={},
    )

    output_dir = tmp_path / "outputs" / "run_123"
    output_dir.mkdir(parents=True, exist_ok=True)
    ml_train._write_artifacts(output_dir=output_dir, artifacts=artifacts, pipeline=pipeline)

    ml_predict.run_prediction(
        joined_path=joined_dir,
        output_dir=output_dir,
        allow_exogenous=[],
    )

    ml_dir = output_dir / "ml"
    assert (ml_dir / "model.json").exists()
    assert (ml_dir / "metrics.json").exists()
    assert (ml_dir / "feature_importance.csv").exists()
    assert (ml_dir / "train_manifest.json").exists()
    assert (ml_dir / "report.md").exists()
    assert (ml_dir / "predictions_by_day.parquet").exists()
    assert (ml_dir / "predictions_latest.csv").exists()

    latest = pd.read_csv(ml_dir / "predictions_latest.csv")
    assert set(latest.columns) == {"symbol", "day", "yhat"}

    report_text = (ml_dir / "report.md").read_text(encoding="utf-8")
    assert artifacts.model_id in report_text
    assert artifacts.featureset_id in report_text
    assert "fold 1" in report_text

    scored = pd.DataFrame({"symbol": ["AAA", "BBB"]})
    merged = _attach_ml_predictions(scored, output_dir)
    assert "ml_signal" in merged.columns
    assert merged["ml_signal"].notna().all()
