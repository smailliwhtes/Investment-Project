from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from market_monitor.ml import benchmark as ml_benchmark
from market_monitor.ml import dataset as ml_dataset
from market_monitor.ml import predict as ml_predict
from market_monitor.ml import split as ml_split
from market_monitor.ml import train_xgb as ml_train
from market_monitor.pipeline import _attach_ml_predictions


def _write_joined_dataset(
    tmp_path: Path,
    *,
    include_raw_exogenous: bool,
    periods: int = 10,
) -> Path:
    days = pd.date_range("2024-01-01", periods=periods, freq="D")
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


def _write_run_manifest(output_dir: Path, run_id: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_id": run_id,
        "started_at": "2025-01-31T00:00:00Z",
        "finished_at": "2025-01-31T00:05:00Z",
        "duration_s": 300.0,
        "artifacts": [],
    }
    (output_dir / "run_manifest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _assert_model_artifact_bundle(model_dir: Path) -> None:
    expected = [
        "metrics.json",
        "feature_importance.csv",
        "train_manifest.json",
        "model.joblib",
        "model.json",
        "report.md",
        "predictions_by_day.parquet",
        "predictions_latest.csv",
        "predict_manifest.json",
    ]
    for name in expected:
        path = model_dir / name
        assert path.exists(), path
        assert path.stat().st_size > 0


def test_ml_dataset_leakage_guard(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=True)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)
    assert "gdelt_mentions" in dataset.excluded_exogenous
    assert "gdelt_mentions" not in dataset.features


def test_ml_walk_forward_purges_overlapping_labels(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)
    splits = ml_split.build_walk_forward_splits(dataset.frame[dataset.day_column].unique(), folds=2, gap=0)

    train_df, val_df = ml_split.split_frame_for_walk_forward(
        dataset.frame,
        splits[0],
        day_column=dataset.day_column,
        label_end_column=dataset.label_end_column,
    )

    assert not val_df.empty
    val_start = pd.to_datetime(splits[0].val_days[0])
    candidate_train = dataset.frame[dataset.frame[dataset.day_column].isin(splits[0].train_days)]
    assert not candidate_train.empty
    assert pd.to_datetime(train_df[dataset.label_end_column]).lt(val_start).all()
    assert len(train_df) < len(candidate_train)


@pytest.mark.parametrize("model_type", ["sklearn_gb", "numpy_mlp"])
def test_ml_train_predict_and_merge(tmp_path: Path, model_type: str) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)

    artifacts, pipeline = ml_train.train_model(
        dataset,
        folds=2,
        gap=0,
        model_type=model_type,
        random_seed=7,
        model_params={
            "hidden_layer_sizes": (24, 12),
            "epochs": 200,
            "learning_rate": 0.01,
            "l2_penalty": 1e-4,
            "activation": "tanh",
            "patience": 25,
        }
        if model_type == "numpy_mlp"
        else {},
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

    train_manifest = json.loads((ml_dir / "train_manifest.json").read_text(encoding="utf-8"))
    assert train_manifest["model_type"] == model_type

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


def test_numpy_mlp_is_deterministic(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)
    model_params = {
        "hidden_layer_sizes": (24, 12),
        "epochs": 200,
        "learning_rate": 0.01,
        "l2_penalty": 1e-4,
        "activation": "tanh",
        "patience": 25,
    }

    artifacts_a, pipeline_a = ml_train.train_model(
        dataset,
        folds=2,
        gap=0,
        model_type="numpy_mlp",
        random_seed=11,
        model_params=model_params,
    )
    artifacts_b, pipeline_b = ml_train.train_model(
        dataset,
        folds=2,
        gap=0,
        model_type="numpy_mlp",
        random_seed=11,
        model_params=model_params,
    )

    feature_frame = dataset.frame[dataset.features]
    preds_a = pipeline_a.predict(feature_frame)
    preds_b = pipeline_b.predict(feature_frame)

    assert artifacts_a.model_id == artifacts_b.model_id
    assert np.allclose(preds_a, preds_b)
    assert np.allclose(
        artifacts_a.feature_importance["importance"].to_numpy(),
        artifacts_b.feature_importance["importance"].to_numpy(),
    )


def test_ml_aggregate_metrics_weight_fold_sizes(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False, periods=16)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)

    artifacts, _ = ml_train.train_model(
        dataset,
        folds=3,
        gap=0,
        model_type="sklearn_gb",
        random_seed=11,
        model_params={},
    )

    folds = artifacts.metrics["folds"]
    total_rows = sum(entry["val_rows"] for entry in folds)
    expected_rmse = np.sqrt(sum(entry["val_rows"] * (entry["rmse"] ** 2) for entry in folds) / total_rows)
    expected_mae = sum(entry["val_rows"] * entry["mae"] for entry in folds) / total_rows

    assert artifacts.metrics["aggregate"]["rmse"] == pytest.approx(expected_rmse)
    assert artifacts.metrics["aggregate"]["mae"] == pytest.approx(expected_mae)


def test_ml_numpy_backend_writes_compatible_artifacts(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    dataset = ml_dataset.build_dataset(joined_path=joined_dir, horizon_days=2)

    artifacts, pipeline = ml_train.train_model(
        dataset,
        folds=2,
        gap=0,
        model_type="numpy_mlp",
        random_seed=11,
        model_params={
            "hidden_layer_sizes": (16, 8),
            "epochs": 80,
            "learning_rate": 0.02,
            "l2_penalty": 1e-4,
            "activation": "relu",
            "patience": 20,
        },
    )

    output_dir = tmp_path / "outputs" / "run_nn"
    output_dir.mkdir(parents=True, exist_ok=True)
    ml_train._write_artifacts(output_dir=output_dir, artifacts=artifacts, pipeline=pipeline)

    manifest = ml_predict.run_prediction(
        joined_path=joined_dir,
        output_dir=output_dir,
        allow_exogenous=[],
    )

    latest = pd.read_csv(output_dir / "ml" / "predictions_latest.csv")
    feature_importance = pd.read_csv(output_dir / "ml" / "feature_importance.csv")
    train_manifest = json.loads((output_dir / "ml" / "train_manifest.json").read_text(encoding="utf-8"))

    assert manifest["model_type"] == "numpy_mlp"
    assert train_manifest["model_type"] == "numpy_mlp"
    assert len(latest) == 2
    assert latest["yhat"].notna().all()
    assert feature_importance["importance"].ge(0).all()
    assert feature_importance["importance"].sum() > 0


def test_ml_benchmark_cli_writes_artifacts_and_manifest(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    output_dir = tmp_path / "outputs" / "benchmark_run"
    _write_run_manifest(output_dir, "benchmark_run")

    canonical_ml_dir = output_dir / "ml"
    canonical_ml_dir.mkdir(parents=True, exist_ok=True)
    sentinel_predictions = canonical_ml_dir / "predictions_latest.csv"
    sentinel_content = "symbol,day,yhat\nAAA,2024-01-01,0.1234\n"
    sentinel_predictions.write_text(sentinel_content, encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "market_monitor",
            "ml",
            "benchmark",
            "--joined-path",
            str(joined_dir),
            "--output-dir",
            str(output_dir),
            "--model-types",
            "sklearn_gb,numpy_mlp",
            "--horizon-days",
            "2",
            "--folds",
            "2",
            "--gap",
            "0",
            "--seed",
            "11",
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[1],
    )
    assert result.returncode == 0, result.stderr or result.stdout

    benchmark_root = output_dir / "ml" / "benchmark"
    assert (benchmark_root / "benchmark_metrics.csv").exists()
    assert (benchmark_root / "benchmark_summary.json").exists()
    assert (benchmark_root / "benchmark_report.md").exists()
    _assert_model_artifact_bundle(benchmark_root / "sklearn_gb")
    _assert_model_artifact_bundle(benchmark_root / "numpy_mlp")

    summary = json.loads((benchmark_root / "benchmark_summary.json").read_text(encoding="utf-8"))
    assert summary["primary_metric"] == "rmse"
    assert summary["winner"]["model_type"] in {"sklearn_gb", "numpy_mlp"}
    assert summary["models"][0]["rank"] == 1
    assert summary["models"][0]["model_type"] == summary["winner"]["model_type"]
    expected_ranking = sorted(
        summary["models"],
        key=lambda row: (
            row["aggregate"]["rmse"],
            row["aggregate"]["mae"],
            -row["aggregate"]["r2"],
            row["model_type"],
        ),
    )
    assert [row["model_type"] for row in summary["models"]] == [
        row["model_type"] for row in expected_ranking
    ]

    metrics = pd.read_csv(benchmark_root / "benchmark_metrics.csv")
    assert list(metrics.columns) == [
        "model_type",
        "fold",
        "rmse",
        "mae",
        "r2",
        "train_start",
        "train_end",
        "val_start",
        "val_end",
        "model_id",
        "featureset_id",
    ]
    assert set(metrics["model_type"]) == {"sklearn_gb", "numpy_mlp"}

    manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["ml"]["benchmark"]["benchmark_summary"] == "ml/benchmark/benchmark_summary.json"
    assert manifest["ml"]["benchmark"]["benchmark_metrics"] == "ml/benchmark/benchmark_metrics.csv"
    assert sentinel_predictions.read_text(encoding="utf-8") == sentinel_content


def test_ml_benchmark_summary_is_deterministic(tmp_path: Path) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    output_a = tmp_path / "outputs" / "bench_a"
    output_b = tmp_path / "outputs" / "bench_b"
    _write_run_manifest(output_a, "bench_a")
    _write_run_manifest(output_b, "bench_b")

    summary_a = ml_benchmark.run_benchmark(
        joined_path=joined_dir,
        output_dir=output_a,
        model_types=["sklearn_gb", "numpy_mlp"],
        horizon_days=2,
        folds=2,
        gap=0,
        seed=11,
        allow_exogenous=[],
        min_rmse_improvement=0.01,
        max_mae_regression=0.005,
    )
    summary_b = ml_benchmark.run_benchmark(
        joined_path=joined_dir,
        output_dir=output_b,
        model_types=["sklearn_gb", "numpy_mlp"],
        horizon_days=2,
        folds=2,
        gap=0,
        seed=11,
        allow_exogenous=[],
        min_rmse_improvement=0.01,
        max_mae_regression=0.005,
    )

    assert summary_a == summary_b
    assert (output_a / "ml" / "benchmark" / "benchmark_summary.json").read_text(encoding="utf-8") == (
        output_b / "ml" / "benchmark" / "benchmark_summary.json"
    ).read_text(encoding="utf-8")


def test_attach_ml_predictions_drops_stale_symbol_rows(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "run_stale"
    ml_dir = output_dir / "ml"
    ml_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"symbol": "AAA", "day": "2024-01-08", "yhat": 0.11},
            {"symbol": "BBB", "day": "2024-01-10", "yhat": 0.22},
        ]
    ).to_csv(ml_dir / "predictions_latest.csv", index=False)
    (ml_dir / "predict_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "model_id": "model_123",
                "featureset_id": "features_123",
                "frontier_day": "2024-01-10",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    merged = _attach_ml_predictions(
        pd.DataFrame({"symbol": ["AAA", "BBB"]}),
        output_dir,
        frontier_day="2024-01-10",
    )

    merged = merged.set_index("symbol")
    assert pd.isna(merged.loc["AAA", "ml_signal"])
    assert merged.loc["BBB", "ml_signal"] == pytest.approx(0.22)
    assert merged.loc["BBB", "ml_model_id"] == "model_123"


def test_ml_benchmark_xgboost_failure_is_clean(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    joined_dir = _write_joined_dataset(tmp_path, include_raw_exogenous=False)
    output_dir = tmp_path / "outputs" / "bench_xgb"
    _write_run_manifest(output_dir, "bench_xgb")

    def _raise_no_xgboost():
        raise RuntimeError("xgboost is not installed. Install it or use --model-type sklearn_gb.")

    monkeypatch.setattr("market_monitor.ml.train_xgb._resolve_xgboost", _raise_no_xgboost)

    with pytest.raises(RuntimeError, match="xgboost is not installed"):
        ml_benchmark.run_benchmark(
            joined_path=joined_dir,
            output_dir=output_dir,
            model_types=["xgboost"],
            horizon_days=2,
            folds=2,
            gap=0,
            seed=11,
            allow_exogenous=[],
            min_rmse_improvement=0.01,
            max_mae_regression=0.005,
        )
