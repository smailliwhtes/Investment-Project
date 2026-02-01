from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from market_monitor.hash_utils import hash_manifest
from market_monitor.ml.dataset import DatasetInfo, build_dataset, update_run_manifest
from market_monitor.ml.split import build_walk_forward_splits, filter_frame_by_days


@dataclass(frozen=True)
class TrainingArtifacts:
    model_id: str
    featureset_id: str
    metrics: dict[str, Any]
    feature_importance: pd.DataFrame
    train_manifest: dict[str, Any]


def _parse_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "y"}


def _resolve_xgboost():
    try:
        import xgboost  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "xgboost is not installed. Install it or use --model-type sklearn_gb."
        ) from exc
    return xgboost


def _resolve_tree_method(xgb) -> str:
    prefer_gpu = _parse_bool(os.getenv("MARKET_APP_ML_GPU"))
    has_cuda = False
    cuda_check = getattr(xgb.core, "_has_cuda_support", None)
    if callable(cuda_check):
        has_cuda = bool(cuda_check())
    if prefer_gpu and has_cuda:
        return "gpu_hist"
    return "hist"


def _build_model(model_type: str, random_seed: int, params: dict[str, Any]) -> Any:
    if model_type == "xgboost":
        xgb = _resolve_xgboost()
        tree_method = _resolve_tree_method(xgb)
        return xgb.XGBRegressor(
            n_estimators=params.get("n_estimators", 200),
            max_depth=params.get("max_depth", 4),
            learning_rate=params.get("learning_rate", 0.08),
            subsample=params.get("subsample", 0.9),
            colsample_bytree=params.get("colsample_bytree", 0.9),
            random_state=random_seed,
            tree_method=tree_method,
            objective="reg:squarederror",
        )
    if model_type == "sklearn_gb":
        return HistGradientBoostingRegressor(
            max_depth=params.get("max_depth", 4),
            learning_rate=params.get("learning_rate", 0.08),
            max_iter=params.get("max_iter", 200),
            random_state=random_seed,
        )
    raise ValueError(f"Unsupported model_type: {model_type}")


def _build_pipeline(model: Any) -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", model),
        ]
    )


def _fold_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def _collect_feature_importance(model: Any, feature_names: list[str]) -> pd.DataFrame:
    importances: dict[str, float] = {}
    if hasattr(model, "feature_importances_"):
        values = getattr(model, "feature_importances_")
        importances = {name: float(val) for name, val in zip(feature_names, values)}
    elif hasattr(model, "coef_"):
        values = np.ravel(getattr(model, "coef_"))
        importances = {name: float(abs(val)) for name, val in zip(feature_names, values)}
    elif hasattr(model, "get_booster"):
        booster = model.get_booster()
        scores = booster.get_score(importance_type="gain")
        importances = {name: float(scores.get(name, 0.0)) for name in feature_names}

    if not importances:
        importances = {name: 0.0 for name in feature_names}

    frame = pd.DataFrame(
        {
            "feature": list(importances.keys()),
            "importance": list(importances.values()),
        }
    )
    frame = frame.sort_values("importance", ascending=False).reset_index(drop=True)
    return frame


def train_model(
    dataset: DatasetInfo,
    *,
    folds: int,
    model_type: str,
    random_seed: int,
    model_params: dict[str, Any],
) -> tuple[TrainingArtifacts, Pipeline]:
    np.random.seed(random_seed)

    df = dataset.frame.copy()
    df = df.sort_values([dataset.day_column, dataset.symbol_column])
    feature_cols = dataset.features

    splits = build_walk_forward_splits(df[dataset.day_column].unique(), folds)
    metrics: list[dict[str, Any]] = []

    X_all = df[feature_cols].to_numpy()
    y_all = df[dataset.label].to_numpy()

    for split in splits:
        train_df = filter_frame_by_days(df, dataset.day_column, split.train_days)
        val_df = filter_frame_by_days(df, dataset.day_column, split.val_days)

        X_train = train_df[feature_cols].to_numpy()
        y_train = train_df[dataset.label].to_numpy()
        X_val = val_df[feature_cols].to_numpy()
        y_val = val_df[dataset.label].to_numpy()

        model = _build_model(model_type, random_seed, model_params)
        pipeline = _build_pipeline(model)
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_val)
        fold_metrics = _fold_metrics(y_val, y_pred)
        metrics.append(
            {
                "fold": split.fold,
                "train_days": [split.train_days[0], split.train_days[-1]],
                "val_days": [split.val_days[0], split.val_days[-1]],
                **fold_metrics,
            }
        )

    model = _build_model(model_type, random_seed, model_params)
    final_pipeline = _build_pipeline(model)
    final_pipeline.fit(X_all, y_all)

    model_id = hash_manifest(
        {
            "dataset_hash": dataset.dataset_hash,
            "featureset_id": dataset.featureset_id,
            "model_type": model_type,
            "model_params": model_params,
            "seed": random_seed,
        }
    )
    featureset_id = dataset.featureset_id

    aggregate = {
        "rmse": float(np.mean([entry["rmse"] for entry in metrics])),
        "mae": float(np.mean([entry["mae"] for entry in metrics])),
        "r2": float(np.mean([entry["r2"] for entry in metrics])),
    }

    metrics_payload = {
        "folds": metrics,
        "aggregate": aggregate,
    }

    model_step = final_pipeline.named_steps["model"]
    feature_importance = _collect_feature_importance(model_step, feature_cols)

    train_manifest = {
        "schema_version": 1,
        "model_id": model_id,
        "featureset_id": featureset_id,
        "dataset_hash": dataset.dataset_hash,
        "join_manifest_hash": dataset.join_manifest.get("content_hash") if dataset.join_manifest else None,
        "label": dataset.label,
        "horizon_days": int(dataset.label.split("_")[-1].replace("d", "")),
        "schema": dataset.schema,
        "feature_columns": feature_cols,
        "excluded_exogenous_columns": dataset.excluded_exogenous,
        "coverage": dataset.coverage,
        "split_boundaries": [
            {
                "fold": split.fold,
                "train_start": split.train_days[0],
                "train_end": split.train_days[-1],
                "val_start": split.val_days[0],
                "val_end": split.val_days[-1],
            }
            for split in splits
        ],
        "model_type": model_type,
        "model_params": model_params,
        "seed": random_seed,
    }

    return TrainingArtifacts(
        model_id=model_id,
        featureset_id=featureset_id,
        metrics=metrics_payload,
        feature_importance=feature_importance,
        train_manifest=train_manifest,
    ), final_pipeline


def _write_artifacts(
    *,
    output_dir: Path,
    artifacts: TrainingArtifacts,
    pipeline: Pipeline,
) -> None:
    ml_dir = output_dir / "ml"
    ml_dir.mkdir(parents=True, exist_ok=True)

    (ml_dir / "metrics.json").write_text(
        json.dumps(artifacts.metrics, indent=2, sort_keys=True), encoding="utf-8"
    )
    artifacts.feature_importance.to_csv(ml_dir / "feature_importance.csv", index=False)
    (ml_dir / "train_manifest.json").write_text(
        json.dumps(artifacts.train_manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    model_path = ml_dir / "model.joblib"
    import joblib

    joblib.dump(pipeline, model_path)

    model_meta = {
        "model_id": artifacts.model_id,
        "featureset_id": artifacts.featureset_id,
        "model_type": artifacts.train_manifest["model_type"],
        "model_path": str(model_path.name),
    }
    (ml_dir / "model.json").write_text(
        json.dumps(model_meta, indent=2, sort_keys=True), encoding="utf-8"
    )

    update_run_manifest(
        output_dir,
        {
            "training": {
                "model_id": artifacts.model_id,
                "featureset_id": artifacts.featureset_id,
                "train_manifest": "ml/train_manifest.json",
                "metrics": "ml/metrics.json",
                "feature_importance": "ml/feature_importance.csv",
                "model": "ml/model.json",
            }
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train ML model on joined market + GDELT features.")
    parser.add_argument("--joined-path", required=True, help="Joined features path (file or dir).")
    parser.add_argument("--output-dir", required=True, help="Run output directory (outputs/<run_id>).")
    parser.add_argument("--horizon-days", type=int, default=5, help="Forward return horizon (days).")
    parser.add_argument("--folds", type=int, default=3, help="Number of walk-forward folds.")
    parser.add_argument(
        "--model-type",
        default="xgboost",
        choices=["xgboost", "sklearn_gb"],
        help="Model backend.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for determinism.")
    parser.add_argument(
        "--allow-exogenous",
        default="",
        help="Comma-separated list of same-day exogenous columns allowed.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    allow_exogenous = [col.strip() for col in args.allow_exogenous.split(",") if col.strip()]
    dataset = build_dataset(
        joined_path=Path(args.joined_path),
        horizon_days=args.horizon_days,
        allow_exogenous=allow_exogenous,
    )

    model_params = {}
    artifacts, pipeline = train_model(
        dataset,
        folds=args.folds,
        model_type=args.model_type,
        random_seed=args.seed,
        model_params=model_params,
    )

    _write_artifacts(
        output_dir=Path(args.output_dir),
        artifacts=artifacts,
        pipeline=pipeline,
    )

    print(f"[ml.train] model_id={artifacts.model_id}")
    print(f"[ml.train] featureset_id={artifacts.featureset_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
