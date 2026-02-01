from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from market_monitor.hash_utils import hash_manifest
from market_monitor.ml.dataset import load_prediction_frame, update_run_manifest


def _load_train_manifest(output_dir: Path) -> dict[str, Any]:
    manifest_path = output_dir / "ml" / "train_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Training manifest not found at {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _load_pipeline(output_dir: Path):
    model_path = output_dir / "ml" / "model.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"Model pipeline not found at {model_path}")
    import joblib

    return joblib.load(model_path)


def _write_predictions(
    *,
    output_dir: Path,
    predictions: pd.DataFrame,
    predict_manifest: dict[str, Any],
) -> None:
    ml_dir = output_dir / "ml"
    ml_dir.mkdir(parents=True, exist_ok=True)

    predictions_path = ml_dir / "predictions_by_day.parquet"
    predictions.to_parquet(predictions_path, index=False)

    latest = predictions.sort_values("day").groupby("symbol").tail(1)
    latest = latest.sort_values("symbol").reset_index(drop=True)
    latest_path = ml_dir / "predictions_latest.csv"
    latest.to_csv(latest_path, index=False)

    manifest_path = ml_dir / "predict_manifest.json"
    manifest_path.write_text(
        json.dumps(predict_manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    update_run_manifest(
        output_dir,
        {
            "prediction": {
                "predictions_by_day": "ml/predictions_by_day.parquet",
                "predictions_latest": "ml/predictions_latest.csv",
                "predict_manifest": "ml/predict_manifest.json",
                "model_id": predict_manifest.get("model_id"),
                "featureset_id": predict_manifest.get("featureset_id"),
            }
        },
    )


def run_prediction(
    *,
    joined_path: Path,
    output_dir: Path,
    allow_exogenous: list[str],
) -> dict[str, Any]:
    train_manifest = _load_train_manifest(output_dir)
    feature_columns = train_manifest["feature_columns"]
    schema = train_manifest.get("schema")
    if not schema:
        raise RuntimeError("Training manifest missing schema information.")

    frame = load_prediction_frame(
        joined_path=joined_path,
        feature_columns=feature_columns,
        allow_exogenous=allow_exogenous,
    )

    pipeline = _load_pipeline(output_dir)
    predictions = pipeline.predict(frame[feature_columns])

    output = pd.DataFrame(
        {
            "symbol": frame["symbol"],
            "day": frame["day"],
            "yhat": predictions,
        }
    )

    coverage = {
        "min_day": str(output["day"].min()) if not output.empty else "",
        "max_day": str(output["day"].max()) if not output.empty else "",
        "rows": int(len(output)),
    }

    featureset_payload = {
        "schema": schema,
        "feature_columns": feature_columns,
        "label": train_manifest.get("label"),
        "horizon_days": train_manifest.get("horizon_days"),
        "join_manifest_hash": train_manifest.get("join_manifest_hash"),
    }
    featureset_id = hash_manifest(featureset_payload)

    if featureset_id != train_manifest.get("featureset_id"):
        raise RuntimeError("Prediction dataset featureset_id does not match training")

    predict_manifest = {
        "schema_version": 1,
        "model_id": train_manifest.get("model_id"),
        "featureset_id": train_manifest.get("featureset_id"),
        "model_type": train_manifest.get("model_type"),
        "coverage": coverage,
        "rows": int(len(output)),
    }

    _write_predictions(
        output_dir=output_dir,
        predictions=output,
        predict_manifest=predict_manifest,
    )

    return predict_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run ML predictions using trained model artifacts.")
    parser.add_argument("--joined-path", required=True, help="Joined features path (file or dir).")
    parser.add_argument("--output-dir", required=True, help="Run output directory (outputs/<run_id>).")
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
    manifest = run_prediction(
        joined_path=Path(args.joined_path),
        output_dir=Path(args.output_dir),
        allow_exogenous=allow_exogenous,
    )

    print(f"[ml.predict] model_id={manifest.get('model_id')}")
    print(f"[ml.predict] featureset_id={manifest.get('featureset_id')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
