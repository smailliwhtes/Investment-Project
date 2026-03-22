from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from market_monitor.ml.dataset import build_dataset, update_run_manifest
from market_monitor.ml.predict import run_prediction
from market_monitor.ml.train_xgb import _canonical_model_type, _write_artifacts, train_model

DEFAULT_MODEL_TYPES = ("sklearn_gb", "numpy_mlp")
DEFAULT_SEED = 42
DEFAULT_MIN_RMSE_IMPROVEMENT = 0.01
DEFAULT_MAX_MAE_REGRESSION = 0.005
SUPPORTED_MODEL_TYPES = frozenset({"xgboost", "sklearn_gb", "numpy_mlp"})


def _default_model_params(model_type: str) -> dict[str, Any]:
    if model_type != "numpy_mlp":
        return {}
    return {
        "hidden_layer_sizes": (64, 32),
        "learning_rate": 0.01,
        "epochs": 300,
        "l2_penalty": 1e-4,
        "activation": "tanh",
        "patience": 30,
    }


def _parse_csv_items(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _parse_model_types(raw_value: str) -> list[str]:
    model_types: list[str] = []
    seen: set[str] = set()
    for item in _parse_csv_items(raw_value):
        canonical = _canonical_model_type(item)
        if canonical not in SUPPORTED_MODEL_TYPES:
            supported = ", ".join(sorted(SUPPORTED_MODEL_TYPES))
            raise ValueError(f"Unsupported model_type '{item}'. Supported values: {supported}")
        if canonical in seen:
            continue
        seen.add(canonical)
        model_types.append(canonical)
    if not model_types:
        raise ValueError("model-types must include at least one supported model type")
    return model_types


def _safe_relative_change(*, baseline: float, candidate: float) -> float:
    if baseline == 0.0:
        return 0.0 if candidate == 0.0 else float("inf")
    return (candidate - baseline) / baseline


def _safe_relative_improvement(*, baseline: float, candidate: float) -> float:
    if baseline == 0.0:
        return 0.0 if candidate == 0.0 else float("inf")
    return (baseline - candidate) / baseline


def _round_metric(value: float) -> float:
    return round(float(value), 10)


def _build_model_rows(
    *,
    artifacts_by_model: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    model_rows: list[dict[str, Any]] = []

    for model_type, payload in artifacts_by_model.items():
        artifacts = payload["artifacts"]
        aggregate = artifacts.metrics.get("aggregate", {})
        model_rows.append(
            {
                "model_type": model_type,
                "model_id": artifacts.model_id,
                "featureset_id": artifacts.featureset_id,
                "dataset_hash": artifacts.train_manifest.get("dataset_hash"),
                "artifact_dir": f"ml/benchmark/{model_type}",
                "aggregate": {
                    "rmse": _round_metric(aggregate.get("rmse", 0.0)),
                    "mae": _round_metric(aggregate.get("mae", 0.0)),
                    "r2": _round_metric(aggregate.get("r2", 0.0)),
                },
            }
        )

        for fold in artifacts.metrics.get("folds", []):
            rows.append(
                {
                    "model_type": model_type,
                    "fold": int(fold["fold"]),
                    "rmse": _round_metric(fold["rmse"]),
                    "mae": _round_metric(fold["mae"]),
                    "r2": _round_metric(fold["r2"]),
                    "train_start": fold["train_days"][0],
                    "train_end": fold["train_days"][1],
                    "val_start": fold["val_days"][0],
                    "val_end": fold["val_days"][1],
                    "model_id": artifacts.model_id,
                    "featureset_id": artifacts.featureset_id,
                }
            )

    ranked_models = sorted(
        model_rows,
        key=lambda row: (
            row["aggregate"]["rmse"],
            row["aggregate"]["mae"],
            -row["aggregate"]["r2"],
            row["model_type"],
        ),
    )
    for index, row in enumerate(ranked_models, start=1):
        row["rank"] = index

    return rows, ranked_models


def _promotion_decision(
    *,
    ranked_models: list[dict[str, Any]],
    min_rmse_improvement: float,
    max_mae_regression: float,
) -> tuple[bool, str]:
    baseline = next((row for row in ranked_models if row["model_type"] == "sklearn_gb"), None)
    challenger = next((row for row in ranked_models if row["model_type"] == "numpy_mlp"), None)

    if baseline is None:
        return False, "No sklearn_gb baseline was benchmarked."
    if challenger is None:
        return False, "No numpy_mlp challenger was benchmarked."

    baseline_rmse = baseline["aggregate"]["rmse"]
    challenger_rmse = challenger["aggregate"]["rmse"]
    baseline_mae = baseline["aggregate"]["mae"]
    challenger_mae = challenger["aggregate"]["mae"]
    rmse_improvement = _safe_relative_improvement(
        baseline=baseline_rmse,
        candidate=challenger_rmse,
    )
    mae_regression = _safe_relative_change(
        baseline=baseline_mae,
        candidate=challenger_mae,
    )

    if rmse_improvement < min_rmse_improvement:
        return (
            False,
            "numpy_mlp did not beat sklearn_gb by the required relative RMSE threshold "
            f"({rmse_improvement:.4%} < {min_rmse_improvement:.4%}).",
        )
    if mae_regression > max_mae_regression:
        return (
            False,
            "numpy_mlp regressed MAE beyond the allowed threshold "
            f"({mae_regression:.4%} > {max_mae_regression:.4%}).",
        )
    return (
        True,
        "numpy_mlp met the RMSE improvement threshold and stayed within the MAE regression budget "
        f"({rmse_improvement:.4%} RMSE improvement, {mae_regression:.4%} MAE regression).",
    )


def _write_benchmark_artifacts(
    *,
    output_dir: Path,
    metrics_rows: list[dict[str, Any]],
    summary_payload: dict[str, Any],
) -> None:
    benchmark_root = output_dir / "ml" / "benchmark"
    benchmark_root.mkdir(parents=True, exist_ok=True)

    metrics_df = pd.DataFrame(metrics_rows)
    if not metrics_df.empty:
        metrics_df = metrics_df.sort_values(["model_type", "fold"]).reset_index(drop=True)
    metrics_path = benchmark_root / "benchmark_metrics.csv"
    metrics_df.to_csv(metrics_path, index=False, float_format="%.10f", lineterminator="\n")

    summary_path = benchmark_root / "benchmark_summary.json"
    summary_path.write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    lines = [
        "# ML Benchmark Report",
        "",
        f"- dataset_hash: {summary_payload['dataset_hash']}",
        f"- featureset_id: {summary_payload['featureset_id']}",
        f"- primary_metric: {summary_payload['primary_metric']}",
        f"- winner: {summary_payload['winner']['model_type']}",
        f"- promotion_recommended: {summary_payload['promotion_recommended']}",
        f"- promotion_reason: {summary_payload['promotion_reason']}",
        "",
        "## Thresholds",
        f"- min_rmse_improvement: {summary_payload['thresholds']['min_rmse_improvement']:.4%}",
        f"- max_mae_regression: {summary_payload['thresholds']['max_mae_regression']:.4%}",
        "",
        "## Ranked Models",
        "",
        "| rank | model_type | rmse | mae | r2 | model_id |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for model in summary_payload["models"]:
        aggregate = model["aggregate"]
        lines.append(
            f"| {model['rank']} | {model['model_type']} | {aggregate['rmse']:.6f} | "
            f"{aggregate['mae']:.6f} | {aggregate['r2']:.6f} | {model['model_id']} |"
        )

    report_path = benchmark_root / "benchmark_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")

    update_run_manifest(
        output_dir,
        {
            "benchmark": {
                "benchmark_metrics": "ml/benchmark/benchmark_metrics.csv",
                "benchmark_summary": "ml/benchmark/benchmark_summary.json",
                "benchmark_report": "ml/benchmark/benchmark_report.md",
                "winner": summary_payload["winner"]["model_type"],
                "promotion_recommended": summary_payload["promotion_recommended"],
                "models": {
                    model["model_type"]: model["artifact_dir"] for model in summary_payload["models"]
                },
            }
        },
    )


def run_benchmark(
    *,
    joined_path: Path,
    output_dir: Path,
    model_types: list[str],
    horizon_days: int,
    folds: int,
    gap: int,
    seed: int,
    allow_exogenous: list[str],
    min_rmse_improvement: float,
    max_mae_regression: float,
) -> dict[str, Any]:
    output_dir = output_dir.expanduser().resolve()
    joined_path = joined_path.expanduser().resolve()
    canonical_model_types = _parse_model_types(",".join(model_types))

    manifest_path = output_dir / "run_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Benchmark output-dir must point to an existing run directory with run_manifest.json: {output_dir}"
        )

    dataset = build_dataset(
        joined_path=joined_path,
        horizon_days=horizon_days,
        allow_exogenous=allow_exogenous,
    )

    benchmark_root = output_dir / "ml" / "benchmark"
    benchmark_root.mkdir(parents=True, exist_ok=True)
    artifacts_by_model: dict[str, dict[str, Any]] = {}
    for model_type in canonical_model_types:
        model_dir = benchmark_root / model_type
        artifacts, pipeline = train_model(
            dataset,
            folds=folds,
            gap=gap,
            model_type=model_type,
            random_seed=seed,
            model_params=_default_model_params(model_type),
        )
        _write_artifacts(
            output_dir=output_dir,
            artifacts=artifacts,
            pipeline=pipeline,
            artifact_root=model_dir,
            update_manifest=False,
        )
        predict_manifest = run_prediction(
            joined_path=joined_path,
            output_dir=output_dir,
            allow_exogenous=allow_exogenous,
            artifact_root=model_dir,
            update_manifest=False,
        )
        artifacts_by_model[model_type] = {
            "artifacts": artifacts,
            "predict_manifest": predict_manifest,
        }

    metrics_rows, ranked_models = _build_model_rows(artifacts_by_model=artifacts_by_model)
    promotion_recommended, promotion_reason = _promotion_decision(
        ranked_models=ranked_models,
        min_rmse_improvement=min_rmse_improvement,
        max_mae_regression=max_mae_regression,
    )

    winner = ranked_models[0]
    summary_payload = {
        "schema_version": 1,
        "models": ranked_models,
        "winner": {
            "model_type": winner["model_type"],
            "model_id": winner["model_id"],
            "rank": winner["rank"],
        },
        "primary_metric": "rmse",
        "promotion_recommended": promotion_recommended,
        "promotion_reason": promotion_reason,
        "thresholds": {
            "min_rmse_improvement": min_rmse_improvement,
            "max_mae_regression": max_mae_regression,
        },
        "seed": seed,
        "dataset_hash": dataset.dataset_hash,
        "featureset_id": dataset.featureset_id,
    }
    _write_benchmark_artifacts(
        output_dir=output_dir,
        metrics_rows=metrics_rows,
        summary_payload=summary_payload,
    )
    return summary_payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark deterministic ML backends on joined market + exogenous features."
    )
    parser.add_argument("--joined-path", required=True, help="Joined features path (file or dir).")
    parser.add_argument("--output-dir", required=True, help="Existing run directory to attach benchmark artifacts to.")
    parser.add_argument(
        "--model-types",
        default=",".join(DEFAULT_MODEL_TYPES),
        help="Comma-separated model backends to benchmark.",
    )
    parser.add_argument("--horizon-days", type=int, default=5, help="Forward return horizon (days).")
    parser.add_argument("--folds", type=int, default=3, help="Number of walk-forward folds.")
    parser.add_argument("--gap", type=int, default=0, help="Embargo gap between train and validation.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED, help="Random seed for determinism.")
    parser.add_argument(
        "--allow-exogenous",
        default="",
        help="Comma-separated list of same-day exogenous columns allowed.",
    )
    parser.add_argument(
        "--min-rmse-improvement",
        type=float,
        default=DEFAULT_MIN_RMSE_IMPROVEMENT,
        help="Minimum relative RMSE improvement required for numpy_mlp promotion.",
    )
    parser.add_argument(
        "--max-mae-regression",
        type=float,
        default=DEFAULT_MAX_MAE_REGRESSION,
        help="Maximum relative MAE regression allowed for numpy_mlp promotion.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    summary = run_benchmark(
        joined_path=Path(args.joined_path),
        output_dir=Path(args.output_dir),
        model_types=_parse_model_types(args.model_types),
        horizon_days=args.horizon_days,
        folds=args.folds,
        gap=args.gap,
        seed=args.seed,
        allow_exogenous=_parse_csv_items(args.allow_exogenous),
        min_rmse_improvement=args.min_rmse_improvement,
        max_mae_regression=args.max_mae_regression,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
