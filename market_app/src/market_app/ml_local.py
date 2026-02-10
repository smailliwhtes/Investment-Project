from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from market_app.geopolitics_local import build_geopolitics_features, lag_geopolitics_features
from market_app.local_config import ConfigResult
from market_app.manifest_local import hash_text
from market_app.ohlcv_local import load_ohlcv, resolve_ohlcv_dir
from market_app.symbols_local import load_symbols


@dataclass(frozen=True)
class TrainingReport:
    model_id: str
    output_dir: Path
    model_dir: Path
    metrics: dict[str, Any]
    manifest: dict[str, Any]


def _build_market_daily_features(symbol: str, frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    df = frame.copy()
    df["day"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["day"]).sort_values("day")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["close"])
    if df.empty:
        return pd.DataFrame()
    df["return_1d"] = df["close"].pct_change()
    df["return_5d"] = df["close"].pct_change(periods=5)
    df["volatility_5d"] = df["return_1d"].rolling(5).std(ddof=0)
    df["volatility_20d"] = df["return_1d"].rolling(20).std(ddof=0)
    df["adv20_usd"] = (df["close"] * df["volume"]).rolling(20).mean()
    df["symbol"] = symbol
    df["day"] = df["day"].dt.strftime("%Y-%m-%d")
    return df[
        [
            "day",
            "symbol",
            "close",
            "return_1d",
            "return_5d",
            "volatility_5d",
            "volatility_20d",
            "adv20_usd",
        ]
    ]


def _build_label(frame: pd.DataFrame, horizon_days: int) -> pd.DataFrame:
    df = frame.sort_values(["symbol", "day"]).copy()
    df["future_vol"] = (
        df.groupby("symbol")["return_1d"].rolling(horizon_days).std(ddof=0).shift(-horizon_days)
    ).reset_index(level=0, drop=True)
    return df


def _walk_forward_splits(days: list[str], folds: int, gap: int) -> list[tuple[list[str], list[str]]]:
    days_sorted = sorted(set(days))
    if len(days_sorted) < folds + 1:
        raise ValueError("Not enough unique days to build walk-forward splits.")
    total_days = len(days_sorted)
    boundaries = [int(np.floor(total_days * (idx + 1) / (folds + 1))) for idx in range(folds + 1)]
    boundaries[-1] = total_days
    splits = []
    for idx in range(folds):
        train_end = boundaries[idx]
        val_end = boundaries[idx + 1]
        train_days = days_sorted[:train_end]
        val_start = train_end + gap
        val_days = days_sorted[val_start:val_end]
        if not train_days or not val_days:
            raise ValueError("Walk-forward split produced empty train or validation window.")
        splits.append((train_days, val_days))
    return splits


def _build_pipeline() -> Pipeline:
    return Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", LogisticRegression(max_iter=200, random_state=42)),
        ]
    )


def _evaluate(y_true: np.ndarray, y_pred: np.ndarray, y_prob: np.ndarray) -> dict[str, float]:
    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "f1": float(f1_score(y_true, y_pred)),
    }
    try:
        metrics["roc_auc"] = float(roc_auc_score(y_true, y_prob))
    except ValueError:
        metrics["roc_auc"] = float("nan")
    return metrics


def run_training(config_result: ConfigResult, *, asof_end: str, run_id: str | None) -> TrainingReport:
    config = config_result.config
    symbols_dir = Path(config["paths"]["symbols_dir"])
    ohlcv_dir = resolve_ohlcv_dir(Path(config["paths"]["ohlcv_dir"]), logger=_NullLogger())
    geopolitics_dir = Path(config["paths"].get("geopolitics_dir", "") or "")
    output_root = Path(config["paths"]["training_output_dir"]).resolve()
    model_root = Path(config["paths"]["model_dir"]).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    model_root.mkdir(parents=True, exist_ok=True)

    symbol_result = load_symbols(symbols_dir, config, logger=_NullLogger())

    market_frames = []
    for symbol in symbol_result.symbols["symbol"].tolist():
        ohlcv = load_ohlcv(symbol, ohlcv_dir)
        market_frames.append(_build_market_daily_features(symbol, ohlcv.frame))

    market_df = pd.concat([df for df in market_frames if not df.empty], ignore_index=True)
    if market_df.empty:
        raise RuntimeError("No market data available for training.")

    market_df = market_df[market_df["day"] <= asof_end].copy()
    market_df = _build_label(market_df, horizon_days=5)
    market_df = market_df.dropna(subset=["future_vol"])

    geopolitics_result = build_geopolitics_features(
        geopolitics_path=geopolitics_dir, output_dir=output_root
    )
    geopolitics_frame = lag_geopolitics_features(geopolitics_result.frame, lag_days=1)
    if not geopolitics_frame.empty:
        market_df = market_df.merge(geopolitics_frame, on="day", how="left")

    threshold = float(market_df["future_vol"].quantile(0.75))
    market_df["label_high_vol"] = market_df["future_vol"] > threshold

    feature_columns = [
        "return_1d",
        "return_5d",
        "volatility_5d",
        "volatility_20d",
        "adv20_usd",
    ]
    geopolitics_cols = [col for col in market_df.columns if col.startswith(("events_count", "tone_", "goldstein_", "mentions_", "sources_", "articles_", "root_", "quad_"))]
    feature_columns.extend(sorted(set(geopolitics_cols)))

    feature_columns = [col for col in feature_columns if col in market_df.columns]
    X = market_df[feature_columns]
    y = market_df["label_high_vol"].astype(int)

    splits = _walk_forward_splits(market_df["day"].unique().tolist(), folds=3, gap=0)
    fold_metrics = []
    for idx, (train_days, val_days) in enumerate(splits, start=1):
        train_mask = market_df["day"].isin(train_days)
        val_mask = market_df["day"].isin(val_days)
        pipeline = _build_pipeline()
        pipeline.fit(X[train_mask], y[train_mask])
        y_prob = pipeline.predict_proba(X[val_mask])[:, 1]
        y_pred = (y_prob >= 0.5).astype(int)
        metrics = _evaluate(y[val_mask].to_numpy(), y_pred, y_prob)
        fold_metrics.append({"fold": idx, **metrics})

    final_pipeline = _build_pipeline()
    final_pipeline.fit(X, y)

    model_id = hash_text(
        json.dumps(
            {
                "config_hash": config_result.config_hash,
                "asof_end": asof_end,
                "features": feature_columns,
                "threshold": threshold,
            },
            sort_keys=True,
        )
    )[:12]
    run_id = run_id or f"train_{model_id}"
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    model_dir = model_root / model_id
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / "model.joblib"
    import joblib

    joblib.dump(final_pipeline, model_path)

    aggregate = {
        "accuracy": float(np.mean([entry["accuracy"] for entry in fold_metrics])),
        "f1": float(np.mean([entry["f1"] for entry in fold_metrics])),
        "roc_auc": float(np.nanmean([entry["roc_auc"] for entry in fold_metrics])),
    }

    manifest = {
        "schema_version": 1,
        "model_id": model_id,
        "label": "label_high_vol",
        "horizon_days": 5,
        "threshold": threshold,
        "asof_end": asof_end,
        "features": feature_columns,
        "config_hash": config_result.config_hash,
        "geopolitics_input_hash": geopolitics_result.input_hash,
    }
    (model_dir / "model_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    metrics_payload = {
        "folds": fold_metrics,
        "aggregate": aggregate,
    }
    (output_dir / "metrics.json").write_text(
        json.dumps(metrics_payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    feature_importance = pd.DataFrame({"feature": feature_columns})
    feature_importance.to_csv(output_dir / "feature_importance.csv", index=False)
    report_lines = [
        "# Offline ML Training Report",
        "",
        f"- model_id: {model_id}",
        f"- asof_end: {asof_end}",
        f"- label: label_high_vol (next-5d realized volatility > {threshold:.6f})",
        "",
        "## Aggregate metrics",
        f"- accuracy: {aggregate['accuracy']:.4f}",
        f"- f1: {aggregate['f1']:.4f}",
        f"- roc_auc: {aggregate['roc_auc']:.4f}",
    ]
    (output_dir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")

    return TrainingReport(
        model_id=model_id,
        output_dir=output_dir,
        model_dir=model_dir,
        metrics=metrics_payload,
        manifest=manifest,
    )


def predict_latest(
    *,
    config: dict[str, Any],
    features: pd.DataFrame,
    scored: pd.DataFrame,
    geopolitics_dir: Path,
    output_dir: Path,
    model_dir: Path,
) -> pd.DataFrame:
    if features.empty or scored.empty:
        return scored
    model_manifest = _load_latest_manifest(model_dir)
    if model_manifest is None:
        return scored
    import joblib

    model_id = model_manifest["model_id"]
    model_path = model_dir / model_id / "model.joblib"
    if not model_path.exists():
        return scored
    pipeline = joblib.load(model_path)

    geo_result = build_geopolitics_features(geopolitics_path=geopolitics_dir, output_dir=output_dir)
    geo_lagged = lag_geopolitics_features(geo_result.frame, lag_days=1)
    merged = features.copy()
    if "as_of_date" in merged.columns:
        merged = merged.rename(columns={"as_of_date": "day"})
    if "day" not in merged.columns:
        merged["day"] = config.get("run", {}).get("as_of_date")
    if not geo_lagged.empty and "day" in merged.columns:
        merged = merged.merge(geo_lagged, on="day", how="left")

    feature_columns = model_manifest.get("features", [])
    missing = [col for col in feature_columns if col not in merged.columns]
    if missing:
        return scored

    X = merged[feature_columns]
    prob = pipeline.predict_proba(X)[:, 1]
    enriched = scored.copy()
    pred_df = pd.DataFrame(
        {
            "symbol": merged["symbol"],
            "predicted_risk_signal": prob,
        }
    )
    enriched = enriched.merge(pred_df, on="symbol", how="left")
    enriched["model_id"] = model_id
    enriched["model_schema_version"] = model_manifest.get("schema_version", 1)
    return enriched


def _load_latest_manifest(model_root: Path) -> dict[str, Any] | None:
    if not model_root.exists():
        return None
    manifests = sorted(model_root.glob("*/model_manifest.json"))
    if not manifests:
        return None
    latest = max(manifests, key=lambda path: path.stat().st_mtime)
    return json.loads(latest.read_text(encoding="utf-8"))


class _NullLogger:
    def warning(self, *args, **kwargs) -> None:
        return None

    def info(self, *args, **kwargs) -> None:
        return None
