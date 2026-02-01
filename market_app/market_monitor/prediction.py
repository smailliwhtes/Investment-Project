from __future__ import annotations

from dataclasses import dataclass
import importlib
import importlib.util
import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.preprocessing import StandardScaler

from market_monitor.features import compute_features


@dataclass
class PredictionArtifacts:
    panel: pd.DataFrame
    predictions: pd.DataFrame
    metrics: dict[str, float]
    calibration_bins: pd.DataFrame
    calibration_plot_path: Path | None
    model_card: str


def _parse_bool(value: str | None) -> bool:
    if value is None:
        return False
    value = value.strip().lower()
    return value in {"1", "true", "yes", "y"}


def _resolve_training_backend() -> str:
    backend = os.getenv("MARKET_APP_PREDICTION_BACKEND", "sklearn").strip().lower()
    return backend if backend else "sklearn"


def _load_xgboost():
    if importlib.util.find_spec("xgboost") is None:
        return None
    return importlib.import_module("xgboost")


def _resolve_tree_method(xgb) -> tuple[str, bool]:
    prefer_gpu = _parse_bool(os.getenv("MARKET_APP_ENABLE_GPU"))
    has_cuda = False
    cuda_check = getattr(xgb.core, "_has_cuda_support", None)
    if callable(cuda_check):
        has_cuda = bool(cuda_check())
    if prefer_gpu and has_cuda:
        return "gpu_hist", True
    return "hist", False


def _build_training_models():
    backend = _resolve_training_backend()
    if backend == "xgboost":
        xgb = _load_xgboost()
        if xgb is not None:
            tree_method, used_gpu = _resolve_tree_method(xgb)
            backend_label = f"xgboost ({tree_method})" if used_gpu else "xgboost (cpu)"
            ridge = xgb.XGBRegressor(
                n_estimators=200,
                max_depth=4,
                learning_rate=0.08,
                subsample=0.9,
                colsample_bytree=0.9,
                random_state=42,
                tree_method=tree_method,
            )
            logit = xgb.XGBClassifier(
                n_estimators=200,
                max_depth=4,
                learning_rate=0.08,
                subsample=0.9,
                colsample_bytree=0.9,
                random_state=42,
                tree_method=tree_method,
                eval_metric="logloss",
            )
            return backend_label, ridge, logit

    ridge = Ridge(alpha=1.0)
    logit = LogisticRegression(max_iter=200)
    fallback_label = "sklearn"
    if backend == "xgboost":
        fallback_label = "sklearn (xgboost unavailable)"
    return fallback_label, ridge, logit


def build_panel(
    symbols: Iterable[str],
    provider,
    lookback_days: int,
    forward_return_days: int,
    forward_drawdown_days: int,
    min_history_days: int,
    logger,
) -> pd.DataFrame:
    rows: list[dict[str, float | str]] = []
    for symbol in symbols:
        try:
            history = provider.get_history(symbol, lookback_days + forward_drawdown_days + 50)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[prediction] {symbol} history unavailable: {exc}")
            continue

        if history.empty or len(history) < min_history_days:
            continue
        history = history.copy()
        history["Date"] = pd.to_datetime(history["Date"], errors="coerce")
        history = history.dropna(subset=["Date"]).sort_values("Date")
        history = history.reset_index(drop=True)
        close = history["Close"].to_numpy(dtype=float)
        dates = history["Date"].to_numpy()

        for idx in range(min_history_days, len(history) - forward_drawdown_days):
            window = history.iloc[max(0, idx - lookback_days) : idx + 1]
            if len(window) < min_history_days:
                continue
            features = compute_features(window)
            label_return = _forward_return(close, idx, forward_return_days)
            label_drawdown = _forward_drawdown(close, idx, forward_drawdown_days)
            rows.append(
                {
                    "symbol": symbol,
                    "date": dates[idx],
                    **features,
                    "forward_return_20d": label_return,
                    "forward_max_drawdown_60d": label_drawdown,
                }
            )
    return pd.DataFrame(rows)


def train_and_predict(
    panel: pd.DataFrame,
    outputs_dir: Path,
    drawdown_threshold: float,
    folds: int,
    embargo_days: int,
) -> PredictionArtifacts:
    panel = panel.dropna(subset=["forward_return_20d", "forward_max_drawdown_60d"]).copy()
    panel["drawdown_exceed"] = (panel["forward_max_drawdown_60d"] <= drawdown_threshold).astype(
        int
    )
    feature_cols = _feature_columns(panel)
    panel = panel.dropna(subset=feature_cols).copy()

    if panel.empty:
        return PredictionArtifacts(
            panel=panel,
            predictions=pd.DataFrame(),
            metrics={},
            calibration_bins=pd.DataFrame(),
            calibration_plot_path=None,
            model_card="No panel data available for prediction.",
        )

    panel = panel.sort_values("date")
    X = panel[feature_cols].to_numpy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    y_return = panel["forward_return_20d"].to_numpy()
    y_drawdown = panel["drawdown_exceed"].to_numpy()

    training_backend, ridge, logit = _build_training_models()
    ridge.fit(X_scaled, y_return)
    logit.fit(X_scaled, y_drawdown)

    pred_return = ridge.predict(X_scaled)
    pred_prob = logit.predict_proba(X_scaled)[:, 1]

    return_lower, return_upper = _return_intervals(y_return, pred_return)
    prob_lower, prob_upper = _prob_intervals(y_drawdown, pred_prob)

    panel_predictions = panel[["symbol", "date"]].copy()
    panel_predictions["pred_forward_return_20d"] = pred_return
    panel_predictions["pred_return_lower"] = return_lower
    panel_predictions["pred_return_upper"] = return_upper
    panel_predictions["pred_drawdown_exceed_prob"] = pred_prob
    panel_predictions["pred_drawdown_prob_lower"] = prob_lower
    panel_predictions["pred_drawdown_prob_upper"] = prob_upper

    metrics = _walk_forward_metrics(panel, feature_cols, y_drawdown, folds, embargo_days)
    calibration_bins, calibration_plot_path = _calibration_plot(y_drawdown, pred_prob, outputs_dir)
    metrics["brier_score_full"] = float(np.mean((y_drawdown - pred_prob) ** 2))

    model_card = _model_card(
        panel,
        feature_cols,
        drawdown_threshold,
        metrics,
        folds,
        embargo_days,
        training_backend,
    )

    return PredictionArtifacts(
        panel=panel,
        predictions=panel_predictions,
        metrics=metrics,
        calibration_bins=calibration_bins,
        calibration_plot_path=calibration_plot_path,
        model_card=model_card,
    )


def latest_predictions(
    panel: pd.DataFrame, artifacts: PredictionArtifacts
) -> pd.DataFrame:
    if panel.empty or artifacts.predictions.empty:
        return pd.DataFrame()
    latest = panel.sort_values("date").groupby("symbol").tail(1)
    merged = latest.merge(artifacts.predictions, on=["symbol", "date"], how="left")
    return merged


def _feature_columns(panel: pd.DataFrame) -> list[str]:
    exclude = {
        "symbol",
        "date",
        "forward_return_20d",
        "forward_max_drawdown_60d",
        "drawdown_exceed",
    }
    return [col for col in panel.columns if col not in exclude]


def _forward_return(close: np.ndarray, idx: int, horizon: int) -> float:
    if idx + horizon >= len(close) or close[idx] <= 0:
        return np.nan
    return float(close[idx + horizon] / close[idx] - 1.0)


def _forward_drawdown(close: np.ndarray, idx: int, horizon: int) -> float:
    if idx + 1 >= len(close):
        return np.nan
    end = min(len(close), idx + horizon + 1)
    window = close[idx:end]
    peak = np.maximum.accumulate(window)
    dd = window / peak - 1.0
    return float(np.nanmin(dd))


def _return_intervals(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    residuals = y_true - y_pred
    lower_q = np.nanquantile(residuals, 0.05)
    upper_q = np.nanquantile(residuals, 0.95)
    return y_pred + lower_q, y_pred + upper_q


def _prob_intervals(y_true: np.ndarray, y_pred: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    errors = np.abs(y_true - y_pred)
    q = np.nanquantile(errors, 0.9)
    lower = np.clip(y_pred - q, 0, 1)
    upper = np.clip(y_pred + q, 0, 1)
    return lower, upper


def _calibration_plot(
    y_true: np.ndarray, y_pred: np.ndarray, outputs_dir: Path
) -> tuple[pd.DataFrame, Path | None]:
    bins = pd.cut(y_pred, bins=np.linspace(0, 1, 11), include_lowest=True)
    calibration = (
        pd.DataFrame({"y_true": y_true, "y_pred": y_pred, "bin": bins})
        .groupby("bin")
        .agg(prob_mean=("y_pred", "mean"), obs_rate=("y_true", "mean"), count=("y_true", "size"))
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(5, 4))
    ax.plot([0, 1], [0, 1], linestyle="--", color="gray")
    ax.plot(calibration["prob_mean"], calibration["obs_rate"], marker="o")
    ax.set_xlabel("Predicted probability")
    ax.set_ylabel("Observed frequency")
    ax.set_title("Calibration (drawdown exceedance)")
    outputs_dir.mkdir(parents=True, exist_ok=True)
    plot_path = outputs_dir / "calibration_plot.png"
    fig.tight_layout()
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    return calibration, plot_path


def _walk_forward_metrics(
    panel: pd.DataFrame,
    feature_cols: list[str],
    y_drawdown: np.ndarray,
    folds: int,
    embargo_days: int,
) -> dict[str, float]:
    panel = panel.sort_values("date")
    dates = pd.to_datetime(panel["date"])
    unique_dates = np.sort(dates.unique())
    if len(unique_dates) < folds + 1:
        return {"brier_score": float("nan")}

    split_indices = np.linspace(0, len(unique_dates) - 1, folds + 1, dtype=int)
    briers: list[float] = []

    for fold in range(folds):
        train_end_idx = split_indices[fold]
        test_end_idx = split_indices[fold + 1]
        train_end_date = unique_dates[train_end_idx]
        test_end_date = unique_dates[test_end_idx]
        embargo_date = train_end_date + np.timedelta64(embargo_days, "D")

        train_mask = dates <= train_end_date
        test_mask = (dates > embargo_date) & (dates <= test_end_date)
        if not np.any(test_mask):
            continue
        X_train = panel.loc[train_mask, feature_cols].to_numpy()
        y_train = y_drawdown[train_mask.to_numpy()]
        X_test = panel.loc[test_mask, feature_cols].to_numpy()
        y_test = y_drawdown[test_mask.to_numpy()]

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        logit = LogisticRegression(max_iter=200)
        logit.fit(X_train_scaled, y_train)
        pred_prob = logit.predict_proba(X_test_scaled)[:, 1]
        brier = float(np.mean((y_test - pred_prob) ** 2))
        briers.append(brier)

    avg_brier = float(np.mean(briers)) if briers else float("nan")
    return {"brier_score": avg_brier}


def _model_card(
    panel: pd.DataFrame,
    feature_cols: list[str],
    drawdown_threshold: float,
    metrics: dict[str, float],
    folds: int,
    embargo_days: int,
    training_backend: str,
) -> str:
    lines = [
        "# Model Card",
        "",
        "## Overview",
        "Leakage-safe panel model for monitoring-only predictive diagnostics.",
        "",
        "## Data",
        f"- Samples: {len(panel)}",
        f"- Unique symbols: {panel['symbol'].nunique()}",
        f"- Feature count: {len(feature_cols)}",
        f"- Training backend: {training_backend}",
        "",
        "## Targets",
        "- forward_return_20d (ridge regression)",
        f"- forward_max_drawdown_60d exceedance (threshold {drawdown_threshold:.2f})",
        "",
        "## Validation",
        f"- Walk-forward folds: {folds}",
        f"- Embargo days: {embargo_days}",
        "",
        "## Calibration",
        f"- Brier score (walk-forward): {metrics.get('brier_score', float('nan')):.4f}",
        f"- Brier score (full fit): {metrics.get('brier_score_full', float('nan')):.4f}",
        "",
        "## Known Failure Modes",
        "- Sparse history or missing price data.",
        "- Regime shifts not represented in training windows.",
        "- Volume-missing universes reduce confidence.",
    ]
    return "\n".join(lines)
