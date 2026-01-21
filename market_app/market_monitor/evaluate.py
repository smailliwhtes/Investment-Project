from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import accuracy_score, f1_score, mean_absolute_error, mean_squared_error

from market_monitor.features import compute_features
from market_monitor.paths import resolve_path
from market_monitor.universe import read_watchlist


@dataclass(frozen=True)
class EvaluationSplit:
    name: str
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def _forward_return(close: np.ndarray, idx: int, horizon: int) -> float:
    if idx + horizon >= len(close) or close[idx] <= 0:
        return np.nan
    return float(close[idx + horizon] / close[idx] - 1.0)


def _build_market_panel(
    symbols: list[str],
    provider,
    *,
    lookback_days: int,
    forward_return_days: int,
    min_history_days: int,
    logger,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            history = provider.get_history(symbol, 0)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[eval] {symbol} history unavailable: {exc}")
            continue
        if history.empty:
            continue
        history = history.copy()
        history["Date"] = pd.to_datetime(history["Date"], errors="coerce")
        history = history.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
        if len(history) < min_history_days:
            continue

        close = history["Close"].to_numpy(dtype=float)
        dates = history["Date"].to_numpy()
        for idx in range(min_history_days, len(history) - forward_return_days):
            window_start = max(0, idx - lookback_days)
            window = history.iloc[window_start : idx + 1]
            features = compute_features(window)
            label = _forward_return(close, idx, forward_return_days)
            rows.append(
                {
                    "symbol": symbol,
                    "Date": dates[idx],
                    "forward_return": label,
                    **features,
                }
            )
    return pd.DataFrame(rows)


def _join_corpus(panel: pd.DataFrame, corpus_features: pd.DataFrame | None) -> pd.DataFrame:
    if panel.empty or corpus_features is None or corpus_features.empty:
        panel = panel.copy()
        panel["corpus_date"] = pd.NaT
        return panel
    corpus = corpus_features.copy()
    corpus["Date"] = pd.to_datetime(corpus["Date"], errors="coerce")
    corpus["corpus_date"] = corpus["Date"]
    merged = panel.merge(corpus, on="Date", how="left")
    return merged


def enforce_no_lookahead(panel: pd.DataFrame, *, cutoff: pd.Timestamp) -> None:
    if panel.empty:
        return
    if "corpus_date" in panel.columns:
        violations = panel[(panel["corpus_date"].notna()) & (panel["corpus_date"] > panel["Date"])]
        if not violations.empty:
            raise ValueError("Future leakage detected: corpus_date exceeds sample Date.")
        max_corpus = panel["corpus_date"].dropna().max()
        if pd.notna(max_corpus) and max_corpus > cutoff:
            raise ValueError("Future leakage detected: corpus data beyond cutoff used in training.")


def _feature_columns(panel: pd.DataFrame, *, include_corpus: bool) -> list[str]:
    exclude = {"symbol", "Date", "forward_return", "label", "corpus_date"}
    cols = [col for col in panel.columns if col not in exclude]
    if not include_corpus:
        cols = [col for col in cols if not col.startswith("conflict_") and not col.endswith("_sum") and not col.endswith("_mean")]
        cols = [col for col in cols if not col.startswith("mentions_") and not col.startswith("sources_") and not col.startswith("articles_")]
        cols = [col for col in cols if not col.startswith("goldstein") and not col.startswith("tone")]
        cols = [col for col in cols if not col.startswith("conflict_event_count")]
    return cols


def build_walk_forward_splits(dates: pd.Series, folds: int) -> list[EvaluationSplit]:
    unique_dates = pd.Series(sorted(pd.to_datetime(dates.unique()))).dropna()
    if unique_dates.empty or folds <= 0:
        return []
    cut_indices = np.linspace(0, len(unique_dates) - 1, folds + 1, dtype=int)
    splits: list[EvaluationSplit] = []
    for idx in range(folds):
        train_end = unique_dates.iloc[cut_indices[idx]]
        test_end = unique_dates.iloc[cut_indices[idx + 1]]
        test_start = unique_dates.iloc[cut_indices[idx] + 1] if cut_indices[idx] + 1 < len(unique_dates) else test_end
        splits.append(
            EvaluationSplit(
                name=f"fold_{idx + 1}",
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
    return splits


def _evaluate_split(
    panel: pd.DataFrame,
    split: EvaluationSplit,
    *,
    include_corpus: bool,
    mode: str,
) -> dict[str, Any]:
    feature_cols = _feature_columns(panel, include_corpus=include_corpus)
    train = panel[panel["Date"] <= split.train_end].dropna(
        subset=["forward_return", "label"] + feature_cols
    ).copy()
    test = panel[(panel["Date"] > split.train_end) & (panel["Date"] <= split.test_end)].dropna(
        subset=["forward_return", "label"] + feature_cols
    )
    if train.empty or test.empty:
        return {}

    X_train = train[feature_cols].to_numpy()
    X_test = test[feature_cols].to_numpy()
    y_train = train["forward_return"].to_numpy()
    y_test = test["forward_return"].to_numpy()

    metrics: dict[str, Any] = {
        "split": split.name,
        "train_end": split.train_end.date().isoformat(),
        "test_start": split.test_start.date().isoformat(),
        "test_end": split.test_end.date().isoformat(),
    }
    if mode in {"both", "regression"}:
        ridge = Ridge(alpha=1.0)
        ridge.fit(X_train, y_train)
        pred = ridge.predict(X_test)
        metrics["mse"] = float(mean_squared_error(y_test, pred))
        metrics["mae"] = float(mean_absolute_error(y_test, pred))
    if mode in {"both", "classification"}:
        if train["label"].nunique() < 2:
            metrics["accuracy"] = float("nan")
            metrics["f1"] = float("nan")
        else:
            clf = LogisticRegression(max_iter=200)
            clf.fit(X_train, train["label"].to_numpy())
            prob = clf.predict_proba(X_test)[:, 1]
            pred_label = (prob >= 0.5).astype(int)
            metrics["accuracy"] = float(accuracy_score(test["label"], pred_label))
            metrics["f1"] = float(f1_score(test["label"], pred_label, zero_division=0))
    return metrics


def run_evaluation(
    *,
    config: dict[str, Any],
    provider,
    corpus_paths,
    outputs_dir: Path,
    logger,
) -> int:
    eval_cfg = config.get("evaluation", {})
    symbols = eval_cfg.get("symbols") or []
    if not symbols:
        repo_root = outputs_dir.parent.parent
        watchlist_path = resolve_path(repo_root, config["paths"]["watchlist_file"])
        symbols = read_watchlist(watchlist_path)["symbol"].tolist()
    if not symbols:
        logger.error("[eval] No symbols available for evaluation.")
        return 2

    panel = _build_market_panel(
        symbols,
        provider,
        lookback_days=int(eval_cfg.get("lookback_days", 252)),
        forward_return_days=int(eval_cfg.get("forward_return_days", 5)),
        min_history_days=int(eval_cfg.get("min_history_days", 252)),
        logger=logger,
    )
    if panel.empty:
        logger.error("[eval] Market panel is empty; evaluation aborted.")
        return 2

    threshold = float(eval_cfg.get("classification_threshold", 0.02))
    panel["label"] = (panel["forward_return"] >= threshold).astype(int)
    corpus_features = None
    corpus_dir = outputs_dir.parent / "corpus"
    daily_features_path = corpus_dir / "daily_features.csv"
    if daily_features_path.exists():
        corpus_features = pd.read_csv(daily_features_path)
    else:
        logger.warning("[eval] daily_features.csv not found; corpus model will be skipped.")

    panel = _join_corpus(panel, corpus_features)
    splits = build_walk_forward_splits(panel["Date"], int(eval_cfg.get("walk_forward_folds", 3)))
    if not splits:
        logger.error("[eval] Unable to build walk-forward splits.")
        return 2

    metrics_rows: list[dict[str, Any]] = []
    mode = str(eval_cfg.get("mode", "both")).lower()
    for split in splits:
        enforce_no_lookahead(panel[panel["Date"] <= split.train_end], cutoff=split.train_end)
        metrics_a = _evaluate_split(panel, split, include_corpus=False, mode=mode)
        if metrics_a:
            metrics_a["model"] = "market_only"
            metrics_rows.append(metrics_a)
        if corpus_features is not None and not corpus_features.empty:
            metrics_b = _evaluate_split(panel, split, include_corpus=True, mode=mode)
            if metrics_b:
                metrics_b["model"] = "market_plus_corpus"
                metrics_rows.append(metrics_b)

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_path = outputs_dir / "eval_metrics.csv"
    metrics_df.to_csv(metrics_path, index=False)

    report_lines = [
        "# Offline Evaluation Report",
        "",
        f"- Symbols evaluated: {len(set(panel['symbol']))}",
        f"- Splits: {len(splits)}",
        f"- Classification threshold: {threshold:.2%}",
        "",
    ]
    if metrics_df.empty:
        report_lines.append("No evaluation metrics generated.")
    else:
        metric_cols = [col for col in ["mse", "mae", "accuracy", "f1"] if col in metrics_df.columns]
        summary = metrics_df.groupby("model")[metric_cols].mean(numeric_only=True)
        report_lines.append("## Average Metrics")
        report_lines.append("")
        report_lines.append(summary.to_markdown())
    report_path = outputs_dir / "eval_report.md"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    logger.info(f"[eval] metrics written to {metrics_path}")
    logger.info(f"[eval] report written to {report_path}")
    return 0
