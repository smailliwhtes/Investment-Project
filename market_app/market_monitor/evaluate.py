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
from market_monitor.quant_math import (
    annualized_volatility,
    beta,
    binary_log_loss,
    brier_score,
    cvar,
    downside_volatility,
    expected_calibration_error,
    information_ratio,
    max_drawdown_from_returns,
    sharpe_ratio,
    sortino_ratio,
)
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
    max_samples_per_symbol: int,
    logger,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    missing_history_count = 0
    missing_history_warn_limit = 25
    total_symbols = len(symbols)

    for symbol_index, symbol in enumerate(symbols, start=1):
        try:
            history = provider.get_history(symbol, 0)
        except Exception as exc:  # noqa: BLE001
            missing_history_count += 1
            if missing_history_count <= missing_history_warn_limit:
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
        start_idx = min_history_days
        end_idx = len(history) - forward_return_days
        if max_samples_per_symbol > 0:
            start_idx = max(start_idx, end_idx - max_samples_per_symbol)

        for idx in range(start_idx, end_idx):
            window_start = max(0, idx - lookback_days)
            window = history.iloc[window_start : idx + 1]
            features = compute_features(window)
            label = _forward_return(close, idx, forward_return_days)
            rows.append(
                {
                    "symbol": symbol,
                    "Date": dates[idx],
                    "label_end_date": dates[idx + forward_return_days],
                    "forward_return": label,
                    **features,
                }
            )
        if symbol_index % 5 == 0 or symbol_index == total_symbols:
            logger.info("[eval] Processed %s/%s symbols", symbol_index, total_symbols)

    if missing_history_count > missing_history_warn_limit:
        logger.warning(
            "[eval] %s additional symbols skipped due missing history.",
            missing_history_count - missing_history_warn_limit,
        )

    return pd.DataFrame(rows)



def _normalize_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper().lstrip("\ufeff")
    if not symbol:
        return ""
    if "," in symbol:
        symbol = symbol.split(",", 1)[0].strip()
    if symbol in {"SYMBOL", "TICKER"}:
        return ""

    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_")
    if any(char not in allowed for char in symbol):
        return ""

    return symbol


def _sanitize_symbols(values: list[Any]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = _normalize_symbol(value)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        ordered.append(symbol)
    return ordered

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
    exclude = {"symbol", "Date", "forward_return", "label", "label_end_date", "corpus_date"}
    cols = [col for col in panel.columns if col not in exclude]
    if not include_corpus:
        cols = [
            col
            for col in cols
            if not col.startswith("conflict_") and not col.endswith("_sum") and not col.endswith("_mean")
        ]
        cols = [
            col
            for col in cols
            if not col.startswith("mentions_") and not col.startswith("sources_") and not col.startswith("articles_")
        ]
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


def _purge_overlapping_train_rows(train: pd.DataFrame, *, test_start: pd.Timestamp) -> pd.DataFrame:
    if train.empty or "label_end_date" not in train.columns:
        return train
    label_end = pd.to_datetime(train["label_end_date"], errors="coerce")
    return train[label_end < test_start].copy()


def _model_portfolio_returns(test: pd.DataFrame, *, signal: np.ndarray) -> pd.Series:
    if test.empty:
        return pd.Series(dtype=float)

    working = test[["Date", "symbol", "forward_return"]].copy()
    working["signal"] = signal
    working["Date"] = pd.to_datetime(working["Date"], errors="coerce")
    working["forward_return"] = pd.to_numeric(working["forward_return"], errors="coerce")
    working["signal"] = pd.to_numeric(working["signal"], errors="coerce")
    working = working.dropna(subset=["Date", "forward_return", "signal"])
    if working.empty:
        return pd.Series(dtype=float)

    rows: list[dict[str, Any]] = []
    for date, group in working.groupby("Date", sort=True):
        ranked = group.sort_values(["signal", "symbol"], ascending=[False, True]).reset_index(drop=True)
        selected = ranked[ranked["signal"] > 0]
        if selected.empty:
            selected = ranked.head(1)
        else:
            take = max(1, int(np.ceil(len(ranked) * 0.2)))
            selected = selected.head(take)
        rows.append(
            {
                "Date": date,
                "portfolio_return": float(selected["forward_return"].mean()),
            }
        )

    if not rows:
        return pd.Series(dtype=float)
    portfolio = pd.DataFrame(rows).sort_values("Date").set_index("Date")["portfolio_return"]
    return portfolio.astype(float)


def _portfolio_vs_benchmark_metrics(
    test: pd.DataFrame,
    *,
    signal: np.ndarray,
    risk_free_rate_annual: float,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "volatility_ann": float("nan"),
        "downside_vol_ann": float("nan"),
        "cvar_95": float("nan"),
        "max_drawdown": float("nan"),
        "sharpe": float("nan"),
        "sortino": float("nan"),
        "beta_spy": float("nan"),
        "information_ratio": float("nan"),
    }

    if test.empty or "Date" not in test.columns or "symbol" not in test.columns:
        return _normalize_metric_values(metrics)

    portfolio_by_date = _model_portfolio_returns(test, signal=signal)
    if portfolio_by_date.empty:
        return _normalize_metric_values(metrics)
    portfolio_arr = portfolio_by_date.to_numpy(dtype=float)
    metrics["volatility_ann"] = annualized_volatility(portfolio_arr)
    metrics["downside_vol_ann"] = downside_volatility(portfolio_arr)
    metrics["cvar_95"] = cvar(portfolio_arr, alpha=0.95)
    metrics["max_drawdown"] = max_drawdown_from_returns(portfolio_arr)
    metrics["sharpe"] = sharpe_ratio(portfolio_arr, risk_free_rate_annual=risk_free_rate_annual)
    metrics["sortino"] = sortino_ratio(portfolio_arr, risk_free_rate_annual=risk_free_rate_annual)

    benchmark = (
        test[test["symbol"].astype(str).str.upper() == "SPY"]
        .groupby("Date")["forward_return"]
        .mean()
        .sort_index()
    )

    if benchmark.empty:
        return _normalize_metric_values(metrics)

    aligned = pd.concat([portfolio_by_date, benchmark], axis=1, join="inner").dropna()
    if aligned.empty or len(aligned) < 2:
        return _normalize_metric_values(metrics)

    portfolio_arr = aligned.iloc[:, 0].to_numpy(dtype=float)
    benchmark_arr = aligned.iloc[:, 1].to_numpy(dtype=float)

    metrics["beta_spy"] = beta(portfolio_arr, benchmark_arr)
    metrics["information_ratio"] = information_ratio(portfolio_arr, benchmark_arr)
    return _normalize_metric_values(metrics)



def _normalize_metric_values(metrics: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in metrics.items():
        if isinstance(value, float) and np.isnan(value):
            normalized[key] = None
        else:
            normalized[key] = value
    return normalized


def _evaluate_split(
    panel: pd.DataFrame,
    split: EvaluationSplit,
    *,
    include_corpus: bool,
    mode: str,
    risk_free_rate_annual: float,
) -> dict[str, Any]:
    feature_cols = _feature_columns(panel, include_corpus=include_corpus)
    train = panel[panel["Date"] <= split.train_end].dropna(
        subset=["forward_return", "label"] + feature_cols
    ).copy()
    train = _purge_overlapping_train_rows(train, test_start=split.test_start)
    test = panel[(panel["Date"] > split.train_end) & (panel["Date"] <= split.test_end)].dropna(
        subset=["forward_return", "label"] + feature_cols
    )
    if train.empty or test.empty:
        return {}

    X_train = train[feature_cols].to_numpy()
    X_test = test[feature_cols].to_numpy()
    y_train = train["forward_return"].to_numpy(dtype=float)
    y_test = test["forward_return"].to_numpy(dtype=float)
    portfolio_signal: np.ndarray | None = None

    metrics: dict[str, Any] = {
        "split": split.name,
        "train_end": split.train_end.date().isoformat(),
        "test_start": split.test_start.date().isoformat(),
        "test_end": split.test_end.date().isoformat(),
        "volatility_ann": float("nan"),
        "downside_vol_ann": float("nan"),
        "cvar_95": float("nan"),
        "max_drawdown": float("nan"),
        "sharpe": float("nan"),
        "sortino": float("nan"),
        "beta_spy": float("nan"),
        "information_ratio": float("nan"),
        "brier": float("nan"),
        "log_loss": float("nan"),
        "calibration_ece": float("nan"),
    }

    if mode in {"both", "regression"}:
        ridge = Ridge(alpha=1.0)
        ridge.fit(X_train, y_train)
        pred = ridge.predict(X_test)
        portfolio_signal = pred
        mse = float(mean_squared_error(y_test, pred))
        metrics["mse"] = mse
        metrics["rmse"] = float(np.sqrt(mse))
        metrics["mae"] = float(mean_absolute_error(y_test, pred))

    if mode in {"both", "classification"}:
        if train["label"].nunique() < 2:
            metrics["accuracy"] = float("nan")
            metrics["f1"] = float("nan")
        else:
            clf = LogisticRegression(max_iter=200)
            clf.fit(X_train, train["label"].to_numpy())
            prob = clf.predict_proba(X_test)[:, 1]
            if portfolio_signal is None:
                portfolio_signal = prob
            pred_label = (prob >= 0.5).astype(int)
            y_label = test["label"].to_numpy(dtype=float)
            metrics["accuracy"] = float(accuracy_score(y_label, pred_label))
            metrics["f1"] = float(f1_score(y_label, pred_label, zero_division=0))
            metrics["brier"] = brier_score(y_label, prob)
            metrics["log_loss"] = binary_log_loss(y_label, prob)
            metrics["calibration_ece"] = expected_calibration_error(y_label, prob)

    if portfolio_signal is not None:
        metrics.update(
            _portfolio_vs_benchmark_metrics(
                test,
                signal=portfolio_signal,
                risk_free_rate_annual=risk_free_rate_annual,
            )
        )

    return _normalize_metric_values(metrics)


def run_evaluation(
    *,
    config: dict[str, Any],
    provider,
    corpus_paths,
    outputs_dir: Path,
    base_dir: Path,
    logger,
) -> int:
    eval_cfg = config.get("evaluation", {})
    symbols = _sanitize_symbols(list(eval_cfg.get("symbols") or []))

    if not symbols:
        scored_path = outputs_dir.parent / "scored.csv"
        if scored_path.exists():
            scored_df = pd.read_csv(scored_path)
            symbols = _sanitize_symbols(scored_df.get("symbol", pd.Series(dtype=str)).tolist())
            if symbols:
                logger.info("[eval] Using %s symbols from %s", len(symbols), scored_path)

    if not symbols:
        watchlist_path = resolve_path(base_dir, config["paths"]["watchlist_file"])
        symbols = _sanitize_symbols(read_watchlist(watchlist_path)["symbol"].tolist())

    if not symbols:
        logger.error("[eval] No symbols available for evaluation.")
        return 2

    panel = _build_market_panel(
        symbols,
        provider,
        lookback_days=int(eval_cfg.get("lookback_days", 252)),
        forward_return_days=int(eval_cfg.get("forward_return_days", 5)),
        min_history_days=int(eval_cfg.get("min_history_days", 252)),
        max_samples_per_symbol=int(eval_cfg.get("max_samples_per_symbol", 750)),
        logger=logger,
    )
    if panel.empty:
        logger.error("[eval] Market panel is empty; evaluation aborted.")
        return 2

    threshold = float(eval_cfg.get("classification_threshold", 0.02))
    risk_free_rate_annual = float(eval_cfg.get("risk_free_rate_annual", 0.0))
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

        metrics_a = _evaluate_split(
            panel,
            split,
            include_corpus=False,
            mode=mode,
            risk_free_rate_annual=risk_free_rate_annual,
        )
        if metrics_a:
            metrics_a["model"] = "market_only"
            metrics_rows.append(metrics_a)

        if corpus_features is not None and not corpus_features.empty:
            metrics_b = _evaluate_split(
                panel,
                split,
                include_corpus=True,
                mode=mode,
                risk_free_rate_annual=risk_free_rate_annual,
            )
            if metrics_b:
                metrics_b["model"] = "market_plus_corpus"
                metrics_rows.append(metrics_b)

    metrics_df = pd.DataFrame(metrics_rows)
    metrics_path = outputs_dir / "eval_metrics.csv"
    metrics_df.to_csv(metrics_path, index=False, lineterminator="\n")

    report_lines = [
        "# Offline Evaluation Report",
        "",
        f"- Symbols evaluated: {len(set(panel['symbol']))}",
        f"- Splits: {len(splits)}",
        f"- Classification threshold: {threshold:.2%}",
        f"- Risk-free annual rate: {risk_free_rate_annual:.2%}",
        "- Portfolio risk metrics use a model-conditioned equal-weight portfolio built from predicted signal ranks.",
        "",
    ]

    if metrics_df.empty:
        report_lines.append("No evaluation metrics generated.")
    else:
        preferred_cols = [
            "mse",
            "rmse",
            "mae",
            "accuracy",
            "f1",
            "brier",
            "log_loss",
            "calibration_ece",
            "volatility_ann",
            "downside_vol_ann",
            "cvar_95",
            "max_drawdown",
            "sharpe",
            "sortino",
            "beta_spy",
            "information_ratio",
        ]
        metric_cols = [col for col in preferred_cols if col in metrics_df.columns]
        summary = metrics_df.groupby("model")[metric_cols].mean(numeric_only=True)
        report_lines.append("## Average Metrics")
        report_lines.append("")
        report_lines.append(summary.to_markdown())

    report_path = outputs_dir / "eval_report.md"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    logger.info(f"[eval] metrics written to {metrics_path}")
    logger.info(f"[eval] report written to {report_path}")
    return 0


