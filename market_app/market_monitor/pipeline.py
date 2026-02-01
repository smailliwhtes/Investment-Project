from __future__ import annotations

from dataclasses import dataclass
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from market_monitor.corpus import run_corpus_pipeline
from market_monitor.data_paths import resolve_corpus_paths, resolve_data_paths
from market_monitor.hash_utils import hash_manifest
from market_monitor.io import build_feature_columns, write_csv
from market_monitor.logging_utils import JsonlLogger, get_console_logger
from market_monitor.macro import load_silver_series
from market_monitor.manifest import build_run_manifest, resolve_git_commit, run_id_from_inputs
from market_monitor.offline import set_offline_mode
from market_monitor.preflight import run_preflight
from market_monitor.provider_factory import build_provider
from market_monitor.report import write_report
from market_monitor.scoring import score_frame
from market_monitor.staging import stage_pipeline
from market_monitor.universe import fetch_universe, filter_universe, read_watchlist, write_universe_csv


@dataclass(frozen=True)
class PipelineResult:
    run_id: str
    run_timestamp: str
    run_start: datetime
    run_end: datetime
    universe_df: pd.DataFrame
    scored_df: pd.DataFrame
    eligible_df: pd.DataFrame
    summary: dict[str, int]
    provider: Any
    output_dir: Path
    config_hash: str


OUTPUT_ELIGIBLE_COLUMNS = [
    "symbol",
    "eligible",
    "gate_fail_reasons",
    "theme_bucket",
    "asset_type",
]

OUTPUT_SCORED_COLUMNS = [
    "symbol",
    "score_1to10",
    "risk_flags",
    "explanation",
    "theme_bucket",
    "asset_type",
    "ml_signal",
    "ml_model_id",
    "ml_featureset_id",
]


def _series_or_empty(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series("", index=df.index)


def _normalize_pipe_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.replace(";", "|", regex=False)


def _combine_pipe_series(*series_list: pd.Series) -> pd.Series:
    if not series_list:
        return pd.Series(dtype=str)
    combined = pd.Series("", index=series_list[0].index)
    for series in series_list:
        cleaned = _normalize_pipe_series(series)
        combined = combined.mask((combined == "") & (cleaned != ""), cleaned)
        combined = combined.mask((combined != "") & (cleaned != ""), combined + "|" + cleaned)
    return combined


def _attach_watchlist_metadata(df: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.assign(
            theme_bucket=pd.Series(dtype=object),
            asset_type=pd.Series(dtype=object),
        )
    meta = universe_df[["symbol", "theme_bucket", "asset_type"]].drop_duplicates()
    return df.merge(meta, on="symbol", how="left")


def _assert_columns_match(df: pd.DataFrame, expected: list[str], label: str) -> None:
    actual = list(df.columns)
    if actual != expected:
        raise RuntimeError(
            f"{label} schema mismatch. Expected columns {expected}, got {actual}."
        )


def _assert_bool_like(series: pd.Series, label: str) -> None:
    if series.empty:
        return
    if pd.api.types.is_bool_dtype(series):
        return
    allowed = {True, False, 0, 1, "0", "1", "true", "false", "True", "False"}
    invalid = series[~series.map(lambda value: value in allowed)]
    if not invalid.empty:
        sample = invalid.head(3).tolist()
        raise RuntimeError(f"{label} must be boolean-like. Invalid values: {sample}")


def _assert_score_range(series: pd.Series) -> None:
    if series.empty:
        return
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        raise RuntimeError("score_1to10 must be numeric with no missing values.")
    if not ((numeric % 1 == 0) & numeric.between(1, 10)).all():
        sample = numeric[~((numeric % 1 == 0) & numeric.between(1, 10))].head(3).tolist()
        raise RuntimeError(f"score_1to10 must be int 1-10. Invalid values: {sample}")


def _assert_pipe_delimited(series: pd.Series, label: str) -> None:
    if series.empty:
        return
    cleaned = series.fillna("").astype(str)
    if cleaned.str.contains(";", regex=False).any():
        raise RuntimeError(f"{label} must be pipe-delimited strings (use '|').")


def _build_contract_eligible(scored: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    if scored.empty:
        output = pd.DataFrame(columns=OUTPUT_ELIGIBLE_COLUMNS)
        _assert_columns_match(output, OUTPUT_ELIGIBLE_COLUMNS, "eligible.csv")
        return output
    merged = _attach_watchlist_metadata(scored, universe_df)
    eligible = _series_or_empty(merged, "eligible").fillna(False).astype(bool)
    gate_fail = _normalize_pipe_series(_series_or_empty(merged, "gate_fail_codes"))
    data_reason = _normalize_pipe_series(_series_or_empty(merged, "data_reason_codes"))
    gate_fail = gate_fail.mask((gate_fail == "") & (data_reason != ""), data_reason)
    gate_fail = gate_fail.mask(eligible, "")
    output = pd.DataFrame(
        {
            "symbol": merged["symbol"],
            "eligible": eligible,
            "gate_fail_reasons": gate_fail,
            "theme_bucket": _series_or_empty(merged, "theme_bucket").fillna(""),
            "asset_type": _series_or_empty(merged, "asset_type").fillna(""),
        }
    )
    _assert_columns_match(output, OUTPUT_ELIGIBLE_COLUMNS, "eligible.csv")
    _assert_bool_like(output["eligible"], "eligible.csv eligible")
    _assert_pipe_delimited(output["gate_fail_reasons"], "eligible.csv gate_fail_reasons")
    return output


def _build_contract_scored(scored: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    if scored.empty:
        output = pd.DataFrame(columns=OUTPUT_SCORED_COLUMNS)
        _assert_columns_match(output, OUTPUT_SCORED_COLUMNS, "scored.csv")
        return output
    merged = _attach_watchlist_metadata(scored, universe_df)
    if "monitor_score_1_10" not in merged.columns:
        raise RuntimeError("scored.csv requires monitor_score_1_10 to build score_1to10.")
    score_values = pd.to_numeric(merged["monitor_score_1_10"], errors="coerce")
    _assert_score_range(score_values)
    explanation = _series_or_empty(merged, "notes").fillna("")
    explanation = explanation.mask(explanation == "", _series_or_empty(merged, "data_status").fillna(""))
    risk_flags = _combine_pipe_series(
        _series_or_empty(merged, "risk_red_codes"),
        _series_or_empty(merged, "risk_amber_codes"),
    )
    output = pd.DataFrame(
        {
            "symbol": merged["symbol"],
            "score_1to10": score_values.astype(int),
            "risk_flags": risk_flags,
            "explanation": explanation.astype(str),
            "theme_bucket": _series_or_empty(merged, "theme_bucket").fillna(""),
            "asset_type": _series_or_empty(merged, "asset_type").fillna(""),
            "ml_signal": pd.to_numeric(_series_or_empty(merged, "ml_signal"), errors="coerce"),
            "ml_model_id": _series_or_empty(merged, "ml_model_id"),
            "ml_featureset_id": _series_or_empty(merged, "ml_featureset_id"),
        }
    )
    _assert_columns_match(output, OUTPUT_SCORED_COLUMNS, "scored.csv")
    _assert_pipe_delimited(output["risk_flags"], "scored.csv risk_flags")
    return output


def _attach_ml_predictions(scored: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    ml_columns = ["ml_signal", "ml_model_id", "ml_featureset_id"]
    if scored.empty:
        return scored.assign(**{col: pd.Series(dtype=object) for col in ml_columns})

    predictions_path = output_dir / "ml" / "predictions_latest.csv"
    if not predictions_path.exists():
        return scored.assign(**{col: None for col in ml_columns})

    predictions = pd.read_csv(predictions_path)
    if "symbol" not in predictions.columns or "yhat" not in predictions.columns:
        raise RuntimeError("ml/predictions_latest.csv missing required columns: symbol, yhat")
    predictions = predictions.rename(columns={"yhat": "ml_signal"})
    predictions = predictions[["symbol", "ml_signal"]]

    model_id = None
    featureset_id = None
    manifest_path = output_dir / "ml" / "predict_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        model_id = manifest.get("model_id")
        featureset_id = manifest.get("featureset_id")

    merged = scored.merge(predictions, on="symbol", how="left")
    merged["ml_model_id"] = model_id
    merged["ml_featureset_id"] = featureset_id
    return merged


def run_pipeline(
    config: dict[str, Any],
    *,
    base_dir: Path,
    mode: str,
    watchlist_path: Path | None,
    output_dir: Path,
    run_id: str | None,
    logger=None,
    write_legacy_outputs: bool = True,
    run_timestamp: str | None = None,
) -> PipelineResult:
    logger = logger or get_console_logger("INFO")
    offline_mode = bool(config["data"].get("offline_mode", False))
    if not offline_mode:
        logger.error("Offline mode is required for this release. Set data.offline_mode=true.")
        raise RuntimeError("Offline mode is required for this release.")
    set_offline_mode(offline_mode)

    run_start = datetime.now(timezone.utc)
    run_timestamp = run_timestamp or run_start.isoformat()

    provider = build_provider(config, logger, base_dir)

    if offline_mode and mode != "watchlist":
        logger.error("Offline mode is enabled; only watchlist mode is supported.")
        raise RuntimeError("Offline mode only supports watchlist mode.")

    if mode == "watchlist":
        if watchlist_path is None:
            watchlist_path = base_dir / config["paths"]["watchlist_file"]
        universe_df = read_watchlist(watchlist_path)
        if universe_df.empty:
            raise RuntimeError(f"Watchlist is empty or missing at {watchlist_path}.")
    else:
        universe_df = fetch_universe()
        write_universe_csv(universe_df, base_dir / config["paths"]["universe_csv"])

    universe_df = filter_universe(
        universe_df,
        config["universe"]["allowed_security_types"],
        config["universe"]["allowed_currencies"],
        config["universe"]["include_etfs"],
    )

    corpus_paths = resolve_corpus_paths(config, base_dir)
    corpus_sources = []
    if corpus_paths.gdelt_conflict_dir or corpus_paths.gdelt_events_raw_dir:
        corpus_sources = [
            path for path in [corpus_paths.gdelt_conflict_dir, corpus_paths.gdelt_events_raw_dir] if path
        ]
    corpus_manifest_hash = None
    if corpus_sources:
        corpus_manifest_hash = hash_manifest({"files": [str(path) for path in corpus_sources]})

    config_hash = config.get("config_hash") or "unknown"
    if run_id is None:
        run_id = run_id_from_inputs(
            timestamp=run_start,
            config_hash=config_hash,
            watchlist_path=watchlist_path if mode == "watchlist" else None,
            watchlist_df=universe_df,
            corpus_manifest_hash=corpus_manifest_hash,
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = base_dir / config["paths"]["cache_dir"]
    cache_dir.mkdir(parents=True, exist_ok=True)
    logs_dir = base_dir / config["paths"]["logs_dir"]
    logs_dir.mkdir(parents=True, exist_ok=True)

    json_logger = JsonlLogger(logs_dir / f"run_{run_id}.jsonl")
    run_meta = {
        "run_id": run_id,
        "run_timestamp_utc": run_timestamp,
        "config_hash": config_hash,
        "provider_name": provider.name,
    }

    paths = resolve_data_paths(config, base_dir)
    silver_macro = None
    if paths.silver_prices_dir:
        macro = load_silver_series(paths.silver_prices_dir)
        if macro:
            silver_macro = macro.features

    preflight = None
    if mode == "watchlist":
        preflight = run_preflight(
            universe_df,
            provider,
            output_dir,
            run_id=run_id,
            run_timestamp=run_timestamp,
            logger=logger,
            corpus_dir=corpus_paths.gdelt_conflict_dir,
            raw_events_dir=corpus_paths.gdelt_events_raw_dir,
        )

    stage1_df, stage2_df, stage3_df, summary = stage_pipeline(
        universe_df,
        provider,
        cache_dir,
        config["data"]["max_cache_age_days"],
        config,
        run_meta,
        logger,
        silver_macro=silver_macro,
    )

    scored = score_frame(stage3_df, config["score"]["weights"]) if not stage3_df.empty else stage3_df

    corpus_outputs_dir = output_dir / "corpus"
    corpus_run = run_corpus_pipeline(
        corpus_dir=corpus_paths.gdelt_conflict_dir,
        raw_events_dir=corpus_paths.gdelt_events_raw_dir,
        outputs_dir=corpus_outputs_dir,
        config=config,
        provider=provider,
        watchlist=universe_df["symbol"].tolist(),
        logger=logger,
    )
    context_columns = []
    if corpus_run.daily_features is not None and not stage3_df.empty:
        context = corpus_run.daily_features.copy()
        context = context.rename(columns={col: f"context_{col}" for col in context.columns if col != "Date"})
        context_columns = [col for col in context.columns if col != "Date"]
        scored = scored.merge(
            context,
            left_on="as_of_date",
            right_on="Date",
            how="left",
        ).drop(columns=["Date"])

    scored = _attach_ml_predictions(scored, output_dir)

    eligible = (
        scored[["symbol", "name", "eligible", "gate_fail_codes", "notes"]]
        if not scored.empty
        else pd.DataFrame(columns=["symbol", "name", "eligible", "gate_fail_codes", "notes"])
    )

    if write_legacy_outputs:
        features_path = output_dir / f"features_{run_id}.csv"
        scored_path = output_dir / f"scored_{run_id}.csv"
        eligible_path = output_dir / f"eligible_{run_id}.csv"
        report_path = output_dir / "run_report.md"
        report_archive = output_dir / f"run_report_{run_id}.md"

        scored_contract = _build_contract_scored(scored, universe_df)
        eligible_contract = _build_contract_eligible(scored, universe_df)

        write_csv(scored_contract, scored_path, OUTPUT_SCORED_COLUMNS)
        write_csv(scored, features_path, build_feature_columns(context_columns))
        write_csv(eligible_contract, eligible_path, OUTPUT_ELIGIBLE_COLUMNS)

        data_usage = {
            "offline_mode": str(config["data"].get("offline_mode", False)),
            "provider_name": provider.name,
            "nasdaq_daily_dir": str(paths.nasdaq_daily_dir) if paths.nasdaq_daily_dir else "unset",
            "nasdaq_daily_found": str(bool(paths.nasdaq_daily_dir and paths.nasdaq_daily_dir.exists())),
            "silver_prices_dir": str(paths.silver_prices_dir) if paths.silver_prices_dir else "unset",
            "silver_prices_found": str(bool(paths.silver_prices_dir and paths.silver_prices_dir.exists())),
            "gdelt_conflict_dir": str(corpus_paths.gdelt_conflict_dir)
            if corpus_paths.gdelt_conflict_dir
            else "unset",
            "gdelt_conflict_found": str(
                bool(corpus_paths.gdelt_conflict_dir and corpus_paths.gdelt_conflict_dir.exists())
            ),
            "gdelt_events_raw_dir": str(corpus_paths.gdelt_events_raw_dir)
            if corpus_paths.gdelt_events_raw_dir
            else "unset",
            "gdelt_events_raw_found": str(
                bool(corpus_paths.gdelt_events_raw_dir and corpus_paths.gdelt_events_raw_dir.exists())
            ),
        }

        write_report(
            report_path,
            summary,
            scored,
            run_id=run_id,
            run_timestamp=run_timestamp,
            data_usage=data_usage,
            prediction_panel=None,
            prediction_metrics=None,
            context_summary=None,
        )
        write_report(
            report_archive,
            summary,
            scored,
            run_id=run_id,
            run_timestamp=run_timestamp,
            data_usage=data_usage,
            prediction_panel=None,
            prediction_metrics=None,
            context_summary=None,
        )

        run_end = datetime.now(timezone.utc)
        manifest = build_run_manifest(
            run_id=run_id,
            run_start=run_start,
            run_end=run_end,
            config=config,
            config_hash=config_hash,
            watchlist_path=watchlist_path if mode == "watchlist" else None,
            watchlist_df=universe_df,
            summary=summary,
            scored=scored,
            preflight=preflight,
            git_commit=resolve_git_commit(base_dir),
            corpus_manifest=corpus_run.manifest if corpus_run else None,
        )
        (output_dir / "run_manifest.json").write_text(
            manifest.to_json(indent=2),
            encoding="utf-8",
        )
        json_logger.log("summary", {"counts": summary})

    run_end = datetime.now(timezone.utc)
    logger.info(f"Outputs written to {output_dir}")
    return PipelineResult(
        run_id=run_id,
        run_timestamp=run_timestamp,
        run_start=run_start,
        run_end=run_end,
        universe_df=universe_df,
        scored_df=scored,
        eligible_df=eligible,
        summary=summary,
        provider=provider,
        output_dir=output_dir,
        config_hash=config_hash,
    )
