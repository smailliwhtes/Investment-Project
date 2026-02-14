from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from market_app.features_local import compute_features
from market_app.corpus import build_local_corpus_features
from market_app.local_config import ConfigResult
from market_app.manifest_local import build_manifest, hash_file, hash_text, write_manifest
from market_app.ohlcv_local import load_ohlcv, resolve_ohlcv_dir
from market_app.reporting_local import write_report
from market_app.schemas_local import (
    DATA_QUALITY_SCHEMA,
    FEATURES_SCHEMA,
    OHLCV_SCHEMA,
    SCORE_SCHEMA,
    UNIVERSE_SCHEMA,
)
from market_app.scoring_local import apply_gates, score_symbols
from market_app.symbols_local import load_symbols
from market_app.themes_local import classify_theme


OUTPUT_SCHEMAS = {
    "universe.csv": "v1",
    "classified.csv": "v1",
    "features.csv": "v1",
    "eligible.csv": "v1",
    "ineligible.csv": "v1",
    "scored.csv": "v1",
    "data_quality.csv": "v1",
}

UNIVERSE_COLUMNS = ["symbol", "name", "exchange", "asset_type", "is_etf", "is_test_issue"]
CLASSIFIED_COLUMNS = [
    "symbol",
    "name",
    "themes",
    "theme_confidence",
    "theme_evidence",
    "theme_uncertain",
]
ELIGIBLE_COLUMNS = ["symbol", "eligible", "gate_fail_reasons"]
SCORED_COLUMNS = [
    "symbol",
    "monitor_score",
    "total_score",
    "risk_flags",
    "risk_level",
    "themes",
    "theme_confidence",
    "last_date",
    "lag_days",
    "lag_bin",
]
DATA_QUALITY_COLUMNS = [
    "symbol",
    "last_date",
    "as_of_date",
    "lag_days",
    "lag_bin",
    "n_rows",
    "missing_days",
    "zero_volume_fraction",
    "bad_ohlc_count",
    "stale_data",
    "stale",
    "has_volume",
    "volume_missing",
    "missing_data",
    "dq_flags",
]


def run_offline_pipeline(
    config_result: ConfigResult,
    *,
    run_id: str | None,
    logger: logging.Logger,
    cli_args: list[str] | None = None,
) -> Path:
    config = config_result.config
    paths_cfg = config["paths"]
    runs_root = Path(paths_cfg["output_dir"]).resolve()
    runs_root.mkdir(parents=True, exist_ok=True)

    symbol_dir = Path(paths_cfg["symbols_dir"]) if paths_cfg.get("symbols_dir") else Path("")
    raw_ohlcv_dir = Path(paths_cfg["ohlcv_dir"]) if paths_cfg.get("ohlcv_dir") else Path("")
    ohlcv_dir = resolve_ohlcv_dir(raw_ohlcv_dir, logger)
    logger.debug(
        "Offline pipeline paths resolved: symbols_dir=%s, ohlcv_dir=%s, output_root=%s",
        symbol_dir,
        ohlcv_dir,
        runs_root,
    )

    symbol_result = load_symbols(symbol_dir, config, logger)
    resolved_run_id = run_id or resolve_run_id(config_result, symbol_result.source_files)
    output_dir = runs_root / resolved_run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    universe = symbol_result.symbols
    if universe.empty:
        logger.warning("Universe is empty after applying filters.")

    max_symbols = config.get("run", {}).get("max_symbols")
    if max_symbols:
        universe = universe.head(int(max_symbols)).reset_index(drop=True)

    classified_records = []
    feature_records = []
    ohlcv_files = []
    symbol_last_dates: dict[str, str] = {}
    quality_records: list[dict[str, object]] = []
    for _, row in universe.iterrows():
        symbol = row["symbol"]
        name = row.get("name", "")
        theme_result = classify_theme(symbol, name, config)
        classified_records.append(
            {
                "symbol": symbol,
                "name": name,
                "themes": theme_result.themes,
                "theme_confidence": theme_result.theme_confidence,
                "theme_evidence": theme_result.theme_evidence,
                "theme_uncertain": theme_result.theme_uncertain,
            }
        )

        ohlcv = load_ohlcv(symbol, ohlcv_dir)
        if ohlcv.source_path:
            ohlcv_files.append(ohlcv.source_path)
        if not ohlcv.frame.empty:
            OHLCV_SCHEMA.validate(ohlcv.frame)
        quality_records.append(ohlcv.quality)
        feature_result = compute_features(symbol, ohlcv.frame, config)
        feature_records.append(feature_result.features)
        symbol_last_dates[symbol] = feature_result.features.get("last_date", "")

    classified = pd.DataFrame(classified_records)
    features = pd.DataFrame(feature_records).sort_values("symbol", kind="mergesort").reset_index(drop=True)
    features, data_quality = _apply_data_quality(
        features,
        config,
        symbol_last_dates,
        quality_records=quality_records,
    )
    features = _add_feature_zscores(features)
    eligible_result = apply_gates(features, config)
    eligible_df = eligible_result.eligible.copy()
    scored = score_symbols(features, classified, config)
    dq_merge_cols = ["symbol", "last_date", "lag_days", "lag_bin", "stale", "dq_flags"]
    missing_dq = sorted(set(scored["symbol"]) - set(data_quality["symbol"]))
    if missing_dq:
        raise RuntimeError(f"Missing data_quality rows for scored symbols: {missing_dq[:10]}")
    scored = scored.drop(columns=[c for c in dq_merge_cols if c != "symbol" and c in scored.columns], errors="ignore")
    scored = scored.merge(data_quality[dq_merge_cols], on="symbol", how="left")
    eligible_df = eligible_df.merge(
        data_quality[["symbol", "last_date", "lag_days", "lag_bin", "stale", "dq_flags"]],
        on="symbol",
        how="left",
    )
    if eligible_df["dq_flags"].isna().any():
        eligible_df["dq_flags"] = eligible_df["dq_flags"].fillna("MISSING_DQ")

    corpus_features = pd.DataFrame()
    corpus_dir = Path(paths_cfg.get("corpus_dir", "")) if paths_cfg.get("corpus_dir") else None
    if config.get("corpus", {}).get("enabled", True):
        if corpus_dir and corpus_dir.exists():
            corpus_features = build_local_corpus_features(corpus_dir)
            if not corpus_features.empty:
                corpus_features.to_csv(output_dir / "corpus_features.csv", index=False)
        elif config.get("corpus", {}).get("required", False):
            raise RuntimeError(f"Corpus required but missing path: {corpus_dir}")
        else:
            logger.warning("Corpus lane skipped: no local corpus files found.")

    universe_path = output_dir / "universe.csv"
    classified_path = output_dir / "classified.csv"
    features_path = output_dir / "features.csv"
    eligible_path = output_dir / "eligible.csv"
    ineligible_path = output_dir / "ineligible.csv"
    scored_path = output_dir / "scored.csv"
    data_quality_path = output_dir / "data_quality.csv"

    universe.to_csv(universe_path, index=False)
    classified.to_csv(classified_path, index=False)
    features.to_csv(features_path, index=False)
    eligible_df = eligible_df.sort_values("symbol", kind="mergesort").reset_index(drop=True)
    scored = scored.sort_values(["monitor_score", "symbol"], ascending=[False, True], kind="mergesort").reset_index(drop=True)
    eligible_df.to_csv(eligible_path, index=False)
    eligible_df.loc[~eligible_df["eligible"]].to_csv(ineligible_path, index=False)
    scored.to_csv(scored_path, index=False)
    data_quality.to_csv(data_quality_path, index=False)

    UNIVERSE_SCHEMA.validate(universe)
    FEATURES_SCHEMA.validate(features)
    SCORE_SCHEMA.validate(scored)
    DATA_QUALITY_SCHEMA.validate(data_quality)

    _assert_required_columns(universe, UNIVERSE_COLUMNS)
    _assert_required_columns(classified, CLASSIFIED_COLUMNS)
    _assert_required_columns(eligible_df, ELIGIBLE_COLUMNS)
    _assert_required_columns(scored, SCORED_COLUMNS)
    _assert_required_columns(data_quality, DATA_QUALITY_COLUMNS)

    write_report(
        output_dir / "report.md",
        run_id=resolved_run_id,
        config=config,
        universe=universe,
        classified=classified,
        eligible=eligible_df,
        scored=scored,
        data_quality=data_quality,
        corpus_features=corpus_features,
    )

    sample_ohlcv = sorted(set(ohlcv_files))
    manifest = build_manifest(
        run_id=resolved_run_id,
        config=config,
        config_hash=config_result.config_hash,
        git_sha=_resolve_git_sha(output_dir),
        symbol_files=symbol_result.source_files,
        ohlcv_files=sample_ohlcv,
        output_dir=output_dir,
        schema_versions=OUTPUT_SCHEMAS,
        as_of_date=str(data_quality["as_of_date"].iloc[0]) if not data_quality.empty else "",
        cli_args=cli_args or [],
        counts={
            "n_universe": int(len(universe)),
            "n_loaded_ohlcv": int(sum(1 for q in quality_records if int(q.get("n_rows", 0)) > 0)),
            "n_eligible": int(eligible_df["eligible"].sum()),
            "n_scored": int(len(scored)),
        },
    )
    write_manifest(output_dir / "manifest.json", manifest)
    logger.info("Offline pipeline complete at %s", output_dir)
    return output_dir


def resolve_run_id(config_result: ConfigResult, symbol_files: list[Path]) -> str:
    payload = json.dumps(
        {
            "config_hash": config_result.config_hash,
            "symbols": {path.name: hash_file(path) for path in symbol_files},
        },
        sort_keys=True,
    )
    digest = hash_text(payload)[:10]
    return f"run_{digest}"


def _apply_data_quality(
    features: pd.DataFrame,
    config: dict[str, Any],
    symbol_last_dates: dict[str, str],
    quality_records: list[dict[str, object]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    working = features.copy()
    if working.empty:
        empty = pd.DataFrame(columns=DATA_QUALITY_COLUMNS)
        return working, empty

    max_lag_days = int(config.get("gates", {}).get("max_lag_days", 5))
    spy_last_date = symbol_last_dates.get("SPY", "")
    spy_dt = pd.to_datetime(spy_last_date, errors="coerce") if spy_last_date else pd.NaT

    all_last_dates = pd.to_datetime(working.get("last_date", pd.Series(index=working.index)), errors="coerce")
    global_max_dt = all_last_dates.max() if not all_last_dates.isna().all() else pd.NaT
    configured_as_of = pd.to_datetime(config.get("as_of_date"), errors="coerce")
    chosen_as_of_dt = (
        configured_as_of
        if not pd.isna(configured_as_of)
        else (spy_dt if not pd.isna(spy_dt) else global_max_dt)
    )
    as_of_text = "" if pd.isna(chosen_as_of_dt) else chosen_as_of_dt.date().isoformat()

    lag_values = []
    stale_values = []
    normalized_last_dates = []
    for _, row in working.iterrows():
        row_last = pd.to_datetime(row.get("last_date", ""), errors="coerce")
        normalized_last = "" if pd.isna(row_last) else row_last.date().isoformat()
        normalized_last_dates.append(normalized_last)
        if pd.isna(row_last) or pd.isna(chosen_as_of_dt):
            lag_values.append(pd.NA)
            stale_values.append(True)
            continue
        lag = int((chosen_as_of_dt.normalize() - row_last.normalize()).days)
        lag_values.append(lag)
        stale_values.append(lag > max_lag_days)

    working["last_date"] = normalized_last_dates
    working["as_of_date"] = as_of_text
    working["lag_days"] = pd.array(lag_values, dtype="Int64")
    working["stale_data"] = stale_values

    quality_df = pd.DataFrame(quality_records)
    if quality_df.empty:
        quality_df = pd.DataFrame(columns=["symbol", "n_rows", "missing_days", "zero_volume_fraction", "bad_ohlc_count"])
    else:
        quality_df = quality_df.drop(columns=["last_date"], errors="ignore")
    data_quality = working[["symbol", "last_date", "as_of_date", "lag_days", "stale_data", "volume_missing", "missing_data", "history_days"]].merge(
        quality_df,
        on="symbol",
        how="left",
    )
    data_quality["lag_bin"] = data_quality["lag_days"].apply(_lag_bin)
    data_quality["has_volume"] = ~data_quality["volume_missing"].fillna(True)
    data_quality["stale"] = data_quality["stale_data"].fillna(True)
    for col in ["n_rows", "missing_days", "bad_ohlc_count"]:
        data_quality[col] = pd.to_numeric(data_quality[col], errors="coerce").fillna(0).astype(int)
    data_quality["zero_volume_fraction"] = pd.to_numeric(data_quality["zero_volume_fraction"], errors="coerce")
    data_quality["dq_flags"] = data_quality.apply(_build_dq_flags, axis=1)

    working = working.merge(
        data_quality[["symbol", "lag_bin", "n_rows", "missing_days", "zero_volume_fraction", "bad_ohlc_count", "dq_flags", "stale"]],
        on="symbol",
        how="left",
    )
    data_quality = data_quality.sort_values("symbol", kind="mergesort").reset_index(drop=True)
    return working, data_quality


def _lag_bin(value: object) -> str:
    if pd.isna(value):
        return "unknown"
    lag = int(value)
    if lag <= 0:
        return "0"
    if lag <= 3:
        return "1-3"
    if lag <= 7:
        return "4-7"
    if lag <= 14:
        return "8-14"
    return "15+"


def _build_dq_flags(row: pd.Series) -> str:
    flags: list[str] = []
    if int(row.get("n_rows", 0)) <= 0:
        flags.append("NO_ROWS")
    if bool(row.get("volume_missing", False)):
        flags.append("MISSING_VOLUME")
    if int(row.get("history_days", 0)) < 30:
        flags.append("SHORT_HISTORY")
    if int(row.get("bad_ohlc_count", 0)) > 0:
        flags.append("BAD_OHLC")
    if bool(row.get("missing_data", False)):
        flags.append("MISSING_DATA")
    if bool(row.get("stale", False)):
        flags.append("STALE")
    return "|".join(flags)


def _add_feature_zscores(features: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = [
        "return_1m",
        "return_3m",
        "return_6m",
        "return_12m",
        "volatility_20d",
        "volatility_60d",
        "downside_volatility",
        "max_drawdown_6m",
        "adv20_usd",
    ]
    for col in numeric_cols:
        if col not in features.columns:
            continue
        series = pd.to_numeric(features[col], errors="coerce")
        mean = series.mean()
        std = series.std(ddof=0)
        if std == 0 or pd.isna(std):
            z = series * 0.0
        else:
            z = ((series - mean) / std).clip(-3, 3)
        features[f"{col}_z"] = z
    return features


def _assert_required_columns(frame: pd.DataFrame, columns: list[str]) -> None:
    missing = [col for col in columns if col not in frame.columns]
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")


def _resolve_git_sha(base_dir: Path) -> str | None:
    git_dir = None
    for parent in [base_dir, *base_dir.parents]:
        candidate = parent / ".git"
        if candidate.exists():
            git_dir = candidate
            break
    if git_dir is None:
        return None
    head = git_dir / "HEAD"
    if not head.exists():
        return None
    head_content = head.read_text(encoding="utf-8").strip()
    if head_content.startswith("ref:"):
        ref_path = git_dir / head_content.split(" ", 1)[1]
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
    return head_content or None
