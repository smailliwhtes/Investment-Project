from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from market_app.features_local import compute_features
from market_app.local_config import ConfigResult
from market_app.manifest_local import build_manifest, hash_file, hash_text, write_manifest
from market_app.ohlcv_local import load_ohlcv, resolve_ohlcv_dir
from market_app.reporting_local import write_report
from market_app.scoring_local import apply_gates, score_symbols
from market_app.symbols_local import load_symbols
from market_app.themes_local import classify_theme


OUTPUT_SCHEMAS = {
    "universe.csv": "v1",
    "classified.csv": "v1",
    "features.csv": "v1",
    "eligible.csv": "v1",
    "scored.csv": "v1",
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
]


def run_offline_pipeline(
    config_result: ConfigResult,
    *,
    run_id: str | None,
    logger: logging.Logger,
) -> Path:
    config = config_result.config
    paths_cfg = config["paths"]
    runs_root = Path(paths_cfg["output_dir"]).resolve()
    runs_root.mkdir(parents=True, exist_ok=True)

    symbol_dir = Path(paths_cfg["symbols_dir"]) if paths_cfg.get("symbols_dir") else Path("")
    raw_ohlcv_dir = Path(paths_cfg["ohlcv_dir"]) if paths_cfg.get("ohlcv_dir") else Path("")
    ohlcv_dir = resolve_ohlcv_dir(raw_ohlcv_dir, logger)

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
        feature_result = compute_features(symbol, ohlcv.frame, config)
        feature_records.append(feature_result.features)

    classified = pd.DataFrame(classified_records)
    features = pd.DataFrame(feature_records)
    features = _add_feature_zscores(features)
    eligible_result = apply_gates(features, config)
    scored = score_symbols(features, classified, config)


    universe_path = output_dir / "universe.csv"
    classified_path = output_dir / "classified.csv"
    features_path = output_dir / "features.csv"
    eligible_path = output_dir / "eligible.csv"
    scored_path = output_dir / "scored.csv"

    universe.to_csv(universe_path, index=False)
    classified.to_csv(classified_path, index=False)
    features.to_csv(features_path, index=False)
    eligible_result.eligible.to_csv(eligible_path, index=False)
    scored.to_csv(scored_path, index=False)

    _assert_required_columns(universe, UNIVERSE_COLUMNS)
    _assert_required_columns(classified, CLASSIFIED_COLUMNS)
    _assert_required_columns(eligible_result.eligible, ELIGIBLE_COLUMNS)
    _assert_required_columns(scored, SCORED_COLUMNS)

    write_report(
        output_dir / "report.md",
        run_id=resolved_run_id,
        config=config,
        universe=universe,
        classified=classified,
        eligible=eligible_result.eligible,
        scored=scored,
        data_quality=features,
    )

    sample_ohlcv = sorted(set(ohlcv_files))[:5]
    manifest = build_manifest(
        run_id=resolved_run_id,
        config=config,
        config_hash=config_result.config_hash,
        git_sha=_resolve_git_sha(output_dir),
        symbol_files=symbol_result.source_files,
        ohlcv_files=sample_ohlcv,
        output_dir=output_dir,
        schema_versions=OUTPUT_SCHEMAS,
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
