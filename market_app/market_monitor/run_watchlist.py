from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from market_monitor.config_schema import load_config
from market_monitor.features.compute_daily_features import compute_daily_features
from market_monitor.features.io import read_ohlcv
from market_monitor.hash_utils import hash_file, hash_text
from market_monitor.logging_utils import LogPaths, configure_logging
from market_monitor.manifest import resolve_git_commit
from market_monitor.ohlcv_doctor import normalize_directory
from market_monitor.regime import compute_regime
from market_monitor.scoring.explain import build_explanations
from market_monitor.scoring.gates import GateConfig, apply_gates, join_pipe
from market_monitor.scoring.score import ScoreConfig, compute_score
from market_monitor.validation import validate_data

REQUIRED_WATCHLIST_COLUMNS = {"symbol", "theme_bucket", "asset_type"}
OUTPUT_SCHEMA_VERSION = "v2"


@dataclass(frozen=True)
class ResolvedPaths:
    ohlcv_raw_dir: Path
    ohlcv_daily_dir: Path
    exogenous_daily_dir: Path
    outputs_dir: Path


def _resolve_paths(config: dict, base_dir: Path, overrides: dict[str, str | None]) -> ResolvedPaths:
    paths_cfg = config.get("paths", {})
    ohlcv_raw = overrides.get("ohlcv_raw_dir") or paths_cfg.get("ohlcv_raw_dir") or "data/ohlcv_raw"
    ohlcv_daily = overrides.get("ohlcv_daily_dir") or paths_cfg.get("ohlcv_daily_dir") or "data/ohlcv_daily"
    exogenous_daily = overrides.get("exogenous_daily_dir") or paths_cfg.get("exogenous_daily_dir") or "data/exogenous/daily_features"
    outputs_dir = overrides.get("outputs_dir") or paths_cfg.get("outputs_dir") or "outputs"

    def _abs(path_str: str) -> Path:
        path = Path(path_str).expanduser()
        return path if path.is_absolute() else (base_dir / path).resolve()

    return ResolvedPaths(
        ohlcv_raw_dir=_abs(ohlcv_raw),
        ohlcv_daily_dir=_abs(ohlcv_daily),
        exogenous_daily_dir=_abs(exogenous_daily),
        outputs_dir=_abs(outputs_dir),
    )


def _load_watchlist(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = REQUIRED_WATCHLIST_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Watchlist missing required columns: {sorted(missing)}")
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["theme_bucket"] = df["theme_bucket"].fillna("").astype(str)
    df["asset_type"] = df["asset_type"].fillna("").astype(str)
    df = df[df["symbol"] != ""].drop_duplicates(subset=["symbol"], keep="first")
    return df


def _latest_common_date(symbols: list[str], ohlcv_dir: Path) -> str:
    max_dates = []
    for symbol in symbols:
        path = ohlcv_dir / f"{symbol}.csv"
        if not path.exists():
            continue
        df = read_ohlcv(path)
        if df.empty:
            continue
        max_dates.append(df["date"].max())
    if not max_dates:
        raise ValueError("No OHLCV data available to infer asof date.")
    return min(max_dates).strftime("%Y-%m-%d")


def _load_exogenous_features(
    exogenous_dir: Path, asof_date: str, include_raw: bool
) -> tuple[dict, dict]:
    manifest_path = exogenous_dir / "features_manifest.json"
    manifest_hash = hash_file(manifest_path) if manifest_path.exists() else None

    day_partition = exogenous_dir / f"day={asof_date}" / "part-00000.csv"
    df = None
    if day_partition.exists():
        df = pd.read_csv(day_partition)
    else:
        candidates = list(exogenous_dir.glob("*.csv"))
        if candidates:
            df = pd.read_csv(candidates[0])

    if df is None or df.empty:
        return {}, {"manifest_hash": manifest_hash, "coverage": 0, "missing_dates": [asof_date]}

    day_col = None
    for col in df.columns:
        if col.lower() in {"day", "date"}:
            day_col = col
            break
    if day_col:
        df = df[df[day_col] == asof_date]

    if df.empty:
        return {}, {"manifest_hash": manifest_hash, "coverage": 0, "missing_dates": [asof_date]}

    row = df.iloc[0].to_dict()
    if not include_raw:
        filtered = {}
        for key, value in row.items():
            if key.lower() in {"day", "date"}:
                continue
            if "lag" in key or "roll" in key:
                filtered[key] = value
        row = filtered

    coverage = 1 if row else 0
    return row, {"manifest_hash": manifest_hash, "coverage": coverage, "missing_dates": []}


def _write_results_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _json_value(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    return value


def _write_results_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, lineterminator="\n")


def _determinism_fingerprint(results_path: Path) -> str:
    content = results_path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return hash_text(content)


def _write_report(path: Path, manifest: dict, results_df: pd.DataFrame) -> None:
    total = len(results_df)
    eligible = int(results_df["gates_passed"].sum()) if not results_df.empty else 0
    lines = [
        f"# Run Report",
        "",
        f"Run ID: {manifest['run_id']}",
        f"As-of Date: {manifest['asof_date']}",
        f"Config Hash: {manifest['config_hash']}",
        "",
        f"Total symbols: {total}",
        f"Eligible: {eligible}",
        f"Ineligible: {total - eligible}",
        "",
        "## Top by Score",
    ]

    top = results_df.sort_values(["priority_score", "symbol"], ascending=[False, True]).head(10)
    for _, row in top.iterrows():
        explanation = row.get("explanation_1", "")
        lines.append(f"- {row['symbol']} | score {row['priority_score']} | {row['risk_flags']} | {explanation}")

    lines.append("")
    lines.append("## Per-symbol Summary")
    for _, row in results_df.iterrows():
        explanation = row.get("explanation_1", "")
        lines.append(f"- {row['symbol']} | score {row['priority_score']} | {row['risk_flags']} | {explanation}")

    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(lines) + "\n")

def _write_diagnostics(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2))


def _log_stage(logger: logging.Logger, stage: str, status: str, details: str | None = None) -> None:
    message = f"[stage:{stage}] {status}"
    if details:
        message = f"{message} | {details}"
    logger.info(message)

def _write_diagnostics(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _log_stage(logger: logging.Logger, stage: str, status: str, details: str | None = None) -> None:
    message = f"[stage:{stage}] {status}"
    if details:
        message = f"{message} | {details}"
    logger.info(message)


def run_watchlist(args: argparse.Namespace) -> dict:
    config_path = Path(args.config).expanduser().resolve()
    base_dir = config_path.parent

    overrides: dict[str, dict] = {}
    if args.asof:
        overrides.setdefault("pipeline", {})["asof_default"] = args.asof
    if args.include_raw_gdelt:
        overrides.setdefault("pipeline", {})["include_raw_exogenous_same_day"] = True

    config_result = load_config(config_path, overrides=overrides)
    config = config_result.config
    config_hash = config_result.config_hash

    paths = _resolve_paths(
        config,
        base_dir,
        {
            "ohlcv_raw_dir": args.ohlcv_raw_dir,
            "ohlcv_daily_dir": args.ohlcv_daily_dir,
            "exogenous_daily_dir": args.exogenous_daily_dir,
            "outputs_dir": args.outputs_dir,
        },
    )

    watchlist_path = Path(args.watchlist).expanduser()
    watchlist_df = _load_watchlist(watchlist_path)
    symbols = watchlist_df["symbol"].tolist()
    asof_date = args.asof or config.get("pipeline", {}).get("asof_default")
    if not asof_date and paths.ohlcv_daily_dir.exists():
        asof_date = _latest_common_date(symbols, paths.ohlcv_daily_dir)
    output_dir = paths.outputs_dir / args.run_id
    log_path = output_dir / "logs" / "run.log"
    logger = configure_logging(LogPaths(console_level=args.log_level, file_path=log_path))
    timings: dict[str, float] = {}
    warnings: list[str] = []
    _log_stage(logger, "run", "start", f"run_id={args.run_id}")

    _log_stage(logger, "validation", "start")
    validation = validate_data(
        watchlist_path=watchlist_path,
        ohlcv_daily_dir=paths.ohlcv_daily_dir,
        exogenous_daily_dir=paths.exogenous_daily_dir,
        asof_date=asof_date or "",
        min_history_days=int(config.get("scoring", {}).get("minimum_history_days", 252)),
        benchmark_symbols=config.get("pipeline", {}).get("benchmarks") or [],
    )
    warnings.extend(validation.warnings)
    diagnostics = {
        "run_id": args.run_id,
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "missing_symbols": validation.missing_symbols,
        "per_symbol_exclusion": validation.per_symbol_reasons,
        "exogenous_coverage_gaps": validation.exogenous_gaps,
        "benchmark_missing": validation.benchmark_missing,
        "warnings": validation.warnings,
    }
    if not validation.ok:
        _write_diagnostics(output_dir / "diagnostics.json", diagnostics)
        error_message = validation.errors[0]
        logger.error(f"[validation] {error_message}")
        raise RuntimeError(error_message)
    _log_stage(
        logger,
        "validation",
        "end",
        f"errors=0 warnings={len(validation.warnings)} symbols={len(watchlist_df)}",
    )

    if config.get("pipeline", {}).get("auto_normalize_ohlcv", True):
        if not paths.ohlcv_daily_dir.exists() or not list(paths.ohlcv_daily_dir.glob("*.csv")):
            _log_stage(logger, "ohlcv_normalize", "start", f"raw_dir={paths.ohlcv_raw_dir}")
            if not paths.ohlcv_raw_dir.exists():
                raise FileNotFoundError(f"Raw OHLCV dir not found: {paths.ohlcv_raw_dir}")
            normalize_directory(
                raw_dir=paths.ohlcv_raw_dir,
                out_dir=paths.ohlcv_daily_dir,
                date_col=None,
                delimiter=None,
                symbol_from_filename=True,
                coerce=True,
                strict=False,
                streaming=True,
                chunk_rows=200_000,
            )
            _log_stage(logger, "ohlcv_normalize", "end", f"out_dir={paths.ohlcv_daily_dir}")

    if not asof_date:
        asof_date = _latest_common_date(symbols, paths.ohlcv_daily_dir)

    ohlcv_manifest_path = paths.ohlcv_daily_dir / "ohlcv_manifest.json"
    ohlcv_manifest_hash = hash_file(ohlcv_manifest_path) if ohlcv_manifest_path.exists() else None

    _log_stage(logger, "features", "start", f"symbols={len(symbols)} workers={args.workers}")
    start = time.perf_counter()
    feature_result = compute_daily_features(
        ohlcv_dir=paths.ohlcv_daily_dir,
        out_dir=output_dir,
        asof_date=asof_date,
        workers=args.workers,
    )
    timings["features"] = time.perf_counter() - start
    features_df = pd.read_csv(feature_result["features_path"])
    features_df = features_df[features_df["symbol"].isin(symbols)]
    _log_stage(
        logger,
        "features",
        "end",
        f"rows={len(features_df)} asof={asof_date}",
    )

    regime_config = config.get("pipeline", {})
    benchmarks = regime_config.get("benchmarks") or ["SPY", "QQQ", "IWM", "TLT", "GLD"]
    _log_stage(logger, "regime", "start", f"benchmarks={benchmarks}")
    start = time.perf_counter()
    regime_result = compute_regime(
        ohlcv_dir=paths.ohlcv_daily_dir,
        benchmarks=benchmarks,
        asof_date=asof_date,
    )
    timings["regime"] = time.perf_counter() - start
    if regime_result.issues:
        warnings.extend(regime_result.issues)
    _log_stage(logger, "regime", "end", f"label={regime_result.regime_label}")

    _log_stage(logger, "exogenous", "start", f"asof={asof_date}")
    start = time.perf_counter()
    exogenous_row, exogenous_meta = _load_exogenous_features(
        paths.exogenous_daily_dir,
        asof_date,
        include_raw=config.get("pipeline", {}).get("include_raw_exogenous_same_day", False),
    )
    timings["exogenous"] = time.perf_counter() - start
    if exogenous_meta.get("missing_dates"):
        warnings.append(f"Exogenous coverage missing: {exogenous_meta['missing_dates']}")
    _log_stage(
        logger,
        "exogenous",
        "end",
        f"coverage={exogenous_meta.get('coverage', 0)} fields={len(exogenous_row)}",
    )

    scoring_cfg = config.get("scoring", {})
    gate_cfg = GateConfig(
        minimum_history_days=int(scoring_cfg.get("minimum_history_days", 252)),
        price_floor=float(scoring_cfg.get("price_floor", 1.0)),
        average_dollar_volume_floor=scoring_cfg.get("average_dollar_volume_floor"),
        max_vol_20d_cap=scoring_cfg.get("max_vol_20d_cap"),
    )
    score_cfg = ScoreConfig(
        base_score=float(scoring_cfg.get("base_score", 5.0)),
        weight_momentum=float(scoring_cfg.get("weight_momentum", 2.0)),
        weight_trend=float(scoring_cfg.get("weight_trend", 1.5)),
        weight_stability=float(scoring_cfg.get("weight_stability", 1.2)),
        weight_liquidity=float(scoring_cfg.get("weight_liquidity", 0.8)),
        regime_risk_off_penalty=float(scoring_cfg.get("regime_risk_off_penalty", 1.0)),
        regime_risk_on_bonus=float(scoring_cfg.get("regime_risk_on_bonus", 0.3)),
        vol_target=float(scoring_cfg.get("vol_target", 0.4)),
        liquidity_target=float(scoring_cfg.get("liquidity_target", 1_000_000.0)),
    )

    results = []
    json_rows = []

    features_by_symbol = {row["symbol"]: row for row in features_df.to_dict(orient="records")}

    _log_stage(logger, "scoring", "start", f"symbols={len(watchlist_df)}")
    start = time.perf_counter()
    for _, watch_row in watchlist_df.iterrows():
        symbol = watch_row["symbol"]
        feature_row = features_by_symbol.get(symbol, {})
        has_ohlcv = not pd.isna(feature_row.get("last_close"))
        history_days = int(feature_row.get("history_days", 0) or 0)
        last_close = feature_row.get("last_close")
        avg_dollar_vol = feature_row.get("avg_dollar_vol")
        vol_20d = feature_row.get("vol_20d")
        max_drawdown_252 = feature_row.get("max_drawdown_252")

        gate_decision = apply_gates(
            has_ohlcv=has_ohlcv,
            history_days=history_days,
            last_close=last_close,
            avg_dollar_vol=avg_dollar_vol,
            vol_20d=vol_20d,
            max_drawdown_252=max_drawdown_252,
            config=gate_cfg,
        )

        score_result = compute_score(
            returns_20d=feature_row.get("returns_20d"),
            trend_50=feature_row.get("trend_50"),
            vol_20d=feature_row.get("vol_20d"),
            avg_dollar_vol=feature_row.get("avg_dollar_vol"),
            regime_label=regime_result.regime_label,
            config=score_cfg,
        )

        explanations = build_explanations(
            symbol=symbol,
            gate=gate_decision,
            score=score_result,
            returns_20d=feature_row.get("returns_20d"),
            vol_20d=feature_row.get("vol_20d"),
            trend_50=feature_row.get("trend_50"),
            rsi_14=feature_row.get("rsi_14"),
            avg_dollar_vol=feature_row.get("avg_dollar_vol"),
            regime_label=regime_result.regime_label,
            exogenous=exogenous_row,
        )

        results.append(
            {
                "symbol": symbol,
                "asof_date": asof_date,
                "schema_version": OUTPUT_SCHEMA_VERSION,
                "theme_bucket": watch_row.get("theme_bucket", ""),
                "asset_type": watch_row.get("asset_type", ""),
                "gates_passed": gate_decision.passed,
                "failed_gates": join_pipe(gate_decision.failed_gates),
                "priority_score": score_result.score,
                "risk_flags": join_pipe(gate_decision.risk_flags),
                "returns_20d": feature_row.get("returns_20d"),
                "vol_20d": feature_row.get("vol_20d"),
                "trend_50": feature_row.get("trend_50"),
                "rsi_14": feature_row.get("rsi_14"),
                "avg_dollar_vol": feature_row.get("avg_dollar_vol"),
                "regime_label": regime_result.regime_label,
                **{f"explanation_{idx+1}": explanations[idx] if idx < len(explanations) else "" for idx in range(5)},
            }
        )

        json_rows.append(
            {
                "symbol": symbol,
                "asof_date": asof_date,
                "schema_version": OUTPUT_SCHEMA_VERSION,
                "theme_bucket": watch_row.get("theme_bucket", ""),
                "asset_type": watch_row.get("asset_type", ""),
                "gates_passed": gate_decision.passed,
                "failed_gates": gate_decision.failed_gates,
                "priority_score": score_result.score,
                "risk_flags": gate_decision.risk_flags,
                "metrics": {
                    "returns_20d": _json_value(feature_row.get("returns_20d")),
                    "vol_20d": _json_value(feature_row.get("vol_20d")),
                    "trend_50": _json_value(feature_row.get("trend_50")),
                    "rsi_14": _json_value(feature_row.get("rsi_14")),
                    "avg_dollar_vol": _json_value(feature_row.get("avg_dollar_vol")),
                    "regime_label": regime_result.regime_label,
                },
                "explanations": explanations,
            }
        )

    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values(
            ["priority_score", "gates_passed", "symbol"], ascending=[False, False, True]
        )
    timings["scoring"] = time.perf_counter() - start
    eligible_count = int(results_df["gates_passed"].sum()) if not results_df.empty else 0
    _log_stage(
        logger,
        "scoring",
        "end",
        f"rows={len(results_df)} eligible={eligible_count} ineligible={len(results_df) - eligible_count}",
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    results_csv_path = output_dir / "results.csv"
    results_jsonl_path = output_dir / "results.jsonl"
    _write_results_csv(results_csv_path, results_df)
    _write_results_jsonl(results_jsonl_path, json_rows)

    eligible_df = pd.DataFrame(
        {
            "symbol": results_df["symbol"],
            "eligible": results_df["gates_passed"],
            "gate_fail_reasons": results_df["failed_gates"],
            "theme_bucket": results_df["theme_bucket"],
            "asset_type": results_df["asset_type"],
        }
    )
    eligible_df.to_csv(output_dir / "eligible.csv", index=False, lineterminator="\n")

    scored_df = pd.DataFrame(
        {
            "symbol": results_df["symbol"],
            "score_1to10": results_df["priority_score"],
            "risk_flags": results_df["risk_flags"],
            "explanation": results_df["explanation_1"],
            "theme_bucket": results_df["theme_bucket"],
            "asset_type": results_df["asset_type"],
            "ml_signal": None,
            "ml_model_id": None,
            "ml_featureset_id": None,
        }
    )
    scored_df.to_csv(output_dir / "scored.csv", index=False, lineterminator="\n")

    diagnostics["per_symbol_exclusion"] = {
        row["symbol"]: row["failed_gates"].split("|") if row["failed_gates"] else []
        for _, row in results_df.iterrows()
        if not row["gates_passed"]
    }
    diagnostics["exogenous_coverage_gaps"] = exogenous_meta.get("missing_dates", [])
    _write_diagnostics(output_dir / "diagnostics.json", diagnostics)

    diagnostics["per_symbol_exclusion"] = {
        row["symbol"]: row["failed_gates"].split("|") if row["failed_gates"] else []
        for _, row in results_df.iterrows()
        if not row["gates_passed"]
    }
    diagnostics["exogenous_coverage_gaps"] = exogenous_meta.get("missing_dates", [])
    _write_diagnostics(output_dir / "diagnostics.json", diagnostics)

    run_manifest = {
        "run_id": args.run_id,
        "asof_date": asof_date,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "config_hash": config_hash,
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "watchlist_content_hash": hash_file(watchlist_path),
        "ohlcv_manifest_hash": ohlcv_manifest_hash,
        "exogenous_manifest_hash": exogenous_meta.get("manifest_hash"),
        "code_version": resolve_git_commit(base_dir),
        "determinism_fingerprint": _determinism_fingerprint(results_csv_path),
        "warnings": warnings,
        "timings": timings,
        "paths": {
            "ohlcv_raw_dir": str(paths.ohlcv_raw_dir),
            "ohlcv_daily_dir": str(paths.ohlcv_daily_dir),
            "exogenous_daily_dir": str(paths.exogenous_daily_dir),
            "outputs_dir": str(paths.outputs_dir),
        },
        "exogenous_coverage": exogenous_meta.get("coverage", 0),
    }

    manifest_path = output_dir / "run_manifest.json"
    with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(run_manifest, indent=2))

    _write_report(output_dir / "report.md", run_manifest, results_df)
    _log_stage(
        logger,
        "outputs",
        "end",
        f"results={results_csv_path.name} eligible={len(eligible_df)} scored={len(scored_df)}",
    )
    if warnings:
        logger.warning(f"[warnings] count={len(warnings)}")
    _log_stage(logger, "run", "end", f"run_id={args.run_id}")

    if args.profile and timings:
        timing_lines = ["Stage timings (s):"]
        for stage, duration in sorted(timings.items()):
            timing_lines.append(f"  - {stage}: {duration:.3f}s")
        logger.info("\n".join(timing_lines))

    return run_manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the offline watchlist monitor.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--watchlist", required=True)
    parser.add_argument("--asof", default=None)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ohlcv-raw-dir", default=None)
    parser.add_argument("--ohlcv-daily-dir", default=None)
    parser.add_argument("--exogenous-daily-dir", default=None)
    parser.add_argument("--outputs-dir", default=None)
    parser.add_argument("--include-raw-gdelt", action="store_true", default=False)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--profile", action="store_true", default=False)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    run_watchlist(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
