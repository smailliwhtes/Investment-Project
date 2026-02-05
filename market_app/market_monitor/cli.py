import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from market_monitor.config_schema import ConfigError, load_config, write_default_config
from market_monitor.corpus import (
    build_corpus_daily_store,
    build_corpus_index,
    build_corpus_manifest,
    discover_corpus_sources,
    run_corpus_pipeline,
    validate_corpus_sources,
    verify_md5_for_zip,
)
from market_monitor.data_paths import resolve_corpus_paths, resolve_data_paths
from market_monitor.doctor import run_doctor
from market_monitor.bulk import (
    BulkManifest,
    build_download_plan,
    download_tasks,
    load_bulk_sources,
    standardize_directory,
    write_manifest,
)
from market_monitor.io import (
    ELIGIBLE_COLUMNS,
    build_feature_columns,
    build_scored_columns,
    write_csv,
)
from market_monitor.features.io import read_ohlcv
from market_monitor.gdelt.doctor import normalize_corpus
from market_monitor.logging_utils import get_console_logger
from market_monitor.paths import find_repo_root, resolve_path
from market_monitor.providers.base import ProviderError
from market_monitor.providers.http import RetryConfig
from market_monitor.manifest import resolve_git_commit, run_id_from_inputs
from market_monitor.offline import set_offline_mode
from market_monitor.ohlcv_doctor import normalize_directory
from market_monitor.preflight import run_preflight
from market_monitor.prediction import build_panel, latest_predictions, train_and_predict
from market_monitor.evaluate import run_evaluation
from market_monitor.pipeline import run_pipeline as engine_run_pipeline
from market_monitor.provider_factory import build_provider
from market_monitor.provision import import_exogenous, import_ohlcv, init_dirs
from market_monitor.run_watchlist import run_watchlist as run_watchlist_pipeline
from market_monitor.themes import tag_themes
from market_monitor.universe import (
    fetch_universe,
    filter_universe,
    read_watchlist,
    write_universe_csv,
)
from market_monitor.validation import validate_data, validate_watchlist
from market_monitor.version import __version__




def _resolve_config_paths(config_arg: str) -> tuple[Path, Path, Path]:
    config_path = Path(config_arg).expanduser().resolve()
    base_dir = config_path.parent
    repo_root = find_repo_root(base_dir)
    return config_path, base_dir, repo_root


def run_pipeline(args: argparse.Namespace) -> int:
    config_path, base_dir, repo_root = _resolve_config_paths(args.config)
    try:
        overrides: dict[str, Any] = {}
        if args.provider:
            overrides.setdefault("data", {})["provider"] = args.provider
        if getattr(args, "price_min", None) is not None:
            overrides.setdefault("gates", {})["price_min"] = args.price_min
        if args.price_max is not None:
            overrides.setdefault("gates", {})["price_max"] = args.price_max
        if args.history_min_days is not None:
            overrides.setdefault("staging", {})["history_min_days"] = args.history_min_days
        if args.outdir:
            overrides.setdefault("paths", {})["outputs_dir"] = args.outdir
        if args.cache_dir:
            overrides.setdefault("paths", {})["cache_dir"] = args.cache_dir
        if getattr(args, "offline", False):
            overrides.setdefault("data", {})["offline_mode"] = True
        if args.max_workers is not None:
            overrides.setdefault("data", {})["max_workers"] = args.max_workers

        config_result = load_config(config_path, overrides=overrides)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    config = config_result.config
    config_hash = config_result.config_hash

    logger = get_console_logger(args.log_level)
    offline_mode = bool(config["data"].get("offline_mode", False))
    if not offline_mode:
        logger.error("Offline mode is required for this release. Set data.offline_mode=true.")
        return 3
    set_offline_mode(offline_mode)

    if config["data"].get("offline_mode", False) and args.mode != "watchlist":
        logger.error("Offline mode is enabled; only --mode watchlist is supported.")
        return 3

    watchlist_path = (
        resolve_path(base_dir, args.watchlist or config["paths"]["watchlist_file"])
        if args.mode == "watchlist"
        else None
    )
    outputs_dir = resolve_path(base_dir, args.outdir or config["paths"]["outputs_dir"])
    try:
        engine_run_pipeline(
            config,
            base_dir=base_dir,
            mode=args.mode,
            watchlist_path=watchlist_path,
            output_dir=outputs_dir,
            run_id=getattr(args, "run_id", None),
            logger=logger,
            write_legacy_outputs=True,
        )
    except RuntimeError as exc:
        logger.error(str(exc))
        return 3
    return 0


def run_watchlist_command(args: argparse.Namespace) -> int:
    try:
        run_watchlist_pipeline(args)
    except RuntimeError as exc:
        print(f"[error] {exc}")
        return 3
    return 0


def run_validate_data(args: argparse.Namespace) -> int:
    config_path, base_dir, _ = _resolve_config_paths(args.config)
    try:
        config_result = load_config(config_path)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    config = config_result.config
    paths_cfg = config.get("paths", {})
    ohlcv_daily_dir = resolve_path(
        base_dir, args.ohlcv_daily_dir or paths_cfg.get("ohlcv_daily_dir", "data/ohlcv_daily")
    )
    exogenous_daily_dir = resolve_path(
        base_dir,
        args.exogenous_daily_dir
        or paths_cfg.get("exogenous_daily_dir", "data/exogenous/daily_features"),
    )
    watchlist_path = resolve_path(
        base_dir,
        args.watchlist or paths_cfg.get("watchlist_file", "watchlists/watchlist_core.csv"),
    )
    scoring_cfg = config.get("scoring", {})
    min_history_days = int(scoring_cfg.get("minimum_history_days", 252))
    benchmarks = config.get("pipeline", {}).get("benchmarks") or []
    asof_date = args.asof or config.get("pipeline", {}).get("asof_default") or ""
    if not asof_date and ohlcv_daily_dir.exists() and watchlist_path.exists():
        watchlist_df, _ = validate_watchlist(watchlist_path)
        max_dates = []
        for symbol in watchlist_df["symbol"].tolist():
            path = ohlcv_daily_dir / f"{symbol}.csv"
            if not path.exists():
                continue
            df = read_ohlcv(path)
            if df.empty:
                continue
            max_dates.append(df["date"].max())
        if max_dates:
            asof_date = min(max_dates).strftime("%Y-%m-%d")

    result = validate_data(
        watchlist_path=watchlist_path,
        ohlcv_daily_dir=ohlcv_daily_dir,
        exogenous_daily_dir=exogenous_daily_dir,
        asof_date=asof_date,
        min_history_days=min_history_days,
        benchmark_symbols=benchmarks,
    )

    if result.ok:
        print("validate-data: OK")
        if result.warnings:
            print("Warnings:")
            for warning in result.warnings:
                print(f"  - {warning}")
        return 0

    print("validate-data: FAILED")
    for error in result.errors:
        print(f"  - {error}")
    if result.warnings:
        print("Warnings:")
        for warning in result.warnings:
            print(f"  - {warning}")
    return 2


def run_provision_init(args: argparse.Namespace) -> int:
    payload = init_dirs(Path(args.root))
    print(json.dumps(payload, indent=2))
    return 0


def run_provision_import_ohlcv(args: argparse.Namespace) -> int:
    result = import_ohlcv(
        src=Path(args.src),
        dest=Path(args.dest),
        normalize=args.normalize,
        date_col=args.date_col,
        delimiter=args.delimiter,
    )
    print(json.dumps(result, indent=2))
    return 0


def run_provision_import_exogenous(args: argparse.Namespace) -> int:
    result = import_exogenous(
        src=Path(args.src),
        dest=Path(args.dest),
        normalize=args.normalize,
        normalized_dest=Path(args.normalized_dest) if args.normalized_dest else None,
        file_glob=args.glob,
        format_hint=args.format,
        write_format=args.write,
        date_col=args.date_col,
        allow_annual=args.allow_annual,
    )
    print(json.dumps(result, indent=2))
    return 0


def _build_context_summary(corpus_run) -> dict[str, Any] | None:
    if corpus_run is None or corpus_run.daily_features is None or corpus_run.daily_features.empty:
        return None
    latest = corpus_run.daily_features.iloc[-1].to_dict()
    analog_outcomes = corpus_run.analog_outcomes or []
    outcome_summary = {}
    if analog_outcomes:
        df = pd.DataFrame(analog_outcomes)
        outcome_summary = (
            df.groupby(["symbol", "forward_days"])["forward_return"].mean().reset_index().to_dict("records")
        )
    return {
        "latest_date": latest.get("Date"),
        "latest_metrics": {
            key: value
            for key, value in latest.items()
            if key
            in {
                "conflict_event_count_total",
                "goldstein_mean",
                "tone_mean",
                "mentions_sum",
                "sources_sum",
                "articles_sum",
            }
        },
        "analogs": corpus_run.analogs or [],
        "event_impact_rows": len(corpus_run.event_impact) if corpus_run.event_impact is not None else 0,
        "analog_outcomes": outcome_summary,
    }


def run_bulk_plan(args: argparse.Namespace) -> int:
    config_path, base_dir, _ = _resolve_config_paths(args.config)
    try:
        config = load_config(config_path).config
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    logger = get_console_logger(args.log_level)
    set_offline_mode(bool(config["data"].get("offline_mode", False)))
    sources = load_bulk_sources(config)
    if args.sources:
        allowed = {name.strip() for name in args.sources.split(",") if name.strip()}
        sources = [src for src in sources if src.name in allowed]

    symbols = _load_bulk_symbols(config, base_dir, args)
    bulk_paths = config.get("bulk", {}).get("paths", {})
    raw_dir = resolve_path(base_dir, bulk_paths.get("raw_dir", "data/raw"))
    manifest_dir = resolve_path(base_dir, bulk_paths.get("manifest_dir", "data/manifests"))

    tasks = build_download_plan(sources, symbols, raw_dir, use_archives=args.use_archives)
    manifest = BulkManifest.create(tasks)
    manifest_path = resolve_path(
        base_dir,
        args.manifest or (manifest_dir / f"bulk_manifest_{manifest.created_at_utc}.json"),
    )
    write_manifest(manifest_path, manifest)
    logger.info(f"[bulk] planned {len(tasks)} tasks -> {manifest_path}")
    return 0


def run_bulk_download(args: argparse.Namespace) -> int:
    config_path, base_dir, _ = _resolve_config_paths(args.config)
    try:
        config = load_config(config_path).config
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    logger = get_console_logger(args.log_level)
    set_offline_mode(bool(config["data"].get("offline_mode", False)))
    sources = load_bulk_sources(config)
    if args.sources:
        allowed = {name.strip() for name in args.sources.split(",") if name.strip()}
        sources = [src for src in sources if src.name in allowed]

    symbols = _load_bulk_symbols(config, base_dir, args)
    bulk_paths = config.get("bulk", {}).get("paths", {})
    raw_dir = resolve_path(base_dir, bulk_paths.get("raw_dir", "data/raw"))
    manifest_dir = resolve_path(base_dir, bulk_paths.get("manifest_dir", "data/manifests"))

    tasks = build_download_plan(sources, symbols, raw_dir, use_archives=args.use_archives)
    manifest = BulkManifest.create(tasks)
    manifest_path = resolve_path(
        base_dir,
        args.manifest or (manifest_dir / f"bulk_manifest_{manifest.created_at_utc}.json"),
    )
    write_manifest(manifest_path, manifest)

    throttling_cfg = config["data"].get("throttling", {})
    retry_config = RetryConfig(
        max_retries=int(throttling_cfg.get("max_retries", 3)),
        base_delay_s=float(throttling_cfg.get("base_delay_s", 0.3)),
        jitter_s=float(throttling_cfg.get("jitter_s", 0.2)),
    )
    summary = download_tasks(
        tasks,
        retry_config=retry_config,
        timeout_s=args.timeout,
        dry_run=args.dry_run,
        extract_archives=args.extract_archives,
        logger=logger,
    )
    logger.info(
        f"[bulk] planned={summary.planned} downloaded={summary.downloaded} "
        f"skipped={summary.skipped} failed={summary.failed}"
    )
    return 0 if summary.failed == 0 else 1


def run_bulk_standardize(args: argparse.Namespace) -> int:
    config_path, base_dir, _ = _resolve_config_paths(args.config)
    try:
        config = load_config(config_path).config
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    logger = get_console_logger(args.log_level)
    set_offline_mode(bool(config["data"].get("offline_mode", False)))
    bulk_paths = config.get("bulk", {}).get("paths", {})
    raw_dir = resolve_path(base_dir, bulk_paths.get("raw_dir", "data/raw"))
    curated_dir = resolve_path(base_dir, bulk_paths.get("curated_dir", "data/curated"))

    input_dir = (
        resolve_path(base_dir, args.input_dir) if args.input_dir else raw_dir / args.source
    )
    output_dir = (
        resolve_path(base_dir, args.output_dir) if args.output_dir else curated_dir / args.source
    )

    results = standardize_directory(
        input_dir,
        output_dir,
        mode=args.mode,
        value_column=args.value_column,
    )
    total_rows = sum(result.rows for result in results)
    logger.info(f"[bulk] standardized {len(results)} files -> {output_dir} ({total_rows} rows)")
    return 0


def run_preflight_command(args: argparse.Namespace) -> int:
    config_path, base_dir, _ = _resolve_config_paths(args.config)
    try:
        config_result = load_config(config_path)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    config = config_result.config
    config_hash = config_result.config_hash
    logger = get_console_logger(args.log_level)
    offline_mode = bool(config["data"].get("offline_mode", False))
    if not offline_mode:
        logger.error("Offline mode is required for preflight.")
        return 3
    set_offline_mode(offline_mode)

    watchlist_path = resolve_path(base_dir, args.watchlist or config["paths"]["watchlist_file"])
    watchlist_df = read_watchlist(watchlist_path)
    if watchlist_df.empty:
        logger.error(f"Watchlist is empty or missing at {watchlist_path}.")
        return 3

    provider = build_provider(config, logger, base_dir)
    outputs_dir = resolve_path(base_dir, args.outdir or config["paths"]["outputs_dir"])
    run_start = datetime.now(timezone.utc)
    run_timestamp = run_start.isoformat()
    run_id = run_id_from_inputs(
        timestamp=run_start,
        config_hash=config_hash,
        watchlist_path=watchlist_path,
        watchlist_df=watchlist_df,
    )
    corpus_paths = resolve_corpus_paths(config, base_dir)

    run_preflight(
        watchlist_df,
        provider,
        outputs_dir,
        run_id=run_id,
        run_timestamp=run_timestamp,
        logger=logger,
        corpus_dir=corpus_paths.gdelt_conflict_dir,
        raw_events_dir=corpus_paths.gdelt_events_raw_dir,
    )
    logger.info(f"[preflight] report written to {outputs_dir}")
    return 0


def _load_config_for_command(config_path: Path) -> tuple[dict[str, Any], str]:
    config_result = load_config(config_path)
    return config_result.config, config_result.config_hash


def run_corpus_build(args: argparse.Namespace) -> int:
    try:
        config_path, base_dir, _ = _resolve_config_paths(args.config)
        config, _ = _load_config_for_command(config_path)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2
    logger = get_console_logger(args.log_level)
    if not bool(config["data"].get("offline_mode", False)):
        logger.error("Offline mode is required for corpus builds. Set data.offline_mode=true.")
        return 3
    set_offline_mode(True)

    corpus_paths = resolve_corpus_paths(config, base_dir)
    sources = discover_corpus_sources(
        corpus_paths.gdelt_conflict_dir,
        corpus_paths.gdelt_events_raw_dir,
    )
    if not sources:
        logger.warning("[corpus] No corpus sources found; skipping build.")
        return 0

    outputs_dir = resolve_path(base_dir, args.outdir or config["paths"]["outputs_dir"]) / "corpus"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    features_cfg = config.get("corpus", {}).get("features", {})
    settings = {
        "rootcode_top_n": int(features_cfg.get("rootcode_top_n", 8)),
        "country_top_k": int(features_cfg.get("country_top_k", 8)),
    }
    daily_features, infos, manifest_payload, _, cache_hit = build_corpus_daily_store(
        sources,
        outputs_dir=outputs_dir,
        settings=settings,
        logger=logger,
    )

    index_path = outputs_dir / "corpus_index.json"
    index_payload = build_corpus_index(sources, index_path)
    manifest = {
        "corpus_dir": str(corpus_paths.root_dir or corpus_paths.gdelt_conflict_dir or "unset"),
        "files": [
            {
                "path": str(info.path),
                "sha256": info.checksum,
                "rows": info.rows,
                "min_date": info.min_date,
                "max_date": info.max_date,
            }
            for info in infos
        ],
    }
    for entry in manifest["files"]:
        cached = next((item for item in index_payload["files"] if item["path"] == entry["path"]), None)
        if cached is not None and (entry["rows"] or entry["min_date"] or entry["max_date"] or not cache_hit):
            cached.update(
                {
                    "rows": entry["rows"],
                    "min_date": entry["min_date"],
                    "max_date": entry["max_date"],
                }
            )
    index_path.write_text(json.dumps(index_payload, indent=2, sort_keys=True), encoding="utf-8")
    manifest["raw_event_zips"] = manifest_payload.get("raw_event_zips", [])
    if "settings" in manifest_payload:
        manifest["settings"] = manifest_payload["settings"]
    (outputs_dir / "corpus_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    if daily_features.empty:
        logger.warning("[corpus] No daily features generated.")
        return 0
    logger.info(f"[corpus] daily features rows: {len(daily_features)}")
    return 0


def run_corpus_validate(args: argparse.Namespace) -> int:
    try:
        config_path, base_dir, _ = _resolve_config_paths(args.config)
        config, _ = _load_config_for_command(config_path)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2
    logger = get_console_logger(args.log_level)
    if not bool(config["data"].get("offline_mode", False)):
        logger.error("Offline mode is required for corpus validation. Set data.offline_mode=true.")
        return 3
    set_offline_mode(True)

    corpus_paths = resolve_corpus_paths(config, base_dir)
    sources = discover_corpus_sources(
        corpus_paths.gdelt_conflict_dir,
        corpus_paths.gdelt_events_raw_dir,
    )
    if not sources:
        logger.warning("[corpus] No corpus sources found; nothing to validate.")
        return 0

    features_cfg = config.get("corpus", {}).get("features", {})
    report = validate_corpus_sources(
        sources,
        rootcode_top_n=int(features_cfg.get("rootcode_top_n", 8)),
        country_top_k=int(features_cfg.get("country_top_k", 8)),
    )
    md5_issues = []
    for source in sources:
        if source.source_type == "zip":
            ok, detail = verify_md5_for_zip(source.path)
            if not ok and detail:
                md5_issues.append(detail)

    outputs_dir = resolve_path(base_dir, args.outdir or config["paths"]["outputs_dir"]) / "corpus"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    report_path = outputs_dir / "corpus_validate.json"
    payload = {
        "dedupe_rate": report.dedupe_rate,
        "duplicate_reasons": report.duplicate_reasons,
        "min_date": report.min_date,
        "max_date": report.max_date,
        "feature_flags": report.feature_flags,
        "files": [
            {
                "path": str(info.path),
                "rows": info.rows,
                "min_date": info.min_date,
                "max_date": info.max_date,
                "columns": info.columns,
            }
            for info in report.sources
        ],
        "md5_issues": md5_issues,
    }
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    logger.info(f"[corpus] validation report written to {report_path}")
    missing_flags = [name for name, enabled in report.feature_flags.items() if not enabled]
    if missing_flags:
        logger.warning("[corpus] missing feature columns: " + ", ".join(sorted(missing_flags)))
    if md5_issues:
        logger.warning("[corpus] md5 issues detected: " + "; ".join(md5_issues))
    return 0


def run_evaluate_command(args: argparse.Namespace) -> int:
    config_path = Path(args.config).expanduser().resolve()
    base_dir = config_path.parent
    try:
        config = load_config(config_path).config
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2
    logger = get_console_logger(args.log_level)
    if args.offline:
        config.setdefault("data", {})["offline_mode"] = True
    if not bool(config["data"].get("offline_mode", False)):
        logger.error("Offline mode is required for evaluation. Set data.offline_mode=true.")
        return 3
    set_offline_mode(True)
    provider = build_provider(config, logger, base_dir)
    corpus_paths = resolve_corpus_paths(config, base_dir)
    outputs_dir = resolve_path(base_dir, args.outdir or config["paths"]["outputs_dir"]) / "eval"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    return run_evaluation(
        config=config,
        provider=provider,
        corpus_paths=corpus_paths,
        outputs_dir=outputs_dir,
        base_dir=base_dir,
        logger=logger,
    )


def _load_bulk_symbols(config: dict[str, Any], base_dir: Path, args: argparse.Namespace) -> pd.Series:
    mode = args.mode or "watchlist"
    if mode == "universe":
        universe_df = fetch_universe()
        universe_df = filter_universe(
            universe_df,
            config["universe"]["allowed_security_types"],
            config["universe"]["allowed_currencies"],
            config["universe"]["include_etfs"],
        )
        return universe_df["symbol"]

    watchlist_path = resolve_path(base_dir, args.watchlist or config["paths"]["watchlist_file"])
    universe_df = read_watchlist(watchlist_path)
    return universe_df["symbol"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("market-monitor")
    parser.add_argument(
        "--version",
        action="version",
        version=f"market-monitor {__version__}",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    doctor_parser = sub.add_parser("doctor", help="Run diagnostics")
    doctor_parser.add_argument("--config", default="config.yaml")
    doctor_parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip connectivity checks (also honored by MM_OFFLINE=1).",
    )
    doctor_parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat provider/bulk connectivity warnings as errors.",
    )

    validate_parser = sub.add_parser("validate", help="Validate config")
    validate_parser.add_argument("--config", required=True)

    init_parser = sub.add_parser("init-config", help="Write default config")
    init_parser.add_argument("--out", required=True)

    run_parser = sub.add_parser("run", help="Run the offline watchlist monitor")
    run_parser.add_argument("--config", default="config.yaml", help="Config path (default: config.yaml)")
    run_parser.add_argument("--watchlist", required=True, help="Watchlist CSV path")
    run_parser.add_argument("--asof", default=None)
    run_parser.add_argument("--run-id", required=True, help="Run identifier (outputs/<run_id>)")
    run_parser.add_argument("--ohlcv-raw-dir", default=None, help="Raw OHLCV dir (default: data/ohlcv_raw)")
    run_parser.add_argument(
        "--ohlcv-daily-dir", default=None, help="Normalized OHLCV dir (default: data/ohlcv_daily)"
    )
    run_parser.add_argument(
        "--exogenous-daily-dir",
        default=None,
        help="Exogenous daily features dir (default: data/exogenous/daily_features)",
    )
    run_parser.add_argument("--outputs-dir", default=None, help="Outputs root (default: outputs)")
    run_parser.add_argument("--include-raw-gdelt", action="store_true", default=False)
    run_parser.add_argument("--log-level", default="INFO")
    run_parser.add_argument("--workers", type=int, default=1)
    run_parser.add_argument("--profile", action="store_true", default=False)

    legacy_run_parser = sub.add_parser("run-legacy", help="Run the legacy monitor pipeline")
    legacy_run_parser.add_argument("--config", default="config.yaml")
    legacy_run_parser.add_argument(
        "--mode", choices=["universe", "watchlist", "themed", "batch"], default="watchlist"
    )
    legacy_run_parser.add_argument("--watchlist")
    legacy_run_parser.add_argument("--themes")
    legacy_run_parser.add_argument("--provider")
    legacy_run_parser.add_argument("--price-min", type=float)
    legacy_run_parser.add_argument("--price-max", type=float)
    legacy_run_parser.add_argument("--history-min-days", type=int)
    legacy_run_parser.add_argument("--outdir")
    legacy_run_parser.add_argument("--cache-dir")
    legacy_run_parser.add_argument("--run-id")
    legacy_run_parser.add_argument(
        "--offline",
        action="store_true",
        help="Force offline mode for this run (overrides config).",
    )
    legacy_run_parser.add_argument("--batch-size", type=int)
    legacy_run_parser.add_argument("--batch-cursor-file")
    legacy_run_parser.add_argument("--max-workers", type=int)
    legacy_run_parser.add_argument("--log-level", default="INFO")

    preflight_parser = sub.add_parser("preflight", help="Run offline preflight checks")
    preflight_parser.add_argument("--config", default="config.yaml")
    preflight_parser.add_argument("--watchlist")
    preflight_parser.add_argument("--outdir")
    preflight_parser.add_argument("--log-level", default="INFO")

    bulk_plan_parser = sub.add_parser("bulk-plan", help="Plan bulk CSV downloads")
    bulk_plan_parser.add_argument("--config", default="config.yaml")
    bulk_plan_parser.add_argument("--mode", choices=["watchlist", "universe"], default="watchlist")
    bulk_plan_parser.add_argument("--watchlist")
    bulk_plan_parser.add_argument("--sources")
    bulk_plan_parser.add_argument("--use-archives", action="store_true")
    bulk_plan_parser.add_argument("--manifest")
    bulk_plan_parser.add_argument("--log-level", default="INFO")

    bulk_download_parser = sub.add_parser("bulk-download", help="Download bulk CSV data")
    bulk_download_parser.add_argument("--config", default="config.yaml")
    bulk_download_parser.add_argument("--mode", choices=["watchlist", "universe"], default="watchlist")
    bulk_download_parser.add_argument("--watchlist")
    bulk_download_parser.add_argument("--sources")
    bulk_download_parser.add_argument("--use-archives", action="store_true")
    bulk_download_parser.add_argument("--manifest")
    bulk_download_parser.add_argument("--timeout", type=float, default=60)
    bulk_download_parser.add_argument("--dry-run", action="store_true")
    bulk_download_parser.add_argument("--extract-archives", action="store_true")
    bulk_download_parser.add_argument("--log-level", default="INFO")

    bulk_standardize_parser = sub.add_parser(
        "bulk-standardize", help="Standardize bulk CSV data into curated outputs"
    )
    bulk_standardize_parser.add_argument("--config", default="config.yaml")
    bulk_standardize_parser.add_argument("--source", required=True)
    bulk_standardize_parser.add_argument("--mode", choices=["ohlcv", "timeseries"], required=True)
    bulk_standardize_parser.add_argument("--input-dir")
    bulk_standardize_parser.add_argument("--output-dir")
    bulk_standardize_parser.add_argument("--value-column")
    bulk_standardize_parser.add_argument("--log-level", default="INFO")

    corpus_parser = sub.add_parser("corpus", help="Corpus ingestion utilities")
    corpus_sub = corpus_parser.add_subparsers(dest="corpus_command")
    corpus_build_parser = corpus_sub.add_parser("build", help="Build corpus daily feature store")
    corpus_build_parser.add_argument("--config", default="config.yaml")
    corpus_build_parser.add_argument("--outdir")
    corpus_build_parser.add_argument("--log-level", default="INFO")

    corpus_validate_parser = corpus_sub.add_parser("validate", help="Validate corpus inputs")
    corpus_validate_parser.add_argument("--config", default="config.yaml")
    corpus_validate_parser.add_argument("--outdir")
    corpus_validate_parser.add_argument("--log-level", default="INFO")

    evaluate_parser = sub.add_parser("evaluate", help="Run offline evaluation harness")
    evaluate_parser.add_argument("--config", default="config.yaml")
    evaluate_parser.add_argument("--outdir")
    evaluate_parser.add_argument(
        "--offline",
        action="store_true",
        help="Force offline mode for evaluation (overrides config).",
    )
    evaluate_parser.add_argument("--log-level", default="INFO")

    validate_data_parser = sub.add_parser("validate-data", help="Validate offline data inputs")
    validate_data_parser.add_argument("--config", default="config.yaml")
    validate_data_parser.add_argument("--watchlist", help="Default: config paths.watchlist_file")
    validate_data_parser.add_argument(
        "--ohlcv-daily-dir", help="Default: config paths.ohlcv_daily_dir or data/ohlcv_daily"
    )
    validate_data_parser.add_argument(
        "--exogenous-daily-dir",
        help="Default: config paths.exogenous_daily_dir or data/exogenous/daily_features",
    )
    validate_data_parser.add_argument("--asof")

    ohlcv_parser = sub.add_parser("ohlcv", help="OHLCV utilities")
    ohlcv_sub = ohlcv_parser.add_subparsers(dest="ohlcv_command")
    ohlcv_norm_parser = ohlcv_sub.add_parser("normalize", help="Normalize OHLCV files")
    ohlcv_norm_parser.add_argument("--raw-dir", required=True)
    ohlcv_norm_parser.add_argument("--out-dir", required=True)
    ohlcv_norm_parser.add_argument("--date-col", default=None)
    ohlcv_norm_parser.add_argument("--delimiter", default=None)
    ohlcv_norm_parser.add_argument("--streaming", action="store_true", default=True)
    ohlcv_norm_parser.add_argument("--chunk-rows", type=int, default=200_000)

    exogenous_parser = sub.add_parser("exogenous", help="Exogenous data utilities")
    exogenous_sub = exogenous_parser.add_subparsers(dest="exogenous_command")
    exogenous_norm_parser = exogenous_sub.add_parser(
        "normalize", help="Normalize exogenous data (alias to gdelt.doctor normalize)"
    )
    exogenous_norm_parser.add_argument("--raw-dir", required=True)
    exogenous_norm_parser.add_argument("--gdelt-dir", required=True)
    exogenous_norm_parser.add_argument("--glob", default="*.csv")
    exogenous_norm_parser.add_argument("--format", default="auto")
    exogenous_norm_parser.add_argument("--write", default="csv")
    exogenous_norm_parser.add_argument("--date-col", default=None)
    exogenous_norm_parser.add_argument("--allow-annual", action="store_true", default=False)

    provision_parser = sub.add_parser("provision", help="Offline data provisioning utilities")
    provision_sub = provision_parser.add_subparsers(dest="provision_command")
    provision_init = provision_sub.add_parser("init-dirs", help="Create canonical data layout")
    provision_init.add_argument("--root", required=True)
    provision_ohlcv = provision_sub.add_parser("import-ohlcv", help="Import OHLCV data")
    provision_ohlcv.add_argument("--src", required=True)
    provision_ohlcv.add_argument("--dest", required=True)
    provision_ohlcv.add_argument("--normalize", action="store_true", default=False)
    provision_ohlcv.add_argument("--date-col", default=None)
    provision_ohlcv.add_argument("--delimiter", default=None)
    provision_exog = provision_sub.add_parser("import-exogenous", help="Import exogenous data")
    provision_exog.add_argument("--src", required=True)
    provision_exog.add_argument("--dest", required=True)
    provision_exog.add_argument("--normalize", action="store_true", default=False)
    provision_exog.add_argument("--normalized-dest")
    provision_exog.add_argument("--glob", default="*.csv")
    provision_exog.add_argument("--format", default="auto")
    provision_exog.add_argument("--write", default="csv")
    provision_exog.add_argument("--date-col", default=None)
    provision_exog.add_argument("--allow-annual", action="store_true", default=False)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "init-config":
        write_default_config(Path(args.out))
        print(f"Default config written to {args.out}")
        return 0

    if args.command == "validate":
        try:
            load_config(Path(args.config))
            print("Config valid.")
            return 0
        except ConfigError as exc:
            print(f"Config invalid: {exc}")
            return 2

    if args.command == "doctor":
        return run_doctor(Path(args.config), offline=args.offline, strict=args.strict)

    if args.command == "run":
        return run_watchlist_command(args)

    if args.command == "run-legacy":
        return run_pipeline(args)

    if args.command == "preflight":
        return run_preflight_command(args)

    if args.command == "corpus":
        if args.corpus_command == "build":
            return run_corpus_build(args)
        if args.corpus_command == "validate":
            return run_corpus_validate(args)
        return 2

    if args.command == "bulk-plan":
        return run_bulk_plan(args)

    if args.command == "bulk-download":
        return run_bulk_download(args)

    if args.command == "bulk-standardize":
        return run_bulk_standardize(args)

    if args.command == "evaluate":
        return run_evaluate_command(args)

    if args.command == "validate-data":
        return run_validate_data(args)

    if args.command == "ohlcv":
        if args.ohlcv_command == "normalize":
            result = normalize_directory(
                raw_dir=Path(args.raw_dir),
                out_dir=Path(args.out_dir),
                date_col=args.date_col,
                delimiter=args.delimiter,
                symbol_from_filename=True,
                coerce=True,
                strict=False,
                streaming=args.streaming,
                chunk_rows=args.chunk_rows,
            )
            print(json.dumps({"manifest_path": str(result["manifest_path"])}, indent=2))
            return 0
        return 2

    if args.command == "exogenous":
        if args.exogenous_command == "normalize":
            normalize_corpus(
                raw_dir=Path(args.raw_dir).expanduser(),
                gdelt_dir=Path(args.gdelt_dir).expanduser(),
                file_glob=args.glob,
                format_hint=args.format,
                write_format=args.write,
                date_col=args.date_col,
                allow_annual=args.allow_annual,
            )
            print("[exogenous] normalization complete.")
            return 0
        return 2

    if args.command == "provision":
        if args.provision_command == "init-dirs":
            return run_provision_init(args)
        if args.provision_command == "import-ohlcv":
            return run_provision_import_ohlcv(args)
        if args.provision_command == "import-exogenous":
            return run_provision_import_exogenous(args)
        return 2

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
