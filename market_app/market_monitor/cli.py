import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from market_monitor.config_schema import ConfigError, load_config, write_default_config
from market_monitor.doctor import run_doctor
from market_monitor.bulk import (
    BulkManifest,
    build_download_plan,
    download_tasks,
    load_bulk_sources,
    standardize_directory,
    write_manifest,
)
from market_monitor.io import ELIGIBLE_COLUMNS, FEATURE_COLUMNS, SCORED_COLUMNS, write_csv
from market_monitor.logging_utils import JsonlLogger, get_console_logger
from market_monitor.paths import find_repo_root, resolve_path
from market_monitor.providers.alphavantage import AlphaVantageProvider
from market_monitor.providers.base import (
    BudgetManager,
    HistoryProvider,
    ProviderAccessError,
    ProviderError,
)
from market_monitor.providers.finnhub import FinnhubProvider
from market_monitor.providers.http import RetryConfig
from market_monitor.providers.stooq import StooqProvider
from market_monitor.providers.twelvedata import TwelveDataProvider
from market_monitor.report import write_report
from market_monitor.scoring import score_frame
from market_monitor.staging import stage_pipeline
from market_monitor.themes import tag_themes
from market_monitor.universe import (
    fetch_universe,
    filter_universe,
    read_watchlist,
    write_universe_csv,
)


class LimitedProvider(HistoryProvider):
    def __init__(self, provider: HistoryProvider, budget: BudgetManager) -> None:
        self.provider = provider
        self.budget = budget
        self.name = provider.name
        self.capabilities = provider.capabilities

    def get_history(self, symbol: str, days: int):
        self.budget.consume()
        return self.provider.get_history(symbol, days)

    def get_quote(self, symbol: str):
        self.budget.consume()
        return self.provider.get_quote(symbol)


class FallbackProvider(HistoryProvider):
    def __init__(self, primary: HistoryProvider, fallbacks: list[HistoryProvider], logger) -> None:
        self.primary = primary
        self.fallbacks = fallbacks
        self.logger = logger
        self.name = primary.name
        self.capabilities = primary.capabilities

    def get_history(self, symbol: str, days: int):
        try:
            return self.primary.get_history(symbol, days)
        except ProviderAccessError as exc:
            self.logger.warning(f"Primary provider {self.primary.name} history unavailable: {exc}")
        for fallback in self.fallbacks:
            try:
                return fallback.get_history(symbol, days)
            except ProviderError:
                continue
        raise ProviderError("All history providers failed")

    def get_quote(self, symbol: str):
        return self.primary.get_quote(symbol)


def _build_provider(config: dict[str, Any], logger) -> HistoryProvider:
    provider_name = config["data"]["provider"]
    budget_cfg = config["data"].get("budget", {})
    fallback_chain = config["data"].get("fallback_chain", [])
    throttling_cfg = config["data"].get("throttling", {})
    retry_config = RetryConfig(
        max_retries=int(throttling_cfg.get("max_retries", 3)),
        base_delay_s=float(throttling_cfg.get("base_delay_s", 0.3)),
        jitter_s=float(throttling_cfg.get("jitter_s", 0.2)),
    )
    session = requests.Session()
    sleep_ms = int(float(throttling_cfg.get("base_delay_s", 0.3)) * 1000)

    def build(name: str) -> HistoryProvider:
        if name == "stooq":
            return StooqProvider(sleep_ms=sleep_ms, retry_config=retry_config, session=session)
        if name == "twelvedata":
            api_key = os.getenv("TWELVEDATA_API_KEY")
            if not api_key:
                raise ProviderError("TWELVEDATA_API_KEY is missing")
            return TwelveDataProvider(api_key, retry_config=retry_config, session=session)
        if name == "alphavantage":
            api_key = os.getenv("ALPHAVANTAGE_API_KEY")
            if not api_key:
                raise ProviderError("ALPHAVANTAGE_API_KEY is missing")
            return AlphaVantageProvider(api_key, retry_config=retry_config, session=session)
        if name == "finnhub":
            api_key = os.getenv("FINNHUB_API_KEY")
            if not api_key:
                raise ProviderError("FINNHUB_API_KEY is missing")
            return FinnhubProvider(api_key, retry_config=retry_config, session=session)
        raise ProviderError(f"Unknown provider {name}")

    try:
        primary: HistoryProvider | None = build(provider_name)
    except ProviderError as exc:
        logger.warning(
            f"Provider {provider_name} unavailable: {exc}. Falling back to {fallback_chain}"
        )
        primary = None

    fallback_providers = []
    for fallback in fallback_chain:
        try:
            fallback_providers.append(build(fallback))
        except ProviderError:
            continue

    if primary is None and fallback_providers:
        primary = fallback_providers.pop(0)

    if primary is None:
        raise ProviderError("No usable provider available")
    provider: HistoryProvider
    if fallback_providers:
        provider = FallbackProvider(primary, fallback_providers, logger)
    else:
        provider = primary

    max_requests = budget_cfg.get(provider.name, {}).get("max_requests_per_run", 999999)
    return LimitedProvider(provider, BudgetManager(max_requests))


def run_pipeline(args: argparse.Namespace) -> int:
    root = find_repo_root()
    config_path = resolve_path(root, args.config)
    try:
        overrides: dict[str, Any] = {}
        if args.provider:
            overrides.setdefault("data", {})["provider"] = args.provider
        if args.price_max is not None:
            overrides.setdefault("gates", {})["price_max"] = args.price_max
        if args.min_adv20_dollar is not None:
            overrides.setdefault("gates", {})["min_adv20_dollar"] = args.min_adv20_dollar
        if args.history_min_days is not None:
            overrides.setdefault("staging", {})["history_min_days"] = args.history_min_days
        if args.max_zero_volume_frac is not None:
            overrides.setdefault("gates", {})["max_zero_volume_frac"] = args.max_zero_volume_frac
        if args.outdir:
            overrides.setdefault("paths", {})["outputs_dir"] = args.outdir
        if args.cache_dir:
            overrides.setdefault("paths", {})["cache_dir"] = args.cache_dir
        if args.max_workers is not None:
            overrides.setdefault("data", {})["max_workers"] = args.max_workers

        config_result = load_config(config_path, overrides=overrides)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    config = config_result.config
    config_hash = config_result.config_hash

    logger = get_console_logger(args.log_level)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_timestamp = datetime.now(timezone.utc).isoformat()

    provider = _build_provider(config, logger)

    outputs_dir = resolve_path(root, config["paths"]["outputs_dir"])
    cache_dir = resolve_path(root, config["paths"]["cache_dir"])
    logs_dir = resolve_path(root, config["paths"]["logs_dir"])
    outputs_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    json_logger = JsonlLogger(logs_dir / f"run_{run_id}.jsonl")
    run_meta = {
        "run_id": run_id,
        "run_timestamp_utc": run_timestamp,
        "config_hash": config_hash,
        "provider_name": provider.name,
    }

    if args.mode == "watchlist":
        watchlist_path = resolve_path(root, args.watchlist or config["paths"]["watchlist_file"])
        universe_df = read_watchlist(watchlist_path)
        if universe_df.empty:
            logger.error(f"Watchlist is empty or missing at {watchlist_path}.")
            return 3
    else:
        universe_df = fetch_universe()
        write_universe_csv(universe_df, resolve_path(root, config["paths"]["universe_csv"]))

    universe_df = filter_universe(
        universe_df,
        config["universe"]["allowed_security_types"],
        config["universe"]["allowed_currencies"],
        config["universe"]["include_etfs"],
    )

    if args.mode == "themed":
        themes = [t.strip() for t in (args.themes or "").split(",") if t.strip()]
        filtered_rows = []
        for _, row in universe_df.iterrows():
            tags, _ = tag_themes(
                row["symbol"], row.get("name") or row["symbol"], config.get("themes", {})
            )
            if not themes or any(t in tags for t in themes):
                filtered_rows.append(row)
        universe_df = pd.DataFrame(filtered_rows)

    if args.mode == "batch":
        batch_size = args.batch_size or config["run"].get("max_symbols_per_run", 200)
        cursor_file = resolve_path(root, args.batch_cursor_file or config["paths"]["state_file"])
        cursor_file.parent.mkdir(parents=True, exist_ok=True)
        cursor = 0
        if cursor_file.exists():
            try:
                cursor = int(cursor_file.read_text(encoding="utf-8").strip() or 0)
            except ValueError:
                cursor = 0
        start = cursor
        end = min(start + batch_size, len(universe_df))
        universe_df = universe_df.iloc[start:end]
        cursor_file.write_text(str(end), encoding="utf-8")

    stage1_df, stage2_df, stage3_df, summary = stage_pipeline(
        universe_df,
        provider,
        cache_dir,
        config["data"]["max_cache_age_days"],
        config,
        run_meta,
        logger,
    )

    scored = (
        score_frame(stage3_df, config["score"]["weights"]) if not stage3_df.empty else stage3_df
    )

    features_path = outputs_dir / f"features_{run_id}.csv"
    scored_path = outputs_dir / f"scored_{run_id}.csv"
    eligible_path = outputs_dir / f"eligible_{run_id}.csv"
    report_path = outputs_dir / f"report_{run_id}.md"

    write_csv(scored, scored_path, SCORED_COLUMNS)
    write_csv(scored, features_path, FEATURE_COLUMNS)
    eligible = (
        scored[["symbol", "name", "eligible", "gate_fail_codes", "notes"]]
        if not scored.empty
        else pd.DataFrame(columns=ELIGIBLE_COLUMNS)
    )
    write_csv(eligible, eligible_path, ELIGIBLE_COLUMNS)

    write_report(report_path, summary, scored)

    json_logger.log("summary", {"counts": summary})
    logger.info(f"Outputs written to {outputs_dir}")
    return 0


def run_bulk_plan(args: argparse.Namespace) -> int:
    root = find_repo_root()
    config_path = resolve_path(root, args.config)
    try:
        config = load_config(config_path).config
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    logger = get_console_logger(args.log_level)
    sources = load_bulk_sources(config)
    if args.sources:
        allowed = {name.strip() for name in args.sources.split(",") if name.strip()}
        sources = [src for src in sources if src.name in allowed]

    symbols = _load_bulk_symbols(config, root, args)
    bulk_paths = config.get("bulk", {}).get("paths", {})
    raw_dir = resolve_path(root, bulk_paths.get("raw_dir", "data/raw"))
    manifest_dir = resolve_path(root, bulk_paths.get("manifest_dir", "data/manifests"))

    tasks = build_download_plan(sources, symbols, raw_dir, use_archives=args.use_archives)
    manifest = BulkManifest.create(tasks)
    manifest_path = resolve_path(
        root,
        args.manifest or (manifest_dir / f"bulk_manifest_{manifest.created_at_utc}.json"),
    )
    write_manifest(manifest_path, manifest)
    logger.info(f"[bulk] planned {len(tasks)} tasks -> {manifest_path}")
    return 0


def run_bulk_download(args: argparse.Namespace) -> int:
    root = find_repo_root()
    config_path = resolve_path(root, args.config)
    try:
        config = load_config(config_path).config
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    logger = get_console_logger(args.log_level)
    sources = load_bulk_sources(config)
    if args.sources:
        allowed = {name.strip() for name in args.sources.split(",") if name.strip()}
        sources = [src for src in sources if src.name in allowed]

    symbols = _load_bulk_symbols(config, root, args)
    bulk_paths = config.get("bulk", {}).get("paths", {})
    raw_dir = resolve_path(root, bulk_paths.get("raw_dir", "data/raw"))
    manifest_dir = resolve_path(root, bulk_paths.get("manifest_dir", "data/manifests"))

    tasks = build_download_plan(sources, symbols, raw_dir, use_archives=args.use_archives)
    manifest = BulkManifest.create(tasks)
    manifest_path = resolve_path(
        root,
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
    root = find_repo_root()
    config_path = resolve_path(root, args.config)
    try:
        config = load_config(config_path).config
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    logger = get_console_logger(args.log_level)
    bulk_paths = config.get("bulk", {}).get("paths", {})
    raw_dir = resolve_path(root, bulk_paths.get("raw_dir", "data/raw"))
    curated_dir = resolve_path(root, bulk_paths.get("curated_dir", "data/curated"))

    input_dir = resolve_path(root, args.input_dir) if args.input_dir else raw_dir / args.source
    output_dir = resolve_path(root, args.output_dir) if args.output_dir else curated_dir / args.source

    results = standardize_directory(
        input_dir,
        output_dir,
        mode=args.mode,
        value_column=args.value_column,
    )
    total_rows = sum(result.rows for result in results)
    logger.info(f"[bulk] standardized {len(results)} files -> {output_dir} ({total_rows} rows)")
    return 0


def _load_bulk_symbols(config: dict[str, Any], root: Path, args: argparse.Namespace) -> pd.Series:
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

    watchlist_path = resolve_path(root, args.watchlist or config["paths"]["watchlist_file"])
    universe_df = read_watchlist(watchlist_path)
    return universe_df["symbol"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("market_monitor")
    sub = parser.add_subparsers(dest="command", required=True)

    doctor_parser = sub.add_parser("doctor", help="Run diagnostics")
    doctor_parser.add_argument("--config", default="config.json")
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

    run_parser = sub.add_parser("run", help="Run the monitor")
    run_parser.add_argument("--config", default="config.json")
    run_parser.add_argument(
        "--mode", choices=["universe", "watchlist", "themed", "batch"], default="watchlist"
    )
    run_parser.add_argument("--watchlist")
    run_parser.add_argument("--themes")
    run_parser.add_argument("--provider")
    run_parser.add_argument("--price-max", type=float)
    run_parser.add_argument("--min-adv20-dollar", type=float)
    run_parser.add_argument("--history-min-days", type=int)
    run_parser.add_argument("--max-zero-volume-frac", type=float)
    run_parser.add_argument("--outdir")
    run_parser.add_argument("--cache-dir")
    run_parser.add_argument("--batch-size", type=int)
    run_parser.add_argument("--batch-cursor-file")
    run_parser.add_argument("--max-workers", type=int)
    run_parser.add_argument("--log-level", default="INFO")

    bulk_plan_parser = sub.add_parser("bulk-plan", help="Plan bulk CSV downloads")
    bulk_plan_parser.add_argument("--config", default="config.json")
    bulk_plan_parser.add_argument("--mode", choices=["watchlist", "universe"], default="watchlist")
    bulk_plan_parser.add_argument("--watchlist")
    bulk_plan_parser.add_argument("--sources")
    bulk_plan_parser.add_argument("--use-archives", action="store_true")
    bulk_plan_parser.add_argument("--manifest")
    bulk_plan_parser.add_argument("--log-level", default="INFO")

    bulk_download_parser = sub.add_parser("bulk-download", help="Download bulk CSV data")
    bulk_download_parser.add_argument("--config", default="config.json")
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
    bulk_standardize_parser.add_argument("--config", default="config.json")
    bulk_standardize_parser.add_argument("--source", required=True)
    bulk_standardize_parser.add_argument("--mode", choices=["ohlcv", "timeseries"], required=True)
    bulk_standardize_parser.add_argument("--input-dir")
    bulk_standardize_parser.add_argument("--output-dir")
    bulk_standardize_parser.add_argument("--value-column")
    bulk_standardize_parser.add_argument("--log-level", default="INFO")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

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
        return run_pipeline(args)

    if args.command == "bulk-plan":
        return run_bulk_plan(args)

    if args.command == "bulk-download":
        return run_bulk_download(args)

    if args.command == "bulk-standardize":
        return run_bulk_standardize(args)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
