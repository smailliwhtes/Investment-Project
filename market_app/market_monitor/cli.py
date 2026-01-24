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
from market_monitor.logging_utils import JsonlLogger, get_console_logger
from market_monitor.paths import find_repo_root, resolve_path
from market_monitor.providers.base import (
    BudgetManager,
    HistoryProvider,
    ProviderAccessError,
    ProviderError,
)
from market_monitor.providers.nasdaq_daily import NasdaqDailyProvider, NasdaqDailySource
from market_monitor.providers.http import RetryConfig
from market_monitor.macro import load_silver_series
from market_monitor.hash_utils import hash_manifest
from market_monitor.manifest import build_run_manifest, resolve_git_commit, run_id_from_inputs
from market_monitor.offline import set_offline_mode
from market_monitor.preflight import run_preflight
from market_monitor.prediction import build_panel, latest_predictions, train_and_predict
from market_monitor.evaluate import run_evaluation
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

    def load_symbol_data(self, symbol: str):
        if not hasattr(self.provider, "load_symbol_data"):
            raise ProviderError("Provider does not support load_symbol_data.")
        return self.provider.load_symbol_data(symbol)

    def resolve_symbol_file(self, symbol: str):
        if hasattr(self.provider, "resolve_symbol_file"):
            return self.provider.resolve_symbol_file(symbol)
        return None


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


def _build_provider(config: dict[str, Any], logger, base_dir: Path) -> HistoryProvider:
    provider_name = config["data"]["provider"]
    offline_mode = config["data"].get("offline_mode", False)
    if offline_mode and provider_name != "nasdaq_daily":
        logger.warning("Offline mode enabled; forcing provider to nasdaq_daily.")
        provider_name = "nasdaq_daily"
    budget_cfg = config["data"].get("budget", {})
    fallback_chain = config["data"].get("fallback_chain", [])
    throttling_cfg = config["data"].get("throttling", {})
    retry_config = RetryConfig(
        max_retries=int(throttling_cfg.get("max_retries", 3)),
        base_delay_s=float(throttling_cfg.get("base_delay_s", 0.3)),
        jitter_s=float(throttling_cfg.get("jitter_s", 0.2)),
    )
    sleep_ms = int(float(throttling_cfg.get("base_delay_s", 0.3)) * 1000)

    def build(name: str) -> HistoryProvider:
        if name == "nasdaq_daily":
            paths = resolve_data_paths(config, base_dir)
            if not paths.nasdaq_daily_dir:
                raise ProviderError("MARKET_APP_NASDAQ_DAILY_DIR is not configured.")
            cache_dir = resolve_path(base_dir, config["paths"]["cache_dir"])
            return NasdaqDailyProvider(
                NasdaqDailySource(directory=paths.nasdaq_daily_dir, cache_dir=cache_dir)
            )
        if name == "stooq":
            from market_monitor.providers.stooq import StooqProvider
            import requests

            session = requests.Session()
            return StooqProvider(sleep_ms=sleep_ms, retry_config=retry_config, session=session)
        if name == "twelvedata":
            from market_monitor.providers.twelvedata import TwelveDataProvider
            import requests

            api_key = os.getenv("TWELVEDATA_API_KEY")
            if not api_key:
                raise ProviderError("TWELVEDATA_API_KEY is missing")
            session = requests.Session()
            return TwelveDataProvider(api_key, retry_config=retry_config, session=session)
        if name == "alphavantage":
            from market_monitor.providers.alphavantage import AlphaVantageProvider
            import requests

            api_key = os.getenv("ALPHAVANTAGE_API_KEY")
            if not api_key:
                raise ProviderError("ALPHAVANTAGE_API_KEY is missing")
            session = requests.Session()
            return AlphaVantageProvider(api_key, retry_config=retry_config, session=session)
        if name == "finnhub":
            from market_monitor.providers.finnhub import FinnhubProvider
            import requests

            api_key = os.getenv("FINNHUB_API_KEY")
            if not api_key:
                raise ProviderError("FINNHUB_API_KEY is missing")
            session = requests.Session()
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
    if offline_mode:
        fallback_chain = []
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

    run_start = datetime.now(timezone.utc)
    run_timestamp = run_start.isoformat()

    provider = _build_provider(config, logger, base_dir)

    if config["data"].get("offline_mode", False) and args.mode != "watchlist":
        logger.error("Offline mode is enabled; only --mode watchlist is supported.")
        return 3

    watchlist_path: Path | None = None
    if args.mode == "watchlist":
        watchlist_path = resolve_path(base_dir, args.watchlist or config["paths"]["watchlist_file"])
        universe_df = read_watchlist(watchlist_path)
        if universe_df.empty:
            logger.error(f"Watchlist is empty or missing at {watchlist_path}.")
            return 3
    else:
        universe_df = fetch_universe()
        write_universe_csv(universe_df, resolve_path(base_dir, config["paths"]["universe_csv"]))

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
            tags, _, _ = tag_themes(
                row["symbol"], row.get("name") or row["symbol"], config.get("themes", {})
            )
            if not themes or any(t in tags for t in themes):
                filtered_rows.append(row)
        universe_df = pd.DataFrame(filtered_rows)

    if args.mode == "batch":
        batch_size = args.batch_size or config["run"].get("max_symbols_per_run", 200)
        cursor_file = resolve_path(base_dir, args.batch_cursor_file or config["paths"]["state_file"])
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

    corpus_paths = resolve_corpus_paths(config, base_dir)
    corpus_sources = discover_corpus_sources(
        corpus_paths.gdelt_conflict_dir,
        corpus_paths.gdelt_events_raw_dir,
    )
    corpus_manifest = build_corpus_manifest(
        [source.path for source in corpus_sources if source.source_type == "csv"],
        [source.path for source in corpus_sources if source.source_type == "zip"],
    )
    corpus_manifest_hash = None
    if corpus_manifest.get("files") or corpus_manifest.get("raw_event_zips"):
        corpus_manifest_hash = hash_manifest(corpus_manifest)

    run_id = run_id_from_inputs(
        timestamp=run_start,
        config_hash=config_hash,
        watchlist_path=watchlist_path if args.mode == "watchlist" else None,
        watchlist_df=universe_df,
        corpus_manifest_hash=corpus_manifest_hash,
    )

    outputs_dir = resolve_path(base_dir, config["paths"]["outputs_dir"])
    cache_dir = resolve_path(base_dir, config["paths"]["cache_dir"])
    logs_dir = resolve_path(base_dir, config["paths"]["logs_dir"])
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

    paths = resolve_data_paths(config, base_dir)
    silver_macro = None
    if paths.silver_prices_dir:
        macro = load_silver_series(paths.silver_prices_dir)
        if macro:
            silver_macro = macro.features

    preflight = None
    if args.mode == "watchlist":
        preflight = run_preflight(
            universe_df,
            provider,
            outputs_dir,
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

    scored = (
        score_frame(stage3_df, config["score"]["weights"]) if not stage3_df.empty else stage3_df
    )

    corpus_outputs_dir = outputs_dir / "corpus"
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

    features_path = outputs_dir / f"features_{run_id}.csv"
    scored_path = outputs_dir / f"scored_{run_id}.csv"
    eligible_path = outputs_dir / f"eligible_{run_id}.csv"
    report_path = outputs_dir / "run_report.md"
    report_archive = outputs_dir / f"run_report_{run_id}.md"

    write_csv(scored, scored_path, build_scored_columns(context_columns))
    write_csv(scored, features_path, build_feature_columns(context_columns))
    eligible = (
        scored[["symbol", "name", "eligible", "gate_fail_codes", "notes"]]
        if not scored.empty
        else pd.DataFrame(columns=ELIGIBLE_COLUMNS)
    )
    write_csv(eligible, eligible_path, ELIGIBLE_COLUMNS)

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

    prediction_panel = None
    prediction_metrics = None
    if config.get("prediction", {}).get("enabled", False) and not stage3_df.empty:
        panel = build_panel(
            stage3_df["symbol"].tolist(),
            provider,
            config["prediction"]["lookback_days"],
            config["prediction"]["forward_return_days"],
            config["prediction"]["forward_drawdown_days"],
            config["prediction"]["min_history_days"],
            logger,
        )
        artifacts = train_and_predict(
            panel,
            outputs_dir,
            config["prediction"]["drawdown_threshold"],
            config["prediction"]["walk_forward_folds"],
            config["prediction"]["embargo_days"],
        )
        latest = latest_predictions(panel, artifacts)
        predictions_path = outputs_dir / f"predictions_{run_id}.csv"
        latest.to_csv(predictions_path, index=False)
        model_card_path = outputs_dir / "model_card.md"
        model_card_path.write_text(artifacts.model_card, encoding="utf-8")
        prediction_panel = artifacts.predictions.merge(
            panel,
            on=["symbol", "date"],
            how="left",
        )
        prediction_metrics = artifacts.metrics

    write_report(
        report_path,
        summary,
        scored,
        run_id=run_id,
        run_timestamp=run_timestamp,
        data_usage=data_usage,
        prediction_panel=prediction_panel,
        prediction_metrics=prediction_metrics,
        context_summary=_build_context_summary(corpus_run),
    )
    write_report(
        report_archive,
        summary,
        scored,
        run_id=run_id,
        run_timestamp=run_timestamp,
        data_usage=data_usage,
        prediction_panel=prediction_panel,
        prediction_metrics=prediction_metrics,
        context_summary=_build_context_summary(corpus_run),
    )

    run_end = datetime.now(timezone.utc)
    manifest = build_run_manifest(
        run_id=run_id,
        run_start=run_start,
        run_end=run_end,
        config=config,
        config_hash=config_hash,
        watchlist_path=watchlist_path if args.mode == "watchlist" else None,
        watchlist_df=universe_df,
        summary=summary,
        scored=scored,
        preflight=preflight,
        git_commit=resolve_git_commit(repo_root),
        corpus_manifest=corpus_run.manifest if corpus_run else None,
    )
    (outputs_dir / "run_manifest.json").write_text(
        manifest.to_json(indent=2),
        encoding="utf-8",
    )

    json_logger.log("summary", {"counts": summary})
    logger.info(f"Outputs written to {outputs_dir}")
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

    provider = _build_provider(config, logger, base_dir)
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
    provider = _build_provider(config, logger, base_dir)
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
    parser = argparse.ArgumentParser("market_monitor")
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

    run_parser = sub.add_parser("run", help="Run the monitor")
    run_parser.add_argument("--config", default="config.yaml")
    run_parser.add_argument(
        "--mode", choices=["universe", "watchlist", "themed", "batch"], default="watchlist"
    )
    run_parser.add_argument("--watchlist")
    run_parser.add_argument("--themes")
    run_parser.add_argument("--provider")
    run_parser.add_argument("--price-min", type=float)
    run_parser.add_argument("--price-max", type=float)
    run_parser.add_argument("--history-min-days", type=int)
    run_parser.add_argument("--outdir")
    run_parser.add_argument("--cache-dir")
    run_parser.add_argument(
        "--offline",
        action="store_true",
        help="Force offline mode for this run (overrides config).",
    )
    run_parser.add_argument("--batch-size", type=int)
    run_parser.add_argument("--batch-cursor-file")
    run_parser.add_argument("--max-workers", type=int)
    run_parser.add_argument("--log-level", default="INFO")

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

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
