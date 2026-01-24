from __future__ import annotations

import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

if importlib.util.find_spec("importlib.metadata") is None:
    import importlib_metadata as importlib_metadata
else:
    from importlib import metadata as importlib_metadata

from market_monitor.bulk import load_bulk_sources
from market_monitor.config_schema import ConfigError, load_config
from market_monitor.data_paths import resolve_corpus_paths, resolve_data_paths
from market_monitor.paths import find_repo_root, resolve_path
from market_monitor.offline import set_offline_mode
from market_monitor.providers.nasdaq_daily import NasdaqDailyProvider, NasdaqDailySource
from market_monitor.universe import read_watchlist
from market_monitor.providers.base import HistoryProvider, ProviderError
from market_monitor.providers.http import RetryConfig, request_with_backoff


@dataclass
class DoctorMessage:
    level: str
    title: str
    detail: str
    fix_steps: list[str]


@dataclass
class ConnectivityResult:
    name: str
    url: str
    status: str
    detail: str


def run_doctor(config_path: Path, *, offline: bool = False, strict: bool = False) -> int:
    print("[doctor] Market Monitor diagnostics")
    messages: list[DoctorMessage] = []
    config_path = config_path.resolve()
    base_dir = config_path.parent
    root = find_repo_root(base_dir)
    offline_flag = os.getenv("OFFLINE_MODE")
    offline = (
        offline
        or os.getenv("MM_OFFLINE") == "1"
        or os.getenv("MARKET_MONITOR_OFFLINE") == "1"
        or (offline_flag is not None and offline_flag.lower() in {"1", "true", "yes"})
    )
    strict = strict or os.getenv("MM_STRICT_CONNECTIVITY") == "1"

    if root != Path.cwd():
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Not running from repo root",
                detail=f"Current directory is {Path.cwd()}, but repo root appears to be {root}.",
                fix_steps=[f'cd "{root}"', "Re-run the doctor command from the repo root."],
            )
        )

    if sys.version_info < (3, 10):  # noqa: UP036
        messages.append(
            DoctorMessage(
                level="ERROR",
                title="Python version too old",
                detail=f"Detected Python {sys.version_info.major}.{sys.version_info.minor}. Market Monitor requires Python 3.10+.",
                fix_steps=["Install Python 3.11.", "Recreate the venv and rerun doctor."],
            )
        )
        _print_messages(messages, root / "outputs" / "logs")
        return 2

    try:
        result = load_config(config_path)
    except ConfigError as exc:
        messages.append(
            DoctorMessage(
                level="ERROR",
                title="Config error",
                detail=str(exc),
                fix_steps=[
                    f"Run: python -m market_monitor init-config --out {config_path}",
                    "Edit the config file to match your environment.",
                ],
            )
        )
        _print_messages(messages, root / "outputs" / "logs")
        return 2

    config = result.config
    offline = offline or config["data"].get("offline_mode", False)
    set_offline_mode(bool(offline))
    logs_dir = resolve_path(base_dir, config["paths"]["logs_dir"])

    watchlist_path = resolve_path(base_dir, config["paths"]["watchlist_file"])
    if not watchlist_path.exists():
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Watchlist file missing",
                detail=f"Expected watchlist at {watchlist_path}, but it does not exist.",
                fix_steps=[
                    "Create inputs/watchlist.txt with one symbol per line.",
                    "Or run with --mode universe to fetch the full universe.",
                ],
            )
        )

    outputs_dir = resolve_path(base_dir, config["paths"]["outputs_dir"])
    outputs_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = resolve_path(base_dir, config["paths"]["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)

    logs_dir.mkdir(parents=True, exist_ok=True)

    _print_data_directories(config, base_dir, outputs_dir, logs_dir, cache_dir)
    _print_runtime_info()
    _print_symbol_coverage(config, base_dir, messages)
    _check_env_vars(config, messages)
    _check_external_data_paths(config, base_dir, messages)
    _check_gate_sanity(config, messages)
    provider_status, provider_detail = _check_provider_health(
        config, messages, offline=offline, strict=strict
    )
    _print_provider_status(config["data"]["provider"], provider_status, provider_detail)

    bulk_results = _check_bulk_sources(config, messages, offline=offline, strict=strict)
    _print_bulk_results(bulk_results, offline=offline)

    cache_stats = _read_cache_stats(logs_dir)
    _print_cache_stats(cache_stats)

    _print_messages(messages, logs_dir)

    has_errors = any(msg.level == "ERROR" for msg in messages)
    return 2 if has_errors else 0


def _check_env_vars(config, messages: list[DoctorMessage]) -> None:
    provider = config["data"]["provider"]
    offline_mode = config["data"].get("offline_mode", False)
    fallbacks = config["data"].get("fallback_chain", [])
    mapping = {
        "twelvedata": "TWELVEDATA_API_KEY",
        "alphavantage": "ALPHAVANTAGE_API_KEY",
        "finnhub": "FINNHUB_API_KEY",
    }
    for provider_name, env_var in mapping.items():
        if offline_mode:
            continue
        if provider_name == provider and not os.getenv(env_var):
            messages.append(
                DoctorMessage(
                    level="ERROR",
                    title="Missing API key",
                    detail=f"{env_var} is required for provider '{provider_name}' but is not set.",
                    fix_steps=[
                        f"Set {env_var} in your environment (or .env locally).",
                        "Re-run doctor to confirm the key is detected.",
                    ],
                )
            )
        elif provider_name in fallbacks and not os.getenv(env_var):
            messages.append(
                DoctorMessage(
                    level="WARN",
                    title="Missing fallback API key",
                    detail=f"{env_var} is not set for fallback provider '{provider_name}'.",
                    fix_steps=[
                        f"Set {env_var} if you want {provider_name} as a fallback.",
                        "Otherwise remove it from fallback_chain in config.yaml.",
                    ],
                )
            )


def _check_external_data_paths(config, base_dir: Path, messages: list[DoctorMessage]) -> None:
    paths = resolve_data_paths(config, base_dir)
    corpus_paths = resolve_corpus_paths(config, base_dir)
    if config["data"].get("offline_mode", False):
        if not paths.nasdaq_daily_dir or not paths.nasdaq_daily_dir.exists():
            messages.append(
                DoctorMessage(
                    level="ERROR",
                    title="NASDAQ daily dataset missing",
                    detail=(
                        "Offline mode is enabled but the NASDAQ daily dataset path is not configured or missing."
                    ),
                    fix_steps=[
                        "Set MARKET_APP_NASDAQ_DAILY_DIR in config.yaml or environment.",
                        "Ensure the folder contains per-ticker CSVs.",
                    ],
                )
            )
    if paths.silver_prices_dir and not paths.silver_prices_dir.exists():
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Silver dataset missing",
                detail=f"Silver dataset not found at {paths.silver_prices_dir}.",
                fix_steps=[
                    "Verify MARKET_APP_SILVER_PRICES_DIR path in config.yaml or env.",
                    "Leave unset if you do not want silver macro features.",
                ],
            )
        )
    if corpus_paths.gdelt_conflict_dir and not corpus_paths.gdelt_conflict_dir.exists():
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Corpus directory missing",
                detail=f"Corpus directory not found at {corpus_paths.gdelt_conflict_dir}.",
                fix_steps=[
                    "Verify MARKET_APP_GDELT_CONFLICT_DIR in config.yaml or env.",
                    "Leave unset if you do not want corpus enrichment.",
                ],
            )
        )
    if corpus_paths.gdelt_events_raw_dir and not corpus_paths.gdelt_events_raw_dir.exists():
        messages.append(
            DoctorMessage(
                level="WARN",
                title="GDELT raw events directory missing",
                detail=f"Raw events directory not found at {corpus_paths.gdelt_events_raw_dir}.",
                fix_steps=[
                    "Create corpus/gdelt_events_raw and drop manual GDELT event zip files there.",
                    "Leave empty if you do not need post-2021 updates.",
                ],
            )
        )


def _check_gate_sanity(config, messages: list[DoctorMessage]) -> None:
    gates = config["gates"]
    price_min = gates.get("price_min")
    price_max = gates.get("price_max")
    if price_min is not None and price_min < 1:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Price floor may be too strict",
                detail=f"price_min is {price_min}, which may exclude most symbols.",
                fix_steps=[
                    "Consider lowering price_min or setting it to null.",
                ],
            )
        )
    if price_max is not None and price_max < 1:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Price ceiling may be too strict",
                detail=f"price_max is {price_max}, which may exclude most symbols.",
                fix_steps=[
                    "Consider raising price_max or setting it to null.",
                ],
            )
        )


def _check_provider_health(
    config, messages: list[DoctorMessage], *, offline: bool, strict: bool
) -> tuple[str, str]:
    if offline:
        return "SKIPPED", "Offline mode enabled."

    throttling = config["data"].get("throttling", {})
    retry_cfg = RetryConfig(
        max_retries=int(throttling.get("max_retries", 2)),
        base_delay_s=float(throttling.get("base_delay_s", 0.3)),
        jitter_s=float(throttling.get("jitter_s", 0.2)),
    )

    provider_name = config["data"]["provider"]
    provider: HistoryProvider | None = None
    if provider_name == "stooq":
        from market_monitor.providers.stooq import StooqProvider

        provider = StooqProvider(retry_config=retry_cfg)
    elif provider_name == "twelvedata":
        from market_monitor.providers.twelvedata import TwelveDataProvider

        api_key = os.getenv("TWELVEDATA_API_KEY")
        if api_key:
            provider = TwelveDataProvider(api_key, retry_config=retry_cfg)
    elif provider_name == "alphavantage":
        from market_monitor.providers.alphavantage import AlphaVantageProvider

        api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if api_key:
            provider = AlphaVantageProvider(api_key, retry_config=retry_cfg)
    elif provider_name == "finnhub":
        from market_monitor.providers.finnhub import FinnhubProvider

        api_key = os.getenv("FINNHUB_API_KEY")
        if api_key:
            provider = FinnhubProvider(api_key, retry_config=retry_cfg)

    if provider is None:
        return "SKIPPED", "Provider not initialized (missing API key or unavailable)."

    try:
        provider.get_history("AAPL", 5)
    except ProviderError as exc:
        messages.append(
            DoctorMessage(
                level="ERROR" if strict else "WARN",
                title="Provider health check failed",
                detail=f"{provider_name} returned an error during a short history check: {exc}.",
                fix_steps=[
                    "Check your API key and account entitlements.",
                    "Wait a minute and retry if you hit a rate limit.",
                    "Switch provider in config.yaml if the issue persists.",
                ],
            )
        )
        return "WARN", f"Provider error: {exc}"
    except requests.RequestException as exc:
        messages.append(
            DoctorMessage(
                level="ERROR" if strict else "WARN",
                title="Provider network check failed",
                detail=f"Network error while contacting {provider_name}: {exc}.",
                fix_steps=[
                    "Confirm your internet connection.",
                    "Retry after a brief pause in case of transient errors.",
                ],
            )
        )
        return "WARN", f"Network error: {exc}"

    return "OK", "History check succeeded."


def _print_data_directories(
    config: dict[str, object],
    base_dir: Path,
    outputs_dir: Path,
    logs_dir: Path,
    cache_dir: Path,
) -> None:
    paths_cfg = config["paths"]
    data_paths = resolve_data_paths(config, base_dir)
    bulk_paths = config.get("bulk", {}).get("paths", {})
    raw_dir = resolve_path(base_dir, bulk_paths.get("raw_dir", "data/raw"))
    curated_dir = resolve_path(base_dir, bulk_paths.get("curated_dir", "data/curated"))
    manifest_dir = resolve_path(base_dir, bulk_paths.get("manifest_dir", "data/manifests"))
    corpus_paths = resolve_corpus_paths(config, base_dir)

    print("[doctor] Data directories")
    print(f"  offline_mode: {config.get('data', {}).get('offline_mode', False)}")
    print(f"  outputs_dir: {outputs_dir}")
    print(f"  logs_dir: {logs_dir}")
    print(f"  cache_dir: {cache_dir}")
    print(f"  watchlist_file: {resolve_path(base_dir, paths_cfg['watchlist_file'])}")
    print(f"  universe_csv: {resolve_path(base_dir, paths_cfg['universe_csv'])}")
    print(f"  state_file: {resolve_path(base_dir, paths_cfg['state_file'])}")
    print(f"  market_app_data_root: {data_paths.market_app_data_root}")
    print(f"  nasdaq_daily_dir: {data_paths.nasdaq_daily_dir}")
    print(f"  silver_prices_dir: {data_paths.silver_prices_dir}")
    print(f"  corpus_root_dir: {corpus_paths.root_dir}")
    print(f"  gdelt_conflict_dir: {corpus_paths.gdelt_conflict_dir}")
    print(f"  gdelt_events_raw_dir: {corpus_paths.gdelt_events_raw_dir}")
    print(f"  bulk_raw_dir: {raw_dir}")
    print(f"  bulk_curated_dir: {curated_dir}")
    print(f"  bulk_manifest_dir: {manifest_dir}")


def _print_provider_status(provider_name: str, status: str, detail: str) -> None:
    print(f"[doctor] Provider reachability ({provider_name}): {status} - {detail}")


def _check_bulk_sources(
    config: dict[str, object],
    messages: list[DoctorMessage],
    *,
    offline: bool,
    strict: bool,
) -> list[ConnectivityResult]:
    sources = load_bulk_sources(config)
    if not sources:
        return []

    throttling = config.get("data", {}).get("throttling", {})
    retry_cfg = RetryConfig(
        max_retries=int(throttling.get("max_retries", 1)),
        base_delay_s=float(throttling.get("base_delay_s", 0.3)),
        jitter_s=float(throttling.get("jitter_s", 0.2)),
    )
    import requests

    session = requests.Session()
    results: list[ConnectivityResult] = []

    for source in sources:
        try:
            url = _resolve_bulk_probe_url(source)
        except ValueError as exc:
            results.append(
                ConnectivityResult(
                    name=source.name,
                    url="",
                    status="SKIPPED",
                    detail=str(exc),
                )
            )
            continue

        if offline:
            results.append(
                ConnectivityResult(
                    name=source.name,
                    url=url,
                    status="SKIPPED",
                    detail="Offline mode enabled.",
                )
            )
            continue

        status, detail = _probe_url(url, session, retry_cfg)
        if status != "OK":
            messages.append(
                DoctorMessage(
                    level="ERROR" if strict else "WARN",
                    title="Bulk source reachability failed",
                    detail=f"{source.name} at {url} returned {detail}.",
                    fix_steps=[
                        "Confirm the URL is reachable in your browser.",
                        "Check if the provider has changed the endpoint.",
                        "Retry later if the host is temporarily unavailable.",
                    ],
                )
            )
        results.append(
            ConnectivityResult(
                name=source.name,
                url=url,
                status=status,
                detail=detail,
            )
        )

    return results


def _resolve_bulk_probe_url(source) -> str:
    if source.static_path:
        return source.build_static_url()
    if source.supports_bulk_archive and source.archive_path:
        return source.build_archive_url()
    if source.symbol_template:
        return source.build_symbol_url("AAPL")
    raise ValueError("No URL template available for bulk connectivity check.")


def _probe_url(url: str, session: requests.Session, retry_cfg: RetryConfig) -> tuple[str, str]:
    headers = {"User-Agent": "market-monitor-doctor/1.0"}
    response: requests.Response | None = None
    try:
        response = session.head(url, allow_redirects=True, timeout=15, headers=headers)
        if response.status_code >= 400:
            raise requests.RequestException(f"HTTP {response.status_code}")
    except requests.RequestException:
        response = request_with_backoff(
            url,
            session=session,
            retry=retry_cfg,
            timeout=15,
            headers={**headers, "Range": "bytes=0-0"},
            stream=True,
        )

    status_code = response.status_code
    detail = _format_probe_detail(response)
    response.close()

    if 200 <= status_code < 400:
        return "OK", detail
    return "WARN", detail


def _format_probe_detail(response: requests.Response) -> str:
    status_code = response.status_code
    size = response.headers.get("Content-Length")
    last_modified = response.headers.get("Last-Modified")
    extras = []
    if size:
        extras.append(f"size={size}")
    if last_modified:
        extras.append(f"last-modified={last_modified}")
    extra_text = f" ({', '.join(extras)})" if extras else ""
    return f"HTTP {status_code}{extra_text}"


def _print_bulk_results(results: list[ConnectivityResult], *, offline: bool) -> None:
    if not results:
        print("[doctor] Bulk source reachability: none configured.")
        return
    if offline:
        print("[doctor] Bulk source reachability: skipped (offline mode).")
        return
    print("[doctor] Bulk source reachability")
    for result in results:
        print(f"  - {result.name}: {result.status} - {result.detail} ({result.url})")


def _read_cache_stats(logs_dir: Path) -> dict[str, object] | None:
    if not logs_dir.exists():
        return None

    log_files = sorted(
        logs_dir.glob("run_*.jsonl"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in log_files:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in reversed(lines):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("event") != "summary":
                continue
            counts = record.get("counts", {})
            hits = counts.get("cache_hits")
            misses = counts.get("cache_misses")
            if hits is None or misses is None:
                continue
            total = hits + misses
            rate = hits / total if total else None
            return {"hits": hits, "misses": misses, "rate": rate, "log": path}
    return None


def _print_cache_stats(cache_stats: dict[str, object] | None) -> None:
    if cache_stats is None:
        print("[doctor] Cache hit rate: unavailable (no run metrics yet).")
        return
    hits = cache_stats["hits"]
    misses = cache_stats["misses"]
    rate = cache_stats["rate"]
    rate_pct = f"{rate:.1%}" if isinstance(rate, float) else "n/a"
    print(f"[doctor] Cache hit rate: {rate_pct} (hits={hits}, misses={misses})")


def _print_runtime_info() -> None:
    print("[doctor] Runtime environment")
    print(f"  python: {sys.version.split()[0]}")
    for pkg in ("pandas", "numpy", "requests"):
        try:
            version = importlib_metadata.version(pkg)
        except importlib_metadata.PackageNotFoundError:
            version = "not installed"
        print(f"  {pkg}: {version}")


def _print_symbol_coverage(config: dict[str, object], base_dir: Path, messages: list[DoctorMessage]) -> None:
    if not config["data"].get("offline_mode", False):
        return

    data_paths = resolve_data_paths(config, base_dir)
    if not data_paths.nasdaq_daily_dir or not data_paths.nasdaq_daily_dir.exists():
        return

    watchlist_path = resolve_path(base_dir, config["paths"]["watchlist_file"])
    if not watchlist_path.exists():
        return

    watchlist = read_watchlist(watchlist_path)
    provider = NasdaqDailyProvider(
        NasdaqDailySource(
            directory=data_paths.nasdaq_daily_dir,
            cache_dir=resolve_path(base_dir, config["paths"]["cache_dir"]),
        )
    )
    found = 0
    missing = 0
    for symbol in watchlist["symbol"].tolist():
        if provider.resolve_symbol_file(symbol):
            found += 1
        else:
            missing += 1

    print("[doctor] Offline symbol coverage")
    print(f"  watchlist symbols: {len(watchlist)}")
    print(f"  found: {found}")
    print(f"  missing: {missing}")
    if missing:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Watchlist symbols missing from NASDAQ daily dataset",
                detail=f"{missing} symbols are missing from {data_paths.nasdaq_daily_dir}.",
                fix_steps=[
                    "Confirm the symbol CSVs exist in the NASDAQ daily folder.",
                    "Check for symbol naming mismatches (dash vs dot).",
                ],
            )
        )


def _print_messages(messages: list[DoctorMessage], logs_dir: Path) -> None:
    logs_hint = f"{logs_dir}"
    if not messages:
        print("[doctor] OK: No blocking issues found.")
        return

    for msg in messages:
        tag = "error" if msg.level == "ERROR" else "warn"
        print(f"[{tag}] {msg.title}")
        print(f"  why: {msg.detail}")
        print("  fix:")
        for step in msg.fix_steps:
            print(f"   - {step}")
        print(f"  logs: {logs_hint}")
