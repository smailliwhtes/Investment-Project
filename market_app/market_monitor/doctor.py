from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import requests

from market_monitor.config_schema import ConfigError, load_config
from market_monitor.paths import find_repo_root, resolve_path
from market_monitor.providers import (
    AlphaVantageProvider,
    FinnhubProvider,
    StooqProvider,
    TwelveDataProvider,
)
from market_monitor.providers.base import HistoryProvider, ProviderError
from market_monitor.providers.http import RetryConfig


@dataclass
class DoctorMessage:
    level: str
    title: str
    detail: str
    fix_steps: list[str]


def run_doctor(config_path: Path) -> int:
    print("[doctor] Market Monitor diagnostics")
    messages: list[DoctorMessage] = []
    root = find_repo_root()

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
    logs_dir = resolve_path(root, config["paths"]["logs_dir"])

    watchlist_path = resolve_path(root, config["paths"]["watchlist_file"])
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

    outputs_dir = resolve_path(root, config["paths"]["outputs_dir"])
    outputs_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = resolve_path(root, config["paths"]["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)

    logs_dir.mkdir(parents=True, exist_ok=True)

    _check_env_vars(config, messages)
    _check_gate_sanity(config, messages)
    _check_provider_health(config, messages)

    _print_messages(messages, logs_dir)

    has_errors = any(msg.level == "ERROR" for msg in messages)
    return 2 if has_errors else 0


def _check_env_vars(config, messages: list[DoctorMessage]) -> None:
    provider = config["data"]["provider"]
    fallbacks = config["data"].get("fallback_chain", [])
    required = set([provider] + fallbacks)
    mapping = {
        "twelvedata": "TWELVEDATA_API_KEY",
        "alphavantage": "ALPHAVANTAGE_API_KEY",
        "finnhub": "FINNHUB_API_KEY",
    }
    for provider_name, env_var in mapping.items():
        if provider_name in required and not os.getenv(env_var):
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


def _check_gate_sanity(config, messages: list[DoctorMessage]) -> None:
    gates = config["gates"]
    if gates["price_max"] < 1:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Price gate may be too strict",
                detail=f"price_max is {gates['price_max']}, which may exclude most symbols.",
                fix_steps=[
                    "Consider raising price_max (e.g., 5.0 or 10.0).",
                    "Run a small watchlist scan to validate eligibility.",
                ],
            )
        )
    if gates["min_adv20_dollar"] > 50_000_000:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Liquidity gate may be too strict",
                detail=f"min_adv20_dollar is {gates['min_adv20_dollar']:,}, which may filter everything.",
                fix_steps=[
                    "Consider lowering min_adv20_dollar for smaller-cap coverage.",
                    "Run a watchlist scan to validate eligibility.",
                ],
            )
        )
    if gates["max_zero_volume_frac"] < 0.01:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Zero-volume gate may be too strict",
                detail=f"max_zero_volume_frac is {gates['max_zero_volume_frac']}, which may filter thinly traded symbols.",
                fix_steps=[
                    "Consider raising max_zero_volume_frac (e.g., 0.05 to 0.10).",
                    "Run a watchlist scan to validate eligibility.",
                ],
            )
        )


def _check_provider_health(config, messages: list[DoctorMessage]) -> None:
    throttling = config["data"].get("throttling", {})
    retry_cfg = RetryConfig(
        max_retries=int(throttling.get("max_retries", 2)),
        base_delay_s=float(throttling.get("base_delay_s", 0.3)),
        jitter_s=float(throttling.get("jitter_s", 0.2)),
    )

    provider_name = config["data"]["provider"]
    provider: HistoryProvider | None = None
    if provider_name == "stooq":
        provider = StooqProvider(retry_config=retry_cfg)
    elif provider_name == "twelvedata":
        api_key = os.getenv("TWELVEDATA_API_KEY")
        if api_key:
            provider = TwelveDataProvider(api_key, retry_config=retry_cfg)
    elif provider_name == "alphavantage":
        api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        if api_key:
            provider = AlphaVantageProvider(api_key, retry_config=retry_cfg)
    elif provider_name == "finnhub":
        api_key = os.getenv("FINNHUB_API_KEY")
        if api_key:
            provider = FinnhubProvider(api_key, retry_config=retry_cfg)

    if provider is None:
        return

    try:
        provider.get_history("AAPL", 5)
    except ProviderError as exc:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Provider health check failed",
                detail=f"{provider_name} returned an error during a short history check: {exc}.",
                fix_steps=[
                    "Check your API key and account entitlements.",
                    "Wait a minute and retry if you hit a rate limit.",
                    "Switch provider in config.json if the issue persists.",
                ],
            )
        )
    except requests.RequestException as exc:
        messages.append(
            DoctorMessage(
                level="WARN",
                title="Provider network check failed",
                detail=f"Network error while contacting {provider_name}: {exc}.",
                fix_steps=[
                    "Confirm your internet connection.",
                    "Retry after a brief pause in case of transient errors.",
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
