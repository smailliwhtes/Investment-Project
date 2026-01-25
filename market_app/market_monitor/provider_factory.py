from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from market_monitor.data_paths import resolve_data_paths
from market_monitor.paths import resolve_path
from market_monitor.providers.base import (
    BudgetManager,
    HistoryProvider,
    ProviderAccessError,
    ProviderError,
)
from market_monitor.providers.http import RetryConfig
from market_monitor.providers.nasdaq_daily import NasdaqDailyProvider, NasdaqDailySource


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


def build_provider(config: dict[str, Any], logger, base_dir: Path) -> HistoryProvider:
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
