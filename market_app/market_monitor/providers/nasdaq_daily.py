from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from market_monitor.cache import CacheResult
from market_monitor.providers.base import HistoryProvider, ProviderCapabilities, ProviderError


@dataclass(frozen=True)
class NasdaqDailySource:
    directory: Path
    cache_dir: Path


class NasdaqDailyProvider(HistoryProvider):
    name = "nasdaq_daily"
    capabilities = ProviderCapabilities(True, False, False, "offline")

    def __init__(self, source: NasdaqDailySource) -> None:
        self.source = source

    def get_history_with_cache(
        self,
        symbol: str,
        days: int,
        *,
        max_cache_age_days: float,
    ) -> CacheResult:
        cache_path = self._cache_path(symbol)
        if cache_path.exists():
            df = pd.read_parquet(cache_path)
            data_freshness_days = self._freshness_days(cache_path)
            if data_freshness_days <= max_cache_age_days:
                return CacheResult(df, data_freshness_days, cache_path, True)

        df = self._load_symbol(symbol)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(cache_path, index=False)
        return CacheResult(df, self._freshness_days(cache_path), cache_path, False)

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        cache_path = self._cache_path(symbol)
        if cache_path.exists():
            df = pd.read_parquet(cache_path)
        else:
            df = self._load_symbol(symbol)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(cache_path, index=False)
        if days > 0:
            return df.tail(days).copy()
        return df.copy()

    def _cache_path(self, symbol: str) -> Path:
        safe_symbol = symbol.replace("/", "-").replace("\\", "-")
        return self.source.cache_dir / "nasdaq_daily" / f"{safe_symbol}.parquet"

    def _freshness_days(self, path: Path) -> float:
        delta = pd.Timestamp.utcnow() - pd.Timestamp(path.stat().st_mtime, unit="s")
        return float(delta.total_seconds() / 86400.0)

    def _load_symbol(self, symbol: str) -> pd.DataFrame:
        source_file = self._find_symbol_file(symbol)
        if source_file is None:
            raise ProviderError(f"NASDAQ_DAILY_MISSING:{symbol}")
        df = pd.read_csv(source_file)
        normalized = self._normalize_ohlc(df, symbol)
        return normalized

    def _find_symbol_file(self, symbol: str) -> Path | None:
        candidates = [
            self.source.directory / f"{symbol}.csv",
            self.source.directory / f"{symbol.upper()}.csv",
            self.source.directory / f"{symbol.lower()}.csv",
            self.source.directory / f"{symbol.upper()}.CSV",
            self.source.directory / f"{symbol.lower()}.CSV",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _normalize_ohlc(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        cols = {c.lower(): c for c in df.columns}
        required = ["date", "open", "high", "low", "close"]
        missing = [c for c in required if c not in cols]
        if missing:
            raise ProviderError(f"NASDAQ_DAILY_SCHEMA_MISSING:{symbol}:{','.join(missing)}")

        normalized = pd.DataFrame(
            {
                "Date": pd.to_datetime(df[cols["date"]], errors="coerce"),
                "Open": pd.to_numeric(df[cols["open"]], errors="coerce"),
                "High": pd.to_numeric(df[cols["high"]], errors="coerce"),
                "Low": pd.to_numeric(df[cols["low"]], errors="coerce"),
                "Close": pd.to_numeric(df[cols["close"]], errors="coerce"),
            }
        )
        normalized = normalized.dropna(subset=["Date"]).sort_values("Date")
        normalized = normalized.drop_duplicates(subset=["Date"], keep="last")
        normalized["Volume"] = np.nan
        return normalized.reset_index(drop=True)
