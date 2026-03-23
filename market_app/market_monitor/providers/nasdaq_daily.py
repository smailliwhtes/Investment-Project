from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path

import pandas as pd

from market_monitor.cache import CacheResult
from market_monitor.features.io import read_ohlcv, resolve_ohlcv_path
from market_monitor.hash_utils import hash_file
from market_monitor.providers.base import HistoryProvider, ProviderCapabilities, ProviderError
from market_monitor.timebase import utcnow

logger = logging.getLogger(__name__)


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
        source_file = self._find_symbol_file(symbol)
        if source_file is None:
            raise ProviderError(f"NASDAQ_DAILY_MISSING:{symbol}")
        digest = hash_file(source_file)
        cache_path = self._cache_path(symbol, source_file, digest=digest)
        logger.debug(
            "nasdaq_daily resolve symbol=%s path=%s digest=%s cache=%s",
            symbol,
            source_file,
            digest[:12],
            cache_path,
        )
        if cache_path.exists():
            df = pd.read_parquet(cache_path)
            data_freshness_days = self._freshness_days(cache_path)
            if data_freshness_days <= max_cache_age_days:
                return CacheResult(df, data_freshness_days, cache_path, True)

        df = self._load_symbol(symbol, source_file=source_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        wrote = self._write_cache(df, cache_path)
        freshness = self._freshness_days(cache_path) if wrote else 0.0
        return CacheResult(df, freshness, cache_path, False)

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        source_file = self._find_symbol_file(symbol)
        if source_file is None:
            raise ProviderError(f"NASDAQ_DAILY_MISSING:{symbol}")
        digest = hash_file(source_file)
        cache_path = self._cache_path(symbol, source_file, digest=digest)
        logger.debug(
            "nasdaq_daily resolve symbol=%s path=%s digest=%s cache=%s",
            symbol,
            source_file,
            digest[:12],
            cache_path,
        )
        if cache_path.exists():
            df = pd.read_parquet(cache_path)
        else:
            df = self._load_symbol(symbol, source_file=source_file)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._write_cache(df, cache_path)
        if days > 0:
            return df.tail(days).copy()
        return df.copy()

    def _cache_path(
        self,
        symbol: str,
        source_file: Path | None = None,
        *,
        digest: str | None = None,
    ) -> Path:
        safe_symbol = symbol.replace("/", "-").replace("\\", "-")
        if source_file and source_file.exists():
            digest = (digest or hash_file(source_file))[:12]
            return self.source.cache_dir / "nasdaq_daily" / f"{safe_symbol}_{digest}.parquet"
        return self.source.cache_dir / "nasdaq_daily" / f"{safe_symbol}.parquet"

    def _freshness_days(self, path: Path) -> float:
        delta = pd.Timestamp(utcnow()) - pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
        return float(delta.total_seconds() / 86400.0)

    @staticmethod
    def _write_cache(df: pd.DataFrame, cache_path: Path) -> bool:
        try:
            df.to_parquet(cache_path, index=False)
        except ImportError:
            if cache_path.exists():
                cache_path.unlink(missing_ok=True)
            return False
        return True

    def _load_symbol(self, symbol: str, *, source_file: Path | None = None) -> pd.DataFrame:
        if source_file is not None:
            return self._load_symbol_from_path(symbol, source_file)
        df, _ = self.load_symbol_data(symbol)
        return df

    def resolve_symbol_file(self, symbol: str) -> Path | None:
        return self._find_symbol_file(symbol)

    def load_symbol_data(self, symbol: str) -> tuple[pd.DataFrame, Path]:
        source_file = self._find_symbol_file(symbol)
        if source_file is None:
            raise ProviderError(f"NASDAQ_DAILY_MISSING:{symbol}")
        normalized = self._load_symbol_from_path(symbol, source_file)
        return normalized, source_file

    def _load_symbol_from_path(self, symbol: str, source_file: Path) -> pd.DataFrame:
        normalized = self._normalize_ohlc(source_file, symbol)
        min_date = None
        max_date = None
        if not normalized.empty and "Date" in normalized.columns:
            dates = pd.to_datetime(normalized["Date"], errors="coerce").dropna()
            if not dates.empty:
                min_date = dates.min().strftime("%Y-%m-%d")
                max_date = dates.max().strftime("%Y-%m-%d")
        logger.debug(
            "nasdaq_daily loaded symbol=%s path=%s rows=%s min_date=%s max_date=%s",
            symbol,
            source_file,
            len(normalized),
            min_date,
            max_date,
        )
        return normalized

    def _find_symbol_file(self, symbol: str) -> Path | None:
        return resolve_ohlcv_path(symbol, self.source.directory)

    @staticmethod
    def _normalize_ohlc(source_file: Path, symbol: str) -> pd.DataFrame:
        try:
            df = read_ohlcv(source_file)
        except ValueError as exc:
            raise ProviderError(f"NASDAQ_DAILY_SCHEMA_MISSING:{symbol}:{exc}") from exc

        normalized = pd.DataFrame(
            {
                "Date": pd.to_datetime(df["date"], errors="coerce"),
                "Open": pd.to_numeric(df["open"], errors="coerce"),
                "High": pd.to_numeric(df["high"], errors="coerce"),
                "Low": pd.to_numeric(df["low"], errors="coerce"),
                "Close": pd.to_numeric(df["close"], errors="coerce"),
                "Volume": pd.to_numeric(df["volume"], errors="coerce"),
            }
        )
        if "adj_close" in df.columns and not pd.to_numeric(df["adj_close"], errors="coerce").isna().all():
            normalized["Adjusted_Close"] = pd.to_numeric(df["adj_close"], errors="coerce")

        normalized = normalized.dropna(subset=["Date"]).sort_values("Date")
        normalized = normalized.drop_duplicates(subset=["Date"], keep="last")
        normalized = normalized.dropna(subset=["Close"])
        return normalized.reset_index(drop=True)
