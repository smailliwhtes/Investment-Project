from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from market_monitor.cache import CacheResult
from market_monitor.providers.base import HistoryProvider, ProviderCapabilities, ProviderError
from market_monitor.timebase import utcnow


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
        wrote = self._write_cache(df, cache_path)
        freshness = self._freshness_days(cache_path) if wrote else 0.0
        return CacheResult(df, freshness, cache_path, False)

    def get_history(self, symbol: str, days: int) -> pd.DataFrame:
        cache_path = self._cache_path(symbol)
        if cache_path.exists():
            df = pd.read_parquet(cache_path)
        else:
            df = self._load_symbol(symbol)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._write_cache(df, cache_path)
        if days > 0:
            return df.tail(days).copy()
        return df.copy()

    def _cache_path(self, symbol: str) -> Path:
        safe_symbol = symbol.replace("/", "-").replace("\\", "-")
        return self.source.cache_dir / "nasdaq_daily" / f"{safe_symbol}.parquet"

    def _freshness_days(self, path: Path) -> float:
        delta = pd.Timestamp(utcnow()) - pd.Timestamp(path.stat().st_mtime, unit="s")
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

    def _load_symbol(self, symbol: str) -> pd.DataFrame:
        df, _ = self.load_symbol_data(symbol)
        return df

    def resolve_symbol_file(self, symbol: str) -> Path | None:
        return self._find_symbol_file(symbol)

    def load_symbol_data(self, symbol: str) -> tuple[pd.DataFrame, Path]:
        source_file = self._find_symbol_file(symbol)
        if source_file is None:
            raise ProviderError(f"NASDAQ_DAILY_MISSING:{symbol}")
        df = pd.read_csv(source_file)
        normalized = self._normalize_ohlc(df, symbol)
        return normalized, source_file

    def _find_symbol_file(self, symbol: str) -> Path | None:
        candidates = []
        for variant in _symbol_variants(symbol):
            candidates.append(self.source.directory / f"{variant}.csv")
            candidates.append(self.source.directory / f"{variant}.CSV")
        for candidate in candidates:
            if candidate.exists():
                return candidate

        normalized_target = _normalize_symbol(symbol)
        for path in self.source.directory.glob("*.csv"):
            if _normalize_symbol(path.stem) == normalized_target:
                return path
        for path in self.source.directory.glob("*.CSV"):
            if _normalize_symbol(path.stem) == normalized_target:
                return path
        return None

    @staticmethod
    def _normalize_ohlc(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        cols = {c.lower().strip(): c for c in df.columns}
        date_col = cols.get("date") or cols.get("timestamp") or cols.get("time")
        open_col = cols.get("open")
        high_col = cols.get("high")
        low_col = cols.get("low")
        close_col = cols.get("close")
        adj_close_col = (
            cols.get("adj close")
            or cols.get("adj_close")
            or cols.get("adjusted close")
            or cols.get("adjusted_close")
        )
        volume_col = cols.get("volume") or cols.get("vol")

        missing = []
        for key, value in {
            "date": date_col,
            "open": open_col,
            "high": high_col,
            "low": low_col,
            "close": close_col,
        }.items():
            if value is None:
                missing.append(key)
        if missing:
            raise ProviderError(f"NASDAQ_DAILY_SCHEMA_MISSING:{symbol}:{','.join(missing)}")

        normalized = pd.DataFrame(
            {
                "Date": pd.to_datetime(df[date_col], errors="coerce"),
                "Open": pd.to_numeric(df[open_col], errors="coerce"),
                "High": pd.to_numeric(df[high_col], errors="coerce"),
                "Low": pd.to_numeric(df[low_col], errors="coerce"),
                "Close": pd.to_numeric(df[close_col], errors="coerce"),
            }
        )
        if adj_close_col is not None:
            normalized["Adjusted_Close"] = pd.to_numeric(df[adj_close_col], errors="coerce")
        if volume_col is not None:
            normalized["Volume"] = pd.to_numeric(df[volume_col], errors="coerce")
        else:
            normalized["Volume"] = np.nan

        normalized = normalized.dropna(subset=["Date"]).sort_values("Date")
        normalized = normalized.drop_duplicates(subset=["Date"], keep="last")
        normalized = normalized.dropna(subset=["Close"])
        return normalized.reset_index(drop=True)


def _symbol_variants(symbol: str) -> set[str]:
    base = symbol.strip()
    variants = {base, base.upper(), base.lower()}
    variants.add(base.replace(".", "-"))
    variants.add(base.replace("-", "."))
    variants.add(base.replace("/", "-"))
    variants.add(base.replace("/", "."))
    variants.add(base.replace(".", ""))
    variants.add(base.replace("-", ""))
    return {v for v in variants if v}


def _normalize_symbol(symbol: str) -> str:
    return symbol.replace("-", "").replace(".", "").replace("/", "").strip().upper()
