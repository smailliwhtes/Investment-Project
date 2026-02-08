import os
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from market_monitor.timebase import utcnow

@dataclass
class CacheResult:
    df: pd.DataFrame
    data_freshness_days: float
    cache_path: Path
    used_cache: bool


class FileLock:
    def __init__(self, path: Path, timeout_s: float = 10.0) -> None:
        self.path = path
        self.timeout_s = timeout_s
        self.fd = None

    def __enter__(self):
        start = time.time()
        while True:
            try:
                self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                return self
            except FileExistsError as exc:
                if time.time() - start > self.timeout_s:
                    raise TimeoutError(f"Timeout acquiring lock {self.path}") from exc
                time.sleep(0.1)

    def __exit__(self, exc_type, exc, tb):
        if self.fd is not None:
            os.close(self.fd)
            with suppress(FileNotFoundError):
                os.remove(self.path)


def cache_key(provider_name: str, symbol: str, adjusted_mode: str) -> str:
    safe_symbol = symbol.replace("/", "-").replace("\\", "-")
    return f"{provider_name}_{safe_symbol}_{adjusted_mode}.csv"


def load_cache(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).sort_values("Date")
    return df


def save_cache(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def freshness_days(path: Path) -> float:
    age_seconds = utcnow().timestamp() - path.stat().st_mtime
    return age_seconds / 86400.0


def merge_delta(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    combined = pd.concat([existing, new], ignore_index=True)
    combined = combined.drop_duplicates(subset=["Date"], keep="last").sort_values("Date")
    return combined


def get_or_fetch(
    cache_dir: Path,
    provider_name: str,
    symbol: str,
    adjusted_mode: str,
    max_cache_age_days: float,
    fetch_fn,
    delta_days: int,
) -> CacheResult:
    cache_path = cache_dir / cache_key(provider_name, symbol, adjusted_mode)
    lock_path = cache_path.with_suffix(".lock")
    cache_df = load_cache(cache_path)
    if cache_df is not None and freshness_days(cache_path) <= max_cache_age_days:
        return CacheResult(cache_df, freshness_days(cache_path), cache_path, True)

    with FileLock(lock_path):
        cache_df = load_cache(cache_path)
        if cache_df is not None and freshness_days(cache_path) <= max_cache_age_days:
            return CacheResult(cache_df, freshness_days(cache_path), cache_path, True)

        new_df = fetch_fn()
        if "Date" in new_df.columns:
            new_df["Date"] = pd.to_datetime(new_df["Date"], errors="coerce")
            new_df = new_df.dropna(subset=["Date"]).sort_values("Date")
        if cache_df is not None and delta_days > 0:
            merged = merge_delta(cache_df, new_df)
        else:
            merged = new_df
        save_cache(cache_path, merged)
        return CacheResult(merged, freshness_days(cache_path), cache_path, False)
