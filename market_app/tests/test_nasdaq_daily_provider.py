from pathlib import Path

import pandas as pd
import pytest

from market_monitor.providers.base import ProviderError
from market_monitor.providers.nasdaq_daily import NasdaqDailyProvider, NasdaqDailySource


def _provider(tmp_path: Path) -> NasdaqDailyProvider:
    fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "nasdaq_daily"
    return NasdaqDailyProvider(NasdaqDailySource(directory=fixtures_dir, cache_dir=tmp_path))


def test_provider_finds_symbol_with_dash_dot(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    path = provider.resolve_symbol_file("BRK.B")
    assert path is not None
    assert path.name == "BRK-B.csv"


def test_provider_sorts_and_dedupes_dates(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    df, _ = provider.load_symbol_data("SORT")
    dates = pd.to_datetime(df["Date"])
    assert dates.is_monotonic_increasing
    assert dates.duplicated().sum() == 0
    last_close = df.loc[df["Date"] == dates.max(), "Close"].iloc[0]
    assert last_close == pytest.approx(12.0)


def test_provider_missing_symbol(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    with pytest.raises(ProviderError):
        provider.load_symbol_data("MISSING")


def test_provider_parses_iso_dates_and_quoted_headers(tmp_path: Path) -> None:
    provider = _provider(tmp_path)
    df, _ = provider.load_symbol_data("ISO_QUOTED")
    dates = pd.to_datetime(df["Date"])
    assert dates.is_monotonic_increasing
    assert dates.iloc[-1].strftime("%Y-%m-%d") == "2026-01-27"


def test_cache_invalidation_on_file_change(tmp_path: Path) -> None:
    source_dir = tmp_path / "source"
    cache_dir = tmp_path / "cache"
    source_dir.mkdir()
    cache_dir.mkdir()

    try:
        pd.DataFrame({"a": [1]}).to_parquet(tmp_path / "probe.parquet")
    except ImportError:
        pytest.skip("Parquet engine not available for cache tests.")

    spy_path = source_dir / "SPY.csv"
    df = pd.DataFrame(
        {
            "Date": ["2026-01-24", "2026-01-25"],
            "Open": [100, 101],
            "High": [101, 102],
            "Low": [99, 100],
            "Close": [100, 101],
            "Volume": [1000, 1100],
        }
    )
    df.to_csv(spy_path, index=False)

    provider = NasdaqDailyProvider(NasdaqDailySource(directory=source_dir, cache_dir=cache_dir))
    first = provider.get_history_with_cache("SPY", 0, max_cache_age_days=999)
    assert first.used_cache is False
    assert len(first.df) == 2

    second = provider.get_history_with_cache("SPY", 0, max_cache_age_days=999)
    assert second.used_cache is True
    assert second.cache_path == first.cache_path

    df_updated = pd.concat(
        [
            df,
            pd.DataFrame(
                {
                    "Date": ["2026-01-27"],
                    "Open": [102],
                    "High": [103],
                    "Low": [101],
                    "Close": [102],
                    "Volume": [1200],
                }
            ),
        ],
        ignore_index=True,
    )
    df_updated.to_csv(spy_path, index=False)

    third = provider.get_history_with_cache("SPY", 0, max_cache_age_days=999)
    assert third.used_cache is False
    assert len(third.df) == 3
    assert third.cache_path != first.cache_path
