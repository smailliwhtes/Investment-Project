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
