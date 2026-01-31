from __future__ import annotations

from pathlib import Path

import pytest

from market_monitor.universe import read_watchlist


def test_watchlist_csv_loads_and_normalizes(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_path.write_text(
        "\n".join(
            [
                "symbol,theme_bucket,asset_type",
                " spy ,  macro_theme , etf",
                "Abc,alpha_theme , equity",
                "GLD, metals , TRUST",
            ]
        ),
        encoding="utf-8",
    )

    df = read_watchlist(watchlist_path)

    assert df["symbol"].tolist() == ["SPY", "ABC", "GLD"]
    assert df["theme_bucket"].tolist() == ["macro_theme", "alpha_theme", "metals"]
    assert df["asset_type"].tolist() == ["ETF", "equity", "trust"]
    assert df["security_type"].tolist() == ["ETF", "COMMON", "COMMON"]


def test_watchlist_csv_rejects_invalid_asset_type(tmp_path: Path) -> None:
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_path.write_text(
        "\n".join(
            [
                "symbol,theme_bucket,asset_type",
                "SPY,macro,etf",
                "ABC,theme,crypto",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"row 3"):
        read_watchlist(watchlist_path)
