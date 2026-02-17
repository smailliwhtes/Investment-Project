from __future__ import annotations

from pathlib import Path

import pytest

from market_monitor.universe import read_watchlist
from market_monitor.validation import validate_watchlist


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


# ---------------------------------------------------------------------------
# Headerless watchlist (one symbol per line) support
# ---------------------------------------------------------------------------


def test_watchlist_csv_headerless_one_symbol_per_line(tmp_path: Path) -> None:
    """A .csv file with just symbols (no header) must be accepted."""
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_path.write_text("AAPL\nMSFT\nGOOG\n", encoding="utf-8")

    df = read_watchlist(watchlist_path)

    assert "symbol" in df.columns
    assert df["symbol"].tolist() == ["AAPL", "MSFT", "GOOG"]


def test_watchlist_csv_headerless_strips_whitespace_and_comments(tmp_path: Path) -> None:
    """Headerless watchlist: blanks and # comments must be ignored."""
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_path.write_text(
        "  AAPL  \n\n# this is a comment\nMSFT\n  \nGOOG\n",
        encoding="utf-8",
    )

    df = read_watchlist(watchlist_path)

    assert df["symbol"].tolist() == ["AAPL", "MSFT", "GOOG"]


def test_watchlist_csv_symbol_only_header(tmp_path: Path) -> None:
    """A CSV with only a 'symbol' header (no theme_bucket/asset_type) must work."""
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_path.write_text("symbol\nAAPL\nMSFT\n", encoding="utf-8")

    df = read_watchlist(watchlist_path)

    assert df["symbol"].tolist() == ["AAPL", "MSFT"]
    assert "theme_bucket" in df.columns
    assert "asset_type" in df.columns


def test_validate_watchlist_headerless_csv(tmp_path: Path) -> None:
    """validate_watchlist must accept headerless CSV files like read_watchlist does."""
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_path.write_text("AAPL\nMSFT\nGOOG\n", encoding="utf-8")

    df, errors = validate_watchlist(watchlist_path)

    assert not errors
    assert not df.empty
    assert "AAPL" in df["symbol"].values


def test_validate_watchlist_symbol_only_header(tmp_path: Path) -> None:
    """validate_watchlist must accept CSV with only symbol header."""
    watchlist_path = tmp_path / "watchlist.csv"
    watchlist_path.write_text("symbol\nAAPL\nMSFT\n", encoding="utf-8")

    df, errors = validate_watchlist(watchlist_path)

    assert not errors
    assert not df.empty
    assert "AAPL" in df["symbol"].values
