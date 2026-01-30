from __future__ import annotations

from pathlib import Path

from market_monitor.data_sources.stooq_txt import (
    discover_stooq_txt,
    parse_stooq_symbol_and_asof,
    read_stooq_daily,
)


def _fixture_root() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "stooq_txt"


def test_discover_stooq_txt() -> None:
    stooq_root = _fixture_root()
    files = discover_stooq_txt(stooq_root)
    assert any(path.name == "AAA.us.txt" for path in files)
    assert any(path.name == "CCC.us.txt" for path in files)


def test_parse_stooq_symbol_and_asof() -> None:
    path = _fixture_root() / "nasdaq stocks" / "2" / "CCC.us.txt"
    symbol_us, symbol, asof = parse_stooq_symbol_and_asof(path)
    assert symbol_us == "CCC.US"
    assert symbol == "CCC"
    assert asof == "2024-01-03"


def test_read_stooq_daily_rows() -> None:
    path = _fixture_root() / "nasdaq stocks" / "2" / "CCC.us.txt"
    symbol_us, rows = read_stooq_daily(path)
    assert symbol_us == "CCC.US"
    assert rows
    assert rows[0].open == 10.0
