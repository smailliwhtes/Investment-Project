from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass(frozen=True)
class StooqOhlcvRow:
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float | None


def discover_stooq_txt(root: Path) -> list[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Stooq root not found: {root}")
    return sorted(root.rglob("*.us.txt"))


def parse_stooq_symbol_and_asof(path: Path) -> tuple[str, str, str]:
    symbol_us = None
    max_date = None
    header_map = None
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                raise ValueError("Missing header row.")
            header_map = _parse_header(header)
            for row in reader:
                if not row:
                    continue
                symbol_us = symbol_us or _extract_symbol(row, header_map)
                parsed_date = _parse_stooq_date(_extract_field(row, header_map, "date"))
                if parsed_date and (max_date is None or parsed_date > max_date):
                    max_date = parsed_date
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Stooq file missing: {path}") from exc

    if not symbol_us:
        symbol_us = _normalize_symbol_us(path.stem)
    if max_date is None:
        max_date = date.fromtimestamp(path.stat().st_mtime)
    return symbol_us, _strip_us(symbol_us), max_date.isoformat()


def read_stooq_daily(path: Path) -> tuple[str, list[StooqOhlcvRow]]:
    symbol_us = None
    rows: list[StooqOhlcvRow] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            raise ValueError("Missing header row.")
        header_map = _parse_header(header)
        for row in reader:
            if not row:
                continue
            symbol_us = symbol_us or _extract_symbol(row, header_map)
            parsed_date = _parse_stooq_date(_extract_field(row, header_map, "date"))
            if parsed_date is None:
                continue
            ohlcv = StooqOhlcvRow(
                date=parsed_date,
                open=_parse_float(_extract_field(row, header_map, "open")),
                high=_parse_float(_extract_field(row, header_map, "high")),
                low=_parse_float(_extract_field(row, header_map, "low")),
                close=_parse_float(_extract_field(row, header_map, "close")),
                volume=_parse_volume(_extract_field(row, header_map, "vol")),
            )
            rows.append(ohlcv)
    if not symbol_us:
        symbol_us = _normalize_symbol_us(path.stem)
    return symbol_us, rows


def _parse_header(header: list[str]) -> dict[str, int]:
    normalized = [col.strip().lower() for col in header]
    if not all(col.startswith("<") and col.endswith(">") for col in normalized):
        raise ValueError(f"Unexpected Stooq header format: {header}")
    cleaned = [col.strip("<> ") for col in normalized]
    mapping: dict[str, int] = {}
    for idx, col in enumerate(cleaned):
        mapping[col] = idx
    return mapping


def _extract_symbol(row: list[str], header_map: dict[str, int]) -> str | None:
    ticker = _extract_field(row, header_map, "ticker")
    if not ticker:
        return None
    return _normalize_symbol_us(ticker)


def _extract_field(row: list[str], header_map: dict[str, int], field: str) -> str | None:
    idx = header_map.get(field)
    if idx is None or idx >= len(row):
        return None
    return row[idx].strip()


def _normalize_symbol_us(value: str) -> str:
    symbol = value.strip().upper()
    if not symbol:
        return symbol
    if not symbol.endswith(".US"):
        symbol = f"{symbol}.US"
    return symbol


def _strip_us(symbol: str) -> str:
    if symbol.upper().endswith(".US"):
        return symbol[:-3]
    return symbol


def _parse_stooq_date(value: str | None) -> date | None:
    if not value:
        return None
    cleaned = value.strip()
    if len(cleaned) == 8 and cleaned.isdigit():
        return datetime.strptime(cleaned, "%Y%m%d").date()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


def _parse_float(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def _parse_volume(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None
