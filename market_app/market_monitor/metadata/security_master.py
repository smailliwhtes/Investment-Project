from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

from market_monitor.config.discovery import load_required_symbols
from market_monitor.data_sources.stooq_txt import (
    discover_stooq_txt,
    parse_stooq_symbol_and_asof,
)

SECURITY_MASTER_COLUMNS = [
    "symbol",
    "symbol_us",
    "name",
    "exchange",
    "is_etf",
    "cik",
    "sic",
    "sector_bucket",
    "source_bucket",
    "ohlcv_path",
    "asof_date",
]


@dataclass(frozen=True)
class SecurityMasterRecord:
    symbol: str
    symbol_us: str
    name: str | None
    exchange: str | None
    is_etf: bool | None
    cik: str | None
    sic: str | None
    sector_bucket: str | None
    source_bucket: str
    ohlcv_path: str
    asof_date: str


@dataclass(frozen=True)
class SecurityMasterConfig:
    stooq_root: Path
    output_path: Path
    required_symbols_path: Path | None = None
    filter_required: bool = False
    path_mode: str = "auto"
    repo_root: Path | None = None
    asof_date: date | None = None


def build_security_master(config: SecurityMasterConfig) -> list[SecurityMasterRecord]:
    stooq_files = discover_stooq_txt(config.stooq_root)
    required_symbols = load_required_symbols(config.required_symbols_path)
    asof_date = (config.asof_date or date.today()).isoformat()

    records: list[SecurityMasterRecord] = []
    for path in stooq_files:
        symbol_us, symbol, _ = parse_stooq_symbol_and_asof(path)
        symbol = symbol.upper()
        if required_symbols and config.filter_required and symbol not in required_symbols:
            continue
        exchange, is_etf, bucket = _infer_exchange_and_etf(path)
        ohlcv_path = _format_path(path, config.path_mode, config.repo_root, config.stooq_root)
        source_bucket = "stooq_txt"
        if bucket:
            source_bucket = f"{source_bucket}+{bucket}"
        records.append(
            SecurityMasterRecord(
                symbol=symbol,
                symbol_us=symbol_us,
                name="",
                exchange=exchange,
                is_etf=is_etf,
                cik=None,
                sic=None,
                sector_bucket=None,
                source_bucket=source_bucket,
                ohlcv_path=ohlcv_path,
                asof_date=asof_date,
            )
        )

    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_security_master(records, config.output_path)
    return records


def _write_security_master(records: Iterable[SecurityMasterRecord], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(SECURITY_MASTER_COLUMNS)
        for rec in records:
            writer.writerow(
                [
                    rec.symbol,
                    rec.symbol_us,
                    rec.name or "",
                    rec.exchange or "",
                    "" if rec.is_etf is None else str(rec.is_etf).lower(),
                    rec.cik or "",
                    rec.sic or "",
                    rec.sector_bucket or "",
                    rec.source_bucket,
                    rec.ohlcv_path,
                    rec.asof_date,
                ]
            )


def _format_path(path: Path, mode: str, repo_root: Path | None, stooq_root: Path) -> str:
    if mode == "absolute":
        return str(path.resolve())
    if mode not in {"auto", "relative"}:
        raise ValueError(f"Unknown path mode: {mode}")

    if mode == "auto" and repo_root:
        try:
            return str(path.resolve().relative_to(repo_root))
        except ValueError:
            pass

    try:
        return str(path.resolve().relative_to(stooq_root.resolve()))
    except ValueError:
        return str(path.resolve())


def _infer_exchange_and_etf(path: Path) -> tuple[str | None, bool | None, str | None]:
    parts = [part.lower() for part in path.parts]
    exchange = None
    bucket = None
    if "nysemkt" in parts:
        exchange = "NYSEMKT"
    elif "nyse" in parts:
        exchange = "NYSE"
    elif "nasdaq" in parts:
        exchange = "NASDAQ"

    if "nysearca" in parts:
        exchange = "NYSEARCA"

    is_etf = None
    if any("etf" in part for part in parts):
        is_etf = True
    elif any("stock" in part for part in parts):
        is_etf = False

    bucket = _extract_bucket(path)
    return exchange, is_etf, bucket


def _extract_bucket(path: Path) -> str | None:
    lower_parts = [part.lower() for part in path.parts]
    if "nasdaq" in lower_parts:
        base = "nasdaq"
    elif "nysemkt" in lower_parts:
        base = "nysemkt"
    elif "nyse" in lower_parts:
        base = "nyse"
    else:
        return None

    segment = None
    for part in lower_parts:
        if part in {"etfs", "etf"}:
            segment = "etfs"
        if part in {"stocks", "stock"}:
            segment = "stocks"
    if not segment:
        return None
    return f"{base}_{segment}"
