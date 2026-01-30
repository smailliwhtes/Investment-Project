from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable


LOGGER = logging.getLogger(__name__)

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

EXCHANGE_CODE_MAP = {
    "A": "NYSEMKT",
    "N": "NYSE",
    "P": "NYSEARCA",
    "Q": "NASDAQ",
    "V": "IEX",
    "Z": "BATS",
}


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
    metadata_root: Path | None = None
    required_symbols_path: Path | None = None
    filter_required: bool = False
    path_mode: str = "auto"
    repo_root: Path | None = None
    metastock_root: Path | None = None
    sic_codes_path: Path | None = None


def discover_stooq_files(root: Path) -> list[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Stooq root not found: {root}")
    return sorted(root.rglob("*.us.txt"))


def load_required_symbols(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    symbols: set[str] = set()
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames:
                lower_fields = [field.lower().strip() for field in reader.fieldnames]
                symbol_field = None
                for candidate in ("symbol", "ticker"):
                    if candidate in lower_fields:
                        symbol_field = reader.fieldnames[lower_fields.index(candidate)]
                        break
                if symbol_field is None:
                    symbol_field = reader.fieldnames[0]
                for row in reader:
                    symbol = (row.get(symbol_field) or "").strip().upper()
                    if symbol:
                        symbols.add(_strip_us(symbol))
    else:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                symbol = line.split("#")[0].strip().upper()
                if symbol:
                    symbols.add(_strip_us(symbol))
    return symbols


def parse_stooq_file(path: Path) -> tuple[str, str, str]:
    symbol_us = None
    max_date = None
    ticker_idx = None
    date_idx = None

    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                raise ValueError("Missing header row.")
            normalized = [col.strip("<> ").lower() for col in header]
            if "ticker" in normalized:
                ticker_idx = normalized.index("ticker")
            if "date" in normalized:
                date_idx = normalized.index("date")
            for row in reader:
                if not row:
                    continue
                if ticker_idx is not None and ticker_idx < len(row) and not symbol_us:
                    symbol_us = _normalize_symbol_us(row[ticker_idx])
                if date_idx is not None and date_idx < len(row):
                    parsed = _parse_stooq_date(row[date_idx])
                    if parsed and (max_date is None or parsed > max_date):
                        max_date = parsed
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Stooq file missing: {path}") from exc

    if not symbol_us:
        symbol_us = _normalize_symbol_us(path.stem)
    if max_date is None:
        max_date = date.fromtimestamp(path.stat().st_mtime)
    return symbol_us, _strip_us(symbol_us), max_date.isoformat()


def build_security_master(config: SecurityMasterConfig) -> list[SecurityMasterRecord]:
    stooq_files = discover_stooq_files(config.stooq_root)
    required_symbols = load_required_symbols(config.required_symbols_path)

    nasdaq_dir = None
    sec_dir = None
    if config.metadata_root:
        nasdaq_dir = config.metadata_root / "nasdaq_trader"
        sec_dir = config.metadata_root / "sec"

    nasdaq_metadata = _load_nasdaq_metadata(nasdaq_dir) if nasdaq_dir else {}
    sec_metadata = _load_sec_metadata(sec_dir) if sec_dir else {}
    submissions_dir = sec_dir / "submissions" if sec_dir else None
    sic_lookup = _load_sic_codes(config.sic_codes_path, sec_dir)
    submissions_sic = _load_submissions_sic(submissions_dir, sic_lookup)
    metastock_metadata = _load_metastock_metadata(config.metastock_root)

    records: list[SecurityMasterRecord] = []
    for path in stooq_files:
        symbol_us, symbol, asof_date = parse_stooq_file(path)
        if required_symbols and config.filter_required and symbol not in required_symbols:
            continue
        exchange, is_etf = _infer_exchange_and_etf(path)
        name = None
        cik = None
        sic = None
        sector_bucket = None

        nasdaq_row = nasdaq_metadata.get(symbol)
        if nasdaq_row:
            name = nasdaq_row.get("name") or name
            exchange = nasdaq_row.get("exchange") or exchange
            is_etf = nasdaq_row.get("is_etf") if nasdaq_row.get("is_etf") is not None else is_etf

        sec_row = sec_metadata.get(symbol)
        if sec_row:
            cik = sec_row.get("cik") or cik
            if not exchange:
                exchange = sec_row.get("exchange")
            if not name:
                name = sec_row.get("name")

        if not name and metastock_metadata:
            name = metastock_metadata.get(symbol)

        if cik:
            sic, sector_bucket = submissions_sic.get(cik, (sic, sector_bucket))

        if not name:
            name = symbol

        ohlcv_path = _format_path(
            path, config.path_mode, config.repo_root, config.stooq_root
        )
        records.append(
            SecurityMasterRecord(
                symbol=symbol,
                symbol_us=symbol_us,
                name=name,
                exchange=exchange,
                is_etf=is_etf,
                cik=cik,
                sic=sic,
                sector_bucket=sector_bucket,
                source_bucket="stooq_txt",
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
            LOGGER.info("Stooq path outside repo root; falling back to stooq-relative path.")

    try:
        return str(path.resolve().relative_to(stooq_root.resolve()))
    except ValueError:
        return str(path.resolve())


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


def _infer_exchange_and_etf(path: Path) -> tuple[str | None, bool | None]:
    parts = [part.lower() for part in path.parts]
    exchange = None
    if "nysemkt" in parts:
        exchange = "NYSEMKT"
    elif "nyse" in parts:
        exchange = "NYSE"
    elif "nasdaq" in parts:
        exchange = "NASDAQ"

    is_etf = None
    if any("etf" in part for part in parts):
        is_etf = True
    elif any("stock" in part for part in parts):
        is_etf = False
    return exchange, is_etf


def _load_nasdaq_metadata(nasdaq_dir: Path) -> dict[str, dict[str, object]]:
    if not nasdaq_dir.exists():
        return {}
    metadata: dict[str, dict[str, object]] = {}
    for filename in ("nasdaqlisted.txt", "otherlisted.txt"):
        path = nasdaq_dir / filename
        if not path.exists():
            continue
        parsed = _parse_pipe_table(path)
        for row in parsed:
            symbol = (row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            name = row.get("name")
            exchange = row.get("exchange")
            is_etf = row.get("is_etf")
            metadata.setdefault(symbol, {})
            if name:
                metadata[symbol]["name"] = name
            if exchange:
                metadata[symbol]["exchange"] = exchange
            if is_etf is not None:
                metadata[symbol]["is_etf"] = is_etf
    return metadata


def _parse_pipe_table(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        lines = [ln.strip() for ln in handle if ln.strip()]
    header_idx = None
    for idx, line in enumerate(lines):
        if line.startswith("Symbol|") or line.startswith("ACT Symbol|"):
            header_idx = idx
            break
    if header_idx is None:
        LOGGER.warning("Could not parse symboldir header in %s", path)
        return []
    header = lines[header_idx].split("|")
    rows = []
    for line in lines[header_idx + 1 :]:
        if line.startswith("File Creation Time"):
            break
        parts = line.split("|")
        if len(parts) != len(header):
            continue
        row = dict(zip(header, parts))
        symbol_key = "Symbol" if "Symbol" in row else "ACT Symbol"
        exchange_code = row.get("Exchange", "").strip()
        exchange = EXCHANGE_CODE_MAP.get(exchange_code, exchange_code or None)
        if not exchange and symbol_key == "Symbol":
            exchange = "NASDAQ"
        rows.append(
            {
                "symbol": row.get(symbol_key, "").strip(),
                "name": (row.get("Security Name") or row.get("SecurityName") or "").strip(),
                "exchange": exchange,
                "is_etf": _parse_yes_no(row.get("ETF")),
            }
        )
    return rows


def _parse_yes_no(value: str | None) -> bool | None:
    if value is None:
        return None
    cleaned = value.strip().upper()
    if cleaned == "Y":
        return True
    if cleaned == "N":
        return False
    return None


def _load_sec_metadata(sec_dir: Path | None) -> dict[str, dict[str, str]]:
    if sec_dir is None or not sec_dir.exists():
        return {}
    metadata: dict[str, dict[str, str]] = {}
    for filename in ("company_tickers.json", "company_tickers_exchange.json"):
        path = sec_dir / filename
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            items = payload.values()
        else:
            items = payload
        for item in items:
            ticker = str(item.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            cik = str(item.get("cik_str") or item.get("cik") or "").strip()
            cik = cik.zfill(10) if cik else ""
            name = item.get("title") or item.get("name") or None
            exchange = item.get("exchange") or item.get("exchange_name") or None
            entry = metadata.setdefault(ticker, {})
            if cik:
                entry["cik"] = cik
            if name:
                entry["name"] = name
            if exchange:
                entry["exchange"] = exchange
    return metadata


def _load_submissions_sic(
    submissions_dir: Path | None,
    sic_lookup: dict[str, str],
) -> dict[str, tuple[str | None, str | None]]:
    if submissions_dir is None or not submissions_dir.exists():
        return {}
    results: dict[str, tuple[str | None, str | None]] = {}
    for path in submissions_dir.glob("CIK*.json"):
        cik = path.stem.replace("CIK", "")
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError:
            LOGGER.warning("Invalid submissions JSON: %s", path)
            continue
        sic = str(payload.get("sic") or "").strip()
        description = payload.get("sicDescription")
        industry = sic_lookup.get(sic) if sic else None
        sector_bucket = industry or description
        results[cik] = (sic or None, sector_bucket)
    return results


def _load_sic_codes(path: Path | None, sec_dir: Path | None) -> dict[str, str]:
    candidates = []
    if path:
        candidates.append(path)
    if sec_dir:
        candidates.append(sec_dir / "sic_codes.csv")
    candidates.append(Path("out") / "sic_codes.csv")

    for candidate in candidates:
        if candidate.exists():
            with candidate.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                lookup = {}
                for row in reader:
                    sic = (row.get("sic") or "").strip()
                    title = (row.get("industry_title") or "").strip()
                    if sic:
                        lookup[sic] = title or None
                return lookup
    return {}


def _load_metastock_metadata(root: Path | None) -> dict[str, str]:
    if root is None or not root.exists():
        return {}
    xmaster_paths = list(root.rglob("XMASTER"))
    if not xmaster_paths:
        return {}
    metadata: dict[str, str] = {}
    for path in xmaster_paths:
        try:
            content = path.read_bytes()
        except OSError:
            continue
        if b"\x00" in content:
            LOGGER.info("Skipping binary MetaStock XMASTER: %s", path)
            continue
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")
        for line in text.splitlines():
            row = _parse_metastock_line(line)
            if row:
                symbol, name = row
                metadata.setdefault(symbol, name)
    return metadata


def _parse_metastock_line(line: str) -> tuple[str, str] | None:
    cleaned = line.strip()
    if not cleaned:
        return None
    for delimiter in ("|", ",", "\t"):
        if delimiter in cleaned:
            parts = [part.strip() for part in cleaned.split(delimiter)]
            break
    else:
        parts = cleaned.split()
    if len(parts) < 2:
        return None
    symbol = parts[0].strip().upper()
    name = " ".join(parts[1:]).strip()
    if not symbol or not name:
        return None
    return symbol, name
