from __future__ import annotations

import csv
from pathlib import Path

EXCHANGE_CODE_MAP = {
    "A": "NYSEMKT",
    "N": "NYSE",
    "P": "NYSEARCA",
    "Q": "NASDAQ",
    "V": "IEX",
    "Z": "BATS",
}


def load_nasdaq_symboldir(nasdaq_dir: Path) -> dict[str, dict[str, object]]:
    if not nasdaq_dir.exists():
        return {}
    metadata: dict[str, dict[str, object]] = {}
    for filename in ("nasdaqlisted.txt", "otherlisted.txt"):
        path = nasdaq_dir / filename
        if not path.exists():
            continue
        for row in _parse_pipe_table(path):
            symbol = (row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            entry = metadata.setdefault(symbol, {})
            if row.get("name"):
                entry["name"] = row["name"]
            if row.get("exchange"):
                entry["exchange"] = row["exchange"]
            if row.get("is_etf") is not None:
                entry["is_etf"] = row["is_etf"]
    return metadata


def _parse_pipe_table(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle, delimiter="|")
        rows = list(reader)
    header_idx = None
    for idx, row in enumerate(rows):
        if row and (row[0].startswith("Symbol") or row[0].startswith("ACT Symbol")):
            header_idx = idx
            break
    if header_idx is None:
        return []
    header = rows[header_idx]
    data_rows = []
    for row in rows[header_idx + 1 :]:
        if row and row[0].startswith("File Creation Time"):
            break
        if len(row) != len(header):
            continue
        record = dict(zip(header, row))
        symbol_key = "Symbol" if "Symbol" in record else "ACT Symbol"
        exchange_code = (record.get("Exchange") or "").strip()
        exchange = EXCHANGE_CODE_MAP.get(exchange_code, exchange_code or None)
        if not exchange and symbol_key == "Symbol":
            exchange = "NASDAQ"
        data_rows.append(
            {
                "symbol": record.get(symbol_key, "").strip(),
                "name": (record.get("Security Name") or record.get("SecurityName") or "").strip(),
                "exchange": exchange,
                "is_etf": _parse_yes_no(record.get("ETF")),
            }
        )
    return data_rows


def _parse_yes_no(value: str | None) -> bool | None:
    if value is None:
        return None
    cleaned = value.strip().upper()
    if cleaned == "Y":
        return True
    if cleaned == "N":
        return False
    return None
