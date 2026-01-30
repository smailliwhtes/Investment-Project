from __future__ import annotations

import json
from pathlib import Path


def load_sec_ticker_map(sec_dir: Path) -> dict[str, dict[str, str]]:
    if not sec_dir.exists():
        return {}
    metadata: dict[str, dict[str, str]] = {}
    for filename in ("company_tickers.json", "company_tickers_exchange.json"):
        path = sec_dir / filename
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        items = payload.values() if isinstance(payload, dict) else payload
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
