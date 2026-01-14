import csv
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"


def _parse_pipe_table(text: str) -> pd.DataFrame:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    header_idx = None
    for idx, line in enumerate(lines):
        if line.startswith("Symbol|") or line.startswith("ACT Symbol|"):
            header_idx = idx
            break
    if header_idx is None:
        raise ValueError("Could not find header row in universe response.")
    header = lines[header_idx].split("|")
    rows = []
    for line in lines[header_idx + 1:]:
        if line.startswith("File Creation Time"):
            break
        parts = line.split("|")
        if len(parts) == len(header):
            rows.append(parts)
    return pd.DataFrame(rows, columns=header)


def fetch_universe() -> pd.DataFrame:
    listed = requests.get(NASDAQ_LISTED_URL, timeout=30).text
    other = requests.get(OTHER_LISTED_URL, timeout=30).text
    df_listed = _parse_pipe_table(listed)
    df_other = _parse_pipe_table(other)
    df_listed = df_listed.rename(columns={"Symbol": "symbol", "Security Name": "name"})
    df_other = df_other.rename(columns={"ACT Symbol": "symbol", "Security Name": "name"})
    combined = pd.concat([df_listed, df_other], ignore_index=True)
    combined = combined[["symbol", "name"]].drop_duplicates()
    combined["exchange"] = None
    combined["security_type"] = "COMMON"
    combined["status"] = None
    combined["currency"] = "USD"
    return combined


def read_watchlist(path: Path) -> pd.DataFrame:
    symbols: List[str] = []
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "name", "exchange", "security_type", "status", "currency"])
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            symbol = line.strip().split("#")[0].strip()
            if symbol:
                symbols.append(symbol.upper())
    return pd.DataFrame({
        "symbol": symbols,
        "name": symbols,
        "exchange": None,
        "security_type": "COMMON",
        "status": None,
        "currency": "USD",
    })


def filter_universe(df: pd.DataFrame, allowed_types: List[str], allowed_currencies: List[str], include_etfs: bool) -> pd.DataFrame:
    filtered = df.copy()
    if allowed_types:
        filtered = filtered[filtered["security_type"].isin(allowed_types)]
    if allowed_currencies:
        filtered = filtered[filtered["currency"].isin(allowed_currencies)]
    if not include_etfs:
        filtered = filtered[filtered["security_type"] != "ETF"]
    return filtered.reset_index(drop=True)


def write_universe_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
