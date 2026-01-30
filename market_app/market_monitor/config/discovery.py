from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

import yaml

DEFAULT_REQUIRED_SYMBOLS = [
    ("SPY", "US equities proxy"),
    ("QQQ", "Nasdaq proxy"),
    ("IWM", "US small caps proxy"),
    ("GLD", "Gold bullion"),
    ("SLV", "Silver bullion"),
    ("GDX", "Gold miners"),
    ("GDXJ", "Junior gold miners"),
    ("REMX", "Rare earths/strategic materials"),
    ("LIT", "Lithium/theme metals"),
    ("URA", "Uranium miners"),
    ("XLE", "US energy sector"),
    ("XOP", "Oil & gas exploration"),
    ("XME", "Metals & mining"),
    ("ITA", "Defense & aerospace"),
    ("XAR", "Defense & aerospace"),
    ("TLT", "Long-term treasuries"),
    ("IEF", "Intermediate treasuries"),
    ("SHY", "Short-term treasuries"),
]


def find_repo_root(start: Path) -> Path | None:
    for parent in (start, *start.parents):
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def find_required_symbols_file(repo_root: Path) -> Path | None:
    candidate = repo_root / "config" / "universe_required.csv"
    if candidate.exists():
        return candidate

    config_yaml = repo_root / "config" / "config.yaml"
    if config_yaml.exists():
        config = _load_yaml(config_yaml)
        watchlist_path = _read_nested(config, ["paths", "watchlist_file"])
        if watchlist_path:
            resolved = (repo_root / watchlist_path).resolve()
            if resolved.exists():
                return resolved

    inputs_watchlist = repo_root / "inputs" / "watchlist.txt"
    if inputs_watchlist.exists():
        return inputs_watchlist

    watchlist = repo_root / "watchlist.txt"
    if watchlist.exists():
        return watchlist

    return None


def ensure_required_symbols_file(repo_root: Path) -> Path:
    existing = find_required_symbols_file(repo_root)
    if existing:
        return existing

    target = repo_root / "config" / "universe_required.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["symbol", "notes"])
        writer.writerows(DEFAULT_REQUIRED_SYMBOLS)
    return target


def load_required_symbols(path: Path | None) -> set[str]:
    if path is None or not path.exists():
        return set()
    symbols: set[str] = set()
    if path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames:
                lower_fields = [field.lower().strip() for field in reader.fieldnames]
                symbol_field = _detect_symbol_field(reader.fieldnames, lower_fields)
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


def _detect_symbol_field(fields: Iterable[str], lower_fields: list[str]) -> str:
    for candidate in ("symbol", "ticker"):
        if candidate in lower_fields:
            return list(fields)[lower_fields.index(candidate)]
    return list(fields)[0]


def _strip_us(symbol: str) -> str:
    if symbol.upper().endswith(".US"):
        return symbol[:-3]
    return symbol


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _read_nested(payload: dict, keys: list[str]) -> str | None:
    current = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    if isinstance(current, str):
        return current
    return None
