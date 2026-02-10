from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


SYMBOL_COLUMNS = [
    "symbol",
    "name",
    "exchange",
    "asset_type",
    "is_etf",
    "is_test_issue",
]


@dataclass(frozen=True)
class SymbolLoadResult:
    symbols: pd.DataFrame
    source_files: list[Path]
    used_sample_data: bool


def load_symbols(symbols_dir: Path, config: dict[str, Any], logger) -> SymbolLoadResult:
    sources: list[Path] = []
    if symbols_dir and symbols_dir.exists() and symbols_dir.resolve() != Path(".").resolve():
        sources = sorted([path for path in symbols_dir.iterdir() if path.is_file()])

    used_sample_data = False
    if not sources:
        symbols_dir = _resolve_sample_symbols_dir()
        sources = sorted([path for path in symbols_dir.iterdir() if path.is_file()])
        used_sample_data = True
        logger.warning(
            "Symbols directory missing or empty. Falling back to bundled sample data at %s.",
            symbols_dir,
        )

    frames: list[pd.DataFrame] = []
    for source in sources:
        if source.suffix.lower() in {".csv", ".txt"}:
            frames.append(_parse_symbol_file(source))

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=SYMBOL_COLUMNS)

    combined = combined.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    combined = _apply_filters(combined, config)
    combined = combined.reindex(columns=SYMBOL_COLUMNS)
    combined["is_etf"] = combined["is_etf"].fillna(False).astype(bool)
    combined["is_test_issue"] = combined["is_test_issue"].fillna(False).astype(bool)
    return SymbolLoadResult(symbols=combined, source_files=sources, used_sample_data=used_sample_data)


def _resolve_sample_symbols_dir() -> Path:
    package_root = Path(__file__).resolve().parent
    sample_dir = package_root / "sample_data" / "symbols"
    if sample_dir.exists():
        return sample_dir
    repo_root = package_root.parents[2]
    return repo_root / "tests" / "data" / "symbols"


def _parse_symbol_file(path: Path) -> pd.DataFrame:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if "|" in text and ("Symbol|" in text or "ACT Symbol|" in text):
        return _parse_pipe_listing(text)
    return _parse_csv_listing(path)


def _parse_pipe_listing(text: str) -> pd.DataFrame:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    header_idx = None
    for idx, line in enumerate(lines):
        if line.startswith("Symbol|") or line.startswith("ACT Symbol|"):
            header_idx = idx
            break
    if header_idx is None:
        return pd.DataFrame(columns=SYMBOL_COLUMNS)
    header = [col.strip() for col in lines[header_idx].split("|")]
    rows = []
    for line in lines[header_idx + 1 :]:
        if line.startswith("File Creation Time"):
            break
        parts = [part.strip() for part in line.split("|")]
        if len(parts) != len(header):
            continue
        rows.append(parts)
    df = pd.DataFrame(rows, columns=header)
    return _normalize_symbol_frame(df)


def _parse_csv_listing(path: Path) -> pd.DataFrame:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return pd.DataFrame(columns=SYMBOL_COLUMNS)
        rows = list(reader)
    df = pd.DataFrame(rows)
    return _normalize_symbol_frame(df)


def _normalize_symbol_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=SYMBOL_COLUMNS)
    columns = {col.lower().strip(): col for col in df.columns}
    symbol_col = columns.get("symbol") or columns.get("act symbol") or columns.get("ticker")
    if symbol_col is None:
        return pd.DataFrame(columns=SYMBOL_COLUMNS)
    name_col = (
        columns.get("security name")
        or columns.get("name")
        or columns.get("security")
        or columns.get("company name")
    )
    exchange_col = columns.get("exchange") or columns.get("listing exchange")
    asset_col = columns.get("asset type") or columns.get("security type")
    etf_col = columns.get("etf") or columns.get("is etf")
    test_col = columns.get("test issue") or columns.get("is test issue")

    normalized = pd.DataFrame(
        {
            "symbol": df[symbol_col].astype(str).str.strip().str.upper()
            if symbol_col
            else "",
            "name": df[name_col].astype(str).str.strip() if name_col else "",
            "exchange": df[exchange_col].astype(str).str.strip() if exchange_col else "",
            "asset_type": df[asset_col].astype(str).str.strip() if asset_col else "",
            "is_etf": _normalize_bool_series(df[etf_col]) if etf_col else False,
            "is_test_issue": _normalize_bool_series(df[test_col]) if test_col else False,
        }
    )
    normalized["asset_type"] = normalized["asset_type"].replace("", "COMMON")
    normalized["is_etf"] = normalized["is_etf"] | normalized["name"].str.contains(
        "ETF", case=False, na=False
    )
    return normalized.reindex(columns=SYMBOL_COLUMNS)


def _normalize_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().isin({"Y", "YES", "TRUE", "1"})


def _apply_filters(df: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    filters = config.get("filters", {})
    include_warrants = bool(filters.get("include_warrants", False))
    include_rights = bool(filters.get("include_rights", False))
    include_units = bool(filters.get("include_units", False))
    include_preferreds = bool(filters.get("include_preferreds", False))

    def _filter_keywords(row: pd.Series) -> bool:
        name = str(row.get("name", "")).upper()
        asset_type = str(row.get("asset_type", "")).upper()
        if not include_warrants and ("WARRANT" in name or "WARRANT" in asset_type):
            return False
        if not include_rights and ("RIGHT" in name or "RIGHT" in asset_type):
            return False
        if not include_units and ("UNIT" in name or "UNIT" in asset_type):
            return False
        if not include_preferreds and (
            "PREFERRED" in name
            or "PFD" in name
            or "PREFERRED" in asset_type
            or "PFD" in asset_type
        ):
            return False
        return True

    if df.empty:
        return df
    return df[df.apply(_filter_keywords, axis=1)].reset_index(drop=True)
