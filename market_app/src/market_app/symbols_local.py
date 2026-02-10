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
    "is_leveraged",
    "is_inverse",
    "country",
    "source_file",
]


SYMBOL_HEADER_ALIASES = {
    "symbol": {"symbol", "nasdaq symbol", "ticker"},
    "act_symbol": {"act symbol"},
    "name": {"security name", "name", "security", "company name"},
    "exchange": {"exchange", "listing exchange"},
    "asset_type": {"asset type", "security type"},
    "etf": {"etf", "is etf"},
    "test_issue": {"test issue", "is test issue"},
    "leveraged": {"leveraged", "leveraged etp"},
    "inverse": {"inverse", "inverse etp"},
    "leverage_ratio": {"leverage ratio"},
    "country": {"country", "country of incorporation"},
}


@dataclass(frozen=True)
class SymbolLoadResult:
    symbols: pd.DataFrame
    source_files: list[Path]
    used_sample_data: bool
    errors: list[str]


def load_symbols(symbols_dir: Path, config: dict[str, Any], logger) -> SymbolLoadResult:
    sources: list[Path] = []
    errors: list[str] = []
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
            frame, parse_errors = _parse_symbol_file(source)
            frames.append(frame)
            errors.extend(parse_errors)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
    else:
        combined = pd.DataFrame(columns=SYMBOL_COLUMNS)

    combined = combined.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    combined = _apply_filters(combined, config)
    combined = combined.reindex(columns=SYMBOL_COLUMNS)
    combined["is_etf"] = combined["is_etf"].fillna(False).astype(bool)
    combined["is_test_issue"] = combined["is_test_issue"].fillna(False).astype(bool)
    return SymbolLoadResult(
        symbols=combined,
        source_files=sources,
        used_sample_data=used_sample_data,
        errors=errors,
    )


def _resolve_sample_symbols_dir() -> Path:
    package_root = Path(__file__).resolve().parent
    sample_dir = package_root / "sample_data" / "symbols"
    if sample_dir.exists():
        return sample_dir
    repo_root = package_root.parents[2]
    return repo_root / "tests" / "data" / "symbols"


def _parse_symbol_file(path: Path) -> tuple[pd.DataFrame, list[str]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if "|" in text and ("Symbol|" in text or "ACT Symbol|" in text):
        return _parse_pipe_listing(text, path)
    return _parse_csv_listing(path)


def _parse_pipe_listing(text: str, path: Path) -> tuple[pd.DataFrame, list[str]]:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    header_idx = None
    errors: list[str] = []
    for idx, line in enumerate(lines):
        if line.startswith("Symbol|") or line.startswith("ACT Symbol|"):
            header_idx = idx
            break
    if header_idx is None:
        return pd.DataFrame(columns=SYMBOL_COLUMNS), errors
    header = [col.strip() for col in lines[header_idx].split("|")]
    rows = []
    for line_num, line in enumerate(lines[header_idx + 1 :], start=header_idx + 2):
        if line.startswith("File Creation Time"):
            break
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < len(header):
            errors.append(f"{path.name}:{line_num} expected {len(header)} fields, got {len(parts)}")
            continue
        if len(parts) > len(header):
            parts = parts[: len(header)]
        rows.append(parts)
    df = pd.DataFrame(rows, columns=header)
    return _normalize_symbol_frame(df, source_file=path.name), errors


def _parse_csv_listing(path: Path) -> tuple[pd.DataFrame, list[str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return pd.DataFrame(columns=SYMBOL_COLUMNS), []
        rows = list(reader)
    df = pd.DataFrame(rows)
    return _normalize_symbol_frame(df, source_file=path.name), []


def _normalize_symbol_frame(df: pd.DataFrame, *, source_file: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=SYMBOL_COLUMNS)
    columns = {col.lower().strip(): col for col in df.columns}
    symbol_col = _find_column(columns, SYMBOL_HEADER_ALIASES["symbol"])
    act_symbol_col = _find_column(columns, SYMBOL_HEADER_ALIASES["act_symbol"])
    symbol_col = symbol_col or act_symbol_col
    if symbol_col is None:
        return pd.DataFrame(columns=SYMBOL_COLUMNS)
    name_col = _find_column(columns, SYMBOL_HEADER_ALIASES["name"])
    exchange_col = _find_column(columns, SYMBOL_HEADER_ALIASES["exchange"])
    asset_col = _find_column(columns, SYMBOL_HEADER_ALIASES["asset_type"])
    etf_col = _find_column(columns, SYMBOL_HEADER_ALIASES["etf"])
    test_col = _find_column(columns, SYMBOL_HEADER_ALIASES["test_issue"])
    leverage_col = _find_column(columns, SYMBOL_HEADER_ALIASES["leveraged"])
    inverse_col = _find_column(columns, SYMBOL_HEADER_ALIASES["inverse"])
    leverage_ratio_col = _find_column(columns, SYMBOL_HEADER_ALIASES["leverage_ratio"])
    country_col = _find_column(columns, SYMBOL_HEADER_ALIASES["country"])

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
            "is_leveraged": _normalize_bool_series(df[leverage_col]) if leverage_col else False,
            "is_inverse": _normalize_bool_series(df[inverse_col]) if inverse_col else False,
            "country": df[country_col].astype(str).str.strip() if country_col else "",
            "source_file": source_file,
        }
    )
    normalized["asset_type"] = normalized["asset_type"].replace("", "COMMON")
    normalized["is_etf"] = normalized["is_etf"] | normalized["name"].str.contains(
        "ETF", case=False, na=False
    )
    if leverage_ratio_col:
        leverage_ratio = (
            pd.to_numeric(df[leverage_ratio_col], errors="coerce")
            .fillna(0.0)
            .abs()
        )
        normalized["is_leveraged"] = normalized["is_leveraged"] | (leverage_ratio >= 2.0)
    normalized["is_inverse"] = normalized["is_inverse"] | normalized["name"].str.contains(
        "INVERSE", case=False, na=False
    )
    return normalized.reindex(columns=SYMBOL_COLUMNS)


def _normalize_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().isin({"Y", "YES", "TRUE", "1"})


def _find_column(columns: dict[str, str], candidates: set[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return columns[candidate]
    return None


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
