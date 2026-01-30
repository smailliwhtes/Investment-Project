from __future__ import annotations

import csv
import json
from pathlib import Path


def load_sic_codes(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        lookup = {}
        for row in reader:
            sic = (row.get("sic") or "").strip()
            title = (row.get("industry_title") or "").strip()
            if sic:
                lookup[sic] = title
        return lookup


def load_sic_by_cik(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        lookup: dict[str, dict[str, str]] = {}
        for row in reader:
            cik = (row.get("cik") or "").strip()
            sic = (row.get("sic") or "").strip()
            sic_desc = (row.get("sic_desc") or "").strip()
            if cik:
                lookup[cik] = {"sic": sic, "sic_desc": sic_desc}
        return lookup


def load_sector_overrides(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        overrides: dict[str, str] = {}
        for row in reader:
            symbol = (row.get("symbol") or "").strip().upper()
            sector = (row.get("sector_bucket") or "").strip()
            if symbol and sector:
                overrides[symbol] = sector
        return overrides


def load_submissions_sic(submissions_dir: Path) -> dict[str, dict[str, str]]:
    if not submissions_dir.exists():
        return {}
    results: dict[str, dict[str, str]] = {}
    for path in submissions_dir.glob("CIK*.json"):
        cik = path.stem.replace("CIK", "")
        try:
            with path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except json.JSONDecodeError:
            continue
        sic = str(payload.get("sic") or "").strip()
        description = payload.get("sicDescription") or ""
        if sic:
            results[cik] = {"sic": sic, "sic_desc": description}
    return results


def derive_sector_bucket(
    symbol: str,
    name: str | None,
    sic: str | None,
    overrides: dict[str, str],
    fallback_bucket: str | None = None,
) -> str | None:
    symbol_upper = symbol.upper()
    if symbol_upper in overrides:
        return overrides[symbol_upper]

    if sic:
        bucket = _map_sic_to_bucket(sic)
        if bucket:
            return bucket

    if name:
        bucket = _name_based_bucket(name)
        if bucket:
            return bucket

    if fallback_bucket:
        return fallback_bucket
    return None


def _map_sic_to_bucket(sic: str) -> str | None:
    try:
        code = int(sic)
    except ValueError:
        return None
    if 1040 <= code <= 1049:
        return "precious_metals"
    if 1000 <= code <= 1499:
        return "mining"
    if 1300 <= code <= 1399:
        return "energy"
    if 3670 <= code <= 3679:
        return "semis"
    if 3720 <= code <= 3729 or 3760 <= code <= 3769:
        return "defense_aerospace"
    return None


def _name_based_bucket(name: str) -> str | None:
    lowered = name.lower()
    if "gold" in lowered or "silver" in lowered or "bullion" in lowered:
        return "precious_metals"
    if "uranium" in lowered or "rare earth" in lowered:
        return "rare_earths"
    if "mining" in lowered or "miners" in lowered:
        return "mining"
    if "defense" in lowered or "aerospace" in lowered:
        return "defense_aerospace"
    if "semiconductor" in lowered or "chip" in lowered:
        return "semis"
    if "treasury" in lowered or "bond" in lowered or "rates" in lowered:
        return "treasuries_rates"
    if "energy" in lowered or "oil" in lowered or "gas" in lowered:
        return "energy"
    if "market" in lowered or "benchmark" in lowered:
        return "broad_market"
    return None
