from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import itertools
import json
import math
import re
import threading
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional, TextIO


# -----------------------------------------------------------------------------
# HARDWIRED PATHS
# Edit these three paths to your real local folders.
# -----------------------------------------------------------------------------
HARDCODED_PATHS = {
    "new_daily_files": Path(r"C:\CHANGE_ME\New Daily Files"),
    "gdelt_corpus": Path(r"C:\CHANGE_ME\GDELT Corpus"),
    "working_csv_files": Path(r"C:\CHANGE_ME\Working CSV Files"),
}

APP_TITLE = "Daily Market Preprocessor"
STATE_DIRNAME = "_preprocessor_state"
MARKET_REGISTRY_FILENAME = "market_processed_registry.json"
GDELT_REGISTRY_FILENAME = "gdelt_processed_registry.json"
CHUNK_CACHE_DIRNAME = "gdelt_chunks"
MANIFEST_PREFIX = "daily_market_preprocessor_manifest"
REGISTRY_SCHEMA_VERSION = 2

EVENT_DEFAULT_INDEX = {
    "id": 0,
    "date": 1,
    "event_code": 27,
    "event_root_code": 29,
    "quad_class": 30,
    "goldstein": 31,
    "num_mentions": 32,
    "num_sources": 33,
    "num_articles": 34,
    "avg_tone": 35,
    "dateadded": 56,
}

GKG_DEFAULT_INDEX = {
    "id": 0,
    "date": 1,
    "themes": 7,
    "enhanced_themes": 8,
    "locations": 9,
    "enhanced_locations": 10,
    "persons": 11,
    "enhanced_persons": 12,
    "organizations": 13,
    "enhanced_organizations": 14,
    "tone": 15,
}

EVENT_METRIC_COLUMNS = [
    "event_row_count",
    "sum_num_mentions",
    "sum_num_sources",
    "sum_num_articles",
    "tone_weighted_sum",
    "goldstein_weighted_sum",
    "goldstein_unweighted_sum",
    "positive_tone_events",
    "negative_tone_events",
    "neutral_tone_events",
    "quad_1_count",
    "quad_2_count",
    "quad_3_count",
    "quad_4_count",
    "root_01_count",
    "root_02_count",
    "root_03_count",
    "root_04_count",
    "root_05_count",
    "root_06_count",
    "root_07_count",
    "root_08_count",
    "root_09_count",
    "root_10_count",
    "root_11_count",
    "root_12_count",
    "root_13_count",
    "root_14_count",
    "root_15_count",
    "root_16_count",
    "root_17_count",
    "root_18_count",
    "root_19_count",
    "root_20_count",
]

GKG_METRIC_COLUMNS = [
    "gkg_row_count",
    "theme_token_count",
    "person_token_count",
    "organization_token_count",
    "location_token_count",
    "docs_with_themes",
    "docs_with_persons",
    "docs_with_organizations",
    "docs_with_locations",
    "tone_sum",
    "positive_score_sum",
    "negative_score_sum",
    "polarity_sum",
]


# -----------------------------------------------------------------------------
# GENERIC HELPERS
# -----------------------------------------------------------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="")
    tmp.replace(path)


def _atomic_write_json(path: Path, payload: dict) -> None:
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=False))


def _tail_last_nonempty_line(path: Path, max_bytes: int = 65536) -> Optional[str]:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        size = path.stat().st_size
        with path.open("rb") as f:
            chunk_size = 8192
            read_bytes = 0
            data = b""
            while read_bytes < max_bytes:
                step = min(chunk_size, size - read_bytes)
                if step <= 0:
                    break
                f.seek(size - read_bytes - step)
                data = f.read(step) + data
                read_bytes += step
                if b"\n" in data:
                    break
            for line in reversed(data.splitlines()):
                s = line.decode("utf-8", errors="ignore").strip()
                if s:
                    return s
    except Exception:
        return None
    return None


def _read_last_date_from_ohlcv_csv(csv_path: Path) -> Optional[str]:
    last = _tail_last_nonempty_line(csv_path)
    if not last or last.lower().startswith("date,"):
        return None
    parts = [p.strip() for p in last.split(",")]
    return parts[0] if parts else None


def _stooq_date_yyyymmdd_to_iso(d: str) -> Optional[str]:
    d = (d or "").strip()
    if len(d) != 8 or not d.isdigit():
        return None
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"


def _gkg_date_to_iso(d: str) -> Optional[str]:
    d = (d or "").strip()
    digits = re.sub(r"\D", "", d)
    if len(digits) < 8:
        return None
    return f"{digits[0:4]}-{digits[4:6]}-{digits[6:8]}"


def _ticker_from_stooq_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    return s.split(".", 1)[0] if "." in s else s


def _metadata_file_category(path: Path) -> Optional[str]:
    name = path.name.upper()
    if name in {"MASTER", "EMASTER", "XMASTER"}:
        return "metastock_index"
    if re.fullmatch(r"F\d+\.DOP", name):
        return "metastock_dop"
    return None


def _float_or_zero(value: object) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _intish(value: float) -> bool:
    return abs(value - round(value)) < 1e-9


def _fmt_num(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        if _intish(value):
            return str(int(round(value)))
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_text(text: str) -> str:
    return _sha256_bytes(text.encode("utf-8", errors="ignore"))


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _relpath_or_name(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return path.name


def _folder_snapshot(root: Path) -> dict:
    if not root.exists():
        return {
            "path": str(root),
            "exists": False,
            "file_count": 0,
            "total_bytes": 0,
            "fingerprint": None,
        }

    files = sorted([p for p in root.rglob("*") if p.is_file()])
    total_bytes = 0
    digest = hashlib.sha256()
    for file_path in files:
        stat = file_path.stat()
        total_bytes += stat.st_size
        rel = _relpath_or_name(file_path, root)
        digest.update(rel.encode("utf-8", errors="ignore"))
        digest.update(str(stat.st_size).encode("utf-8"))
        digest.update(str(stat.st_mtime_ns).encode("utf-8"))

    return {
        "path": str(root),
        "exists": True,
        "file_count": len(files),
        "total_bytes": total_bytes,
        "fingerprint": digest.hexdigest(),
    }


def _scan_new_daily_tree(root: Path) -> dict:
    if not root.exists():
        return {"candidates": [], "ignored_metadata": []}

    candidates: list[Path] = []
    ignored_metadata: list[dict] = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if STATE_DIRNAME in path.parts:
            continue

        metadata_category = _metadata_file_category(path)
        if metadata_category:
            ignored_metadata.append({
                "path": str(path),
                "name": path.name,
                "category": metadata_category,
                "size_bytes": path.stat().st_size,
            })
            continue

        name = path.name.lower()
        if name.endswith(".us.txt") or name.endswith(".txt"):
            candidates.append(path)

    return {
        "candidates": sorted(candidates),
        "ignored_metadata": sorted(ignored_metadata, key=lambda x: (x["category"], x["name"], x["path"])),
    }


def _scan_gdelt_tree(root: Path) -> dict:
    if not root.exists():
        return {"candidates": [], "ignored": []}

    candidates: list[Path] = []
    ignored: list[dict] = []
    allowed_suffixes = {".csv", ".tsv", ".txt", ".gz", ".zip"}

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if STATE_DIRNAME in path.parts:
            continue

        suffix = path.suffix.lower()
        if suffix in allowed_suffixes:
            candidates.append(path)
        else:
            ignored.append({
                "path": str(path),
                "name": path.name,
                "suffix": suffix,
                "size_bytes": path.stat().st_size,
                "category": "non_gdelt_candidate",
            })

    return {
        "candidates": sorted(candidates),
        "ignored": sorted(ignored, key=lambda x: (x["suffix"], x["name"], x["path"])),
    }


def _read_existing_rows(csv_path: Path) -> list[tuple[str, str, str, str, str, str]]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return []
    rows: list[tuple[str, str, str, str, str, str]] = []
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        for row in csv.DictReader(f):
            date = (row.get("date") or "").strip()
            if not date:
                continue
            rows.append((
                date,
                str(row.get("open", "")).strip(),
                str(row.get("high", "")).strip(),
                str(row.get("low", "")).strip(),
                str(row.get("close", "")).strip(),
                str(row.get("volume", "")).strip(),
            ))
    return rows


def _atomic_write_rows(csv_path: Path, rows: list[tuple[str, str, str, str, str, str]]) -> None:
    tmp_path = csv_path.with_suffix(f"{csv_path.suffix}.tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as out:
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(["date", "open", "high", "low", "close", "volume"])
        writer.writerows(rows)
    tmp_path.replace(csv_path)


def _default_metric_row(columns: list[str]) -> dict[str, float]:
    return {c: 0.0 for c in columns}


def _add_metric_row(target: dict[str, float], source: dict[str, float]) -> None:
    for k, v in source.items():
        target[k] = target.get(k, 0.0) + float(v)


def _merge_daily_metrics(bucket: dict[str, dict[str, float]], day: str, contrib: dict[str, float], columns: list[str]) -> None:
    row = bucket.setdefault(day, _default_metric_row(columns))
    _add_metric_row(row, contrib)


def _header_index(header: Optional[dict[str, int]], names: list[str], default_index: int) -> int:
    if not header:
        return default_index
    for name in names:
        key = name.upper()
        if key in header:
            return header[key]
    return default_index


def _split_semicolon_count(value: object) -> int:
    s = str(value or "").strip()
    if not s:
        return 0
    return sum(1 for part in s.split(";") if part.strip())


def _choose_gkg_count(enhanced_value: object, basic_value: object) -> int:
    n = _split_semicolon_count(enhanced_value)
    if n:
        return n
    return _split_semicolon_count(basic_value)


def _parse_gkg_tone(value: object) -> tuple[float, float, float, float]:
    s = str(value or "").strip()
    if not s:
        return 0.0, 0.0, 0.0, 0.0
    parts = [p.strip() for p in s.split(",")]
    vals = [_float_or_zero(parts[i]) if i < len(parts) else 0.0 for i in range(4)]
    return vals[0], vals[1], vals[2], vals[3]


def _detect_delimiter_from_line(line: str) -> str:
    return "\t" if line.count("\t") >= line.count(",") else ","


def _detect_gdelt_dataset_type(name: str, first_row: list[str]) -> Optional[str]:
    joined = "|".join(first_row).upper()
    upper_name = name.upper()
    if "GKGRECORDID" in joined or "GKG" in upper_name:
        return "gkg"
    if "GLOBALEVENTID" in joined:
        return "events"
    if first_row:
        first = first_row[0].strip()
        if re.fullmatch(r"\d{14}-T?\d+", first):
            return "gkg"
        if first.isdigit() and len(first_row) >= 50:
            return "events"
    if ".EXPORT" in upper_name or "EVENT" in upper_name:
        return "events"
    return None


def _iter_text_members(path: Path) -> Iterator[tuple[str, TextIO]]:
    suffix = path.suffix.lower()
    if suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8", errors="ignore", newline="") as f:
            yield path.name, f
        return

    if suffix == ".zip":
        with zipfile.ZipFile(path) as zf:
            for member in sorted(zf.namelist()):
                if member.endswith("/"):
                    continue
                if Path(member).name.startswith("."):
                    continue
                with zf.open(member, "r") as raw:
                    text = raw.read().decode("utf-8", errors="ignore")
                from io import StringIO
                yield member, StringIO(text)
        return

    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        yield path.name, f


def _read_first_nonempty_line(stream: TextIO) -> Optional[str]:
    for line in stream:
        if line.strip():
            return line
    return None


def _parse_event_reader(reader: csv.reader, header: Optional[dict[str, int]]) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
    idx_id = _header_index(header, ["GlobalEventID"], EVENT_DEFAULT_INDEX["id"])
    idx_date = _header_index(header, ["SQLDATE", "Day"], EVENT_DEFAULT_INDEX["date"])
    idx_root = _header_index(header, ["EventRootCode"], EVENT_DEFAULT_INDEX["event_root_code"])
    idx_quad = _header_index(header, ["QuadClass"], EVENT_DEFAULT_INDEX["quad_class"])
    idx_gold = _header_index(header, ["GoldsteinScale"], EVENT_DEFAULT_INDEX["goldstein"])
    idx_mentions = _header_index(header, ["NumMentions"], EVENT_DEFAULT_INDEX["num_mentions"])
    idx_sources = _header_index(header, ["NumSources"], EVENT_DEFAULT_INDEX["num_sources"])
    idx_articles = _header_index(header, ["NumArticles"], EVENT_DEFAULT_INDEX["num_articles"])
    idx_tone = _header_index(header, ["AvgTone"], EVENT_DEFAULT_INDEX["avg_tone"])
    idx_dateadded = _header_index(header, ["DATEADDED"], EVENT_DEFAULT_INDEX["dateadded"])

    daily: dict[str, dict[str, float]] = {}
    stats = {"rows": 0, "bad_rows": 0}

    for row in reader:
        if not row or not any(str(cell).strip() for cell in row):
            continue
        if len(row) <= max(idx_id, idx_date, idx_root, idx_quad, idx_gold, idx_mentions, idx_sources, idx_articles, idx_tone, idx_dateadded):
            stats["bad_rows"] += 1
            continue

        day = _stooq_date_yyyymmdd_to_iso(str(row[idx_date]).strip())
        if not day:
            day = _stooq_date_yyyymmdd_to_iso(str(row[idx_dateadded]).strip()[:8])
        if not day:
            stats["bad_rows"] += 1
            continue

        mentions = max(_float_or_zero(row[idx_mentions]), 1.0)
        tone = _float_or_zero(row[idx_tone])
        goldstein = _float_or_zero(row[idx_gold])
        quad = str(row[idx_quad]).strip()
        root_code = str(row[idx_root]).strip().zfill(2)[:2]

        contrib = _default_metric_row(EVENT_METRIC_COLUMNS)
        contrib["event_row_count"] = 1.0
        contrib["sum_num_mentions"] = mentions
        contrib["sum_num_sources"] = _float_or_zero(row[idx_sources])
        contrib["sum_num_articles"] = _float_or_zero(row[idx_articles])
        contrib["tone_weighted_sum"] = tone * mentions
        contrib["goldstein_weighted_sum"] = goldstein * mentions
        contrib["goldstein_unweighted_sum"] = goldstein
        if tone > 0:
            contrib["positive_tone_events"] = 1.0
        elif tone < 0:
            contrib["negative_tone_events"] = 1.0
        else:
            contrib["neutral_tone_events"] = 1.0
        if quad in {"1", "2", "3", "4"}:
            contrib[f"quad_{quad}_count"] = 1.0
        if root_code.isdigit() and 1 <= int(root_code) <= 20:
            contrib[f"root_{root_code}_count"] = 1.0

        _merge_daily_metrics(daily, day, contrib, EVENT_METRIC_COLUMNS)
        stats["rows"] += 1

    return daily, stats


def _parse_gkg_reader(reader: csv.reader, header: Optional[dict[str, int]]) -> tuple[dict[str, dict[str, float]], dict[str, int]]:
    idx_id = _header_index(header, ["GKGRECORDID"], GKG_DEFAULT_INDEX["id"])
    idx_date = _header_index(header, ["DATE", "DATE", "DATEADDED"], GKG_DEFAULT_INDEX["date"])
    idx_themes = _header_index(header, ["V1THEMES", "Themes"], GKG_DEFAULT_INDEX["themes"])
    idx_v2themes = _header_index(header, ["V2ENHANCEDTHEMES", "V2Themes"], GKG_DEFAULT_INDEX["enhanced_themes"])
    idx_locations = _header_index(header, ["V1LOCATIONS", "Locations"], GKG_DEFAULT_INDEX["locations"])
    idx_v2locations = _header_index(header, ["V2ENHANCEDLOCATIONS", "V2Locations"], GKG_DEFAULT_INDEX["enhanced_locations"])
    idx_persons = _header_index(header, ["V1PERSONS", "Persons"], GKG_DEFAULT_INDEX["persons"])
    idx_v2persons = _header_index(header, ["V2ENHANCEDPERSONS", "V2Persons"], GKG_DEFAULT_INDEX["enhanced_persons"])
    idx_orgs = _header_index(header, ["V1ORGANIZATIONS", "Organizations"], GKG_DEFAULT_INDEX["organizations"])
    idx_v2orgs = _header_index(header, ["V2ENHANCEDORGANIZATIONS", "V2Organizations"], GKG_DEFAULT_INDEX["enhanced_organizations"])
    idx_tone = _header_index(header, ["V2TONE", "Tone"], GKG_DEFAULT_INDEX["tone"])

    daily: dict[str, dict[str, float]] = {}
    stats = {"rows": 0, "bad_rows": 0}

    for row in reader:
        if not row or not any(str(cell).strip() for cell in row):
            continue
        if len(row) <= max(idx_id, idx_date, idx_themes, idx_v2themes, idx_locations, idx_v2locations, idx_persons, idx_v2persons, idx_orgs, idx_v2orgs, idx_tone):
            stats["bad_rows"] += 1
            continue

        day = _gkg_date_to_iso(str(row[idx_date]).strip())
        if not day:
            stats["bad_rows"] += 1
            continue

        theme_count = _choose_gkg_count(row[idx_v2themes], row[idx_themes])
        person_count = _choose_gkg_count(row[idx_v2persons], row[idx_persons])
        org_count = _choose_gkg_count(row[idx_v2orgs], row[idx_orgs])
        loc_count = _choose_gkg_count(row[idx_v2locations], row[idx_locations])
        tone, pos, neg, polarity = _parse_gkg_tone(row[idx_tone])

        contrib = _default_metric_row(GKG_METRIC_COLUMNS)
        contrib["gkg_row_count"] = 1.0
        contrib["theme_token_count"] = float(theme_count)
        contrib["person_token_count"] = float(person_count)
        contrib["organization_token_count"] = float(org_count)
        contrib["location_token_count"] = float(loc_count)
        contrib["docs_with_themes"] = 1.0 if theme_count else 0.0
        contrib["docs_with_persons"] = 1.0 if person_count else 0.0
        contrib["docs_with_organizations"] = 1.0 if org_count else 0.0
        contrib["docs_with_locations"] = 1.0 if loc_count else 0.0
        contrib["tone_sum"] = tone
        contrib["positive_score_sum"] = pos
        contrib["negative_score_sum"] = neg
        contrib["polarity_sum"] = polarity

        _merge_daily_metrics(daily, day, contrib, GKG_METRIC_COLUMNS)
        stats["rows"] += 1

    return daily, stats


def _parse_gdelt_member(member_name: str, stream: TextIO) -> tuple[Optional[str], dict[str, dict[str, float]], dict[str, dict[str, float]], dict]:
    first_line = _read_first_nonempty_line(stream)
    if first_line is None:
        return None, {}, {}, {"member_name": member_name, "status": "empty"}

    delimiter = _detect_delimiter_from_line(first_line)
    first_row = next(csv.reader([first_line], delimiter=delimiter))
    dataset_type = _detect_gdelt_dataset_type(member_name, first_row)
    header = None

    if first_row and first_row[0].strip().upper() in {"GLOBALEVENTID", "GKGRECORDID"}:
        header = {str(v).strip().upper(): i for i, v in enumerate(first_row)}
        rows_iter = csv.reader(stream, delimiter=delimiter)
    else:
        rows_iter = csv.reader(itertools.chain([first_line], stream), delimiter=delimiter)

    if dataset_type == "events":
        daily_events, stats = _parse_event_reader(rows_iter, header)
        return "events", daily_events, {}, {"member_name": member_name, "status": "parsed", **stats}
    if dataset_type == "gkg":
        daily_gkg, stats = _parse_gkg_reader(rows_iter, header)
        return "gkg", {}, daily_gkg, {"member_name": member_name, "status": "parsed", **stats}

    return None, {}, {}, {"member_name": member_name, "status": "skipped_unknown_layout"}


def _chunk_cache_dir(working_csv_files_dir: Path) -> Path:
    return working_csv_files_dir / STATE_DIRNAME / CHUNK_CACHE_DIRNAME


def _chunk_cache_path(working_csv_files_dir: Path, source_sha256: str) -> Path:
    return _chunk_cache_dir(working_csv_files_dir) / f"{source_sha256}.json"


def _build_gdelt_chunk(path: Path, source_sha256: str) -> dict:
    events_daily: dict[str, dict[str, float]] = {}
    gkg_daily: dict[str, dict[str, float]] = {}
    member_summaries: list[dict] = []
    detected_types: set[str] = set()

    for member_name, stream in _iter_text_members(path):
        dataset_type, member_events, member_gkg, summary = _parse_gdelt_member(member_name, stream)
        member_summaries.append(summary)
        if dataset_type == "events":
            detected_types.add("events")
            for day, contrib in member_events.items():
                _merge_daily_metrics(events_daily, day, contrib, EVENT_METRIC_COLUMNS)
        elif dataset_type == "gkg":
            detected_types.add("gkg")
            for day, contrib in member_gkg.items():
                _merge_daily_metrics(gkg_daily, day, contrib, GKG_METRIC_COLUMNS)

    return {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "source_name": path.name,
        "source_path": str(path),
        "source_sha256": source_sha256,
        "created_utc": _now_utc_iso(),
        "detected_types": sorted(detected_types),
        "member_summaries": member_summaries,
        "events_daily": events_daily,
        "gkg_daily": gkg_daily,
    }


def _load_or_build_gdelt_chunk(path: Path, working_csv_files_dir: Path) -> tuple[dict, str]:
    source_sha256 = _sha256_file(path)
    cache_path = _chunk_cache_path(working_csv_files_dir, source_sha256)
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return json.loads(cache_path.read_text(encoding="utf-8")), "cache"

    chunk = _build_gdelt_chunk(path, source_sha256)
    _safe_mkdir(cache_path.parent)
    _atomic_write_json(cache_path, chunk)
    return chunk, "parsed"


def _market_registry_path(working_csv_files_dir: Path) -> Path:
    return working_csv_files_dir / STATE_DIRNAME / MARKET_REGISTRY_FILENAME


def _gdelt_registry_path(working_csv_files_dir: Path) -> Path:
    return working_csv_files_dir / STATE_DIRNAME / GDELT_REGISTRY_FILENAME


def _load_registry(path: Path, entries_key: str = "entries") -> dict:
    if not path.exists() or path.stat().st_size == 0:
        return {
            "schema_version": REGISTRY_SCHEMA_VERSION,
            "created_utc": _now_utc_iso(),
            "updated_utc": _now_utc_iso(),
            entries_key: {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Registry payload is not a dict.")
        payload.setdefault("schema_version", REGISTRY_SCHEMA_VERSION)
        payload.setdefault("created_utc", _now_utc_iso())
        payload.setdefault("updated_utc", _now_utc_iso())
        payload.setdefault(entries_key, {})
        return payload
    except Exception:
        backup = path.with_suffix(path.suffix + f".corrupt_{_now_stamp()}")
        try:
            path.replace(backup)
        except Exception:
            pass
        return {
            "schema_version": REGISTRY_SCHEMA_VERSION,
            "created_utc": _now_utc_iso(),
            "updated_utc": _now_utc_iso(),
            entries_key: {},
            "recovered_from_corrupt_registry": str(backup),
        }


def _save_registry(path: Path, registry: dict) -> None:
    registry["updated_utc"] = _now_utc_iso()
    _safe_mkdir(path.parent)
    _atomic_write_json(path, registry)


def _signature_for_processed_source(source_sha256: str, ticker: str) -> str:
    return _sha256_text(f"{source_sha256}|{ticker.upper()}")


# -----------------------------------------------------------------------------
# UPDATE / PREPROCESSING CORE
# -----------------------------------------------------------------------------
@dataclass
class UpdateResult:
    ticker: str
    source: str
    out_csv: str
    last_existing_date: Optional[str]
    rows_appended: int
    status: str
    source_sha256: Optional[str] = None
    registry_signature: Optional[str] = None
    error: Optional[str] = None


def _build_event_daily_rows(events_daily: dict[str, dict[str, float]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for day in sorted(events_daily):
        m = events_daily[day]
        mentions = m.get("sum_num_mentions", 0.0)
        event_rows = m.get("event_row_count", 0.0)
        rows.append({
            "date": day,
            **{c: m.get(c, 0.0) for c in EVENT_METRIC_COLUMNS},
            "weighted_avg_tone": (m.get("tone_weighted_sum", 0.0) / mentions) if mentions else 0.0,
            "weighted_avg_goldstein": (m.get("goldstein_weighted_sum", 0.0) / mentions) if mentions else 0.0,
            "avg_goldstein_per_event": (m.get("goldstein_unweighted_sum", 0.0) / event_rows) if event_rows else 0.0,
        })
    return rows


def _build_gkg_daily_rows(gkg_daily: dict[str, dict[str, float]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for day in sorted(gkg_daily):
        m = gkg_daily[day]
        row_count = m.get("gkg_row_count", 0.0)
        rows.append({
            "date": day,
            **{c: m.get(c, 0.0) for c in GKG_METRIC_COLUMNS},
            "avg_tone": (m.get("tone_sum", 0.0) / row_count) if row_count else 0.0,
            "avg_positive_score": (m.get("positive_score_sum", 0.0) / row_count) if row_count else 0.0,
            "avg_negative_score": (m.get("negative_score_sum", 0.0) / row_count) if row_count else 0.0,
            "avg_polarity": (m.get("polarity_sum", 0.0) / row_count) if row_count else 0.0,
        })
    return rows


def _write_dict_rows_csv(path: Path, rows: list[dict[str, object]]) -> None:
    _safe_mkdir(path.parent)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _fmt_num(v) for k, v in row.items()})
    tmp_path.replace(path)


def _build_join_ready_rows(event_rows: list[dict[str, object]], gkg_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    event_by_date = {str(r["date"]): r for r in event_rows}
    gkg_by_date = {str(r["date"]): r for r in gkg_rows}
    all_dates = sorted(set(event_by_date) | set(gkg_by_date))
    joined: list[dict[str, object]] = []
    for day in all_dates:
        row: dict[str, object] = {"date": day}
        event_row = event_by_date.get(day, {})
        gkg_row = gkg_by_date.get(day, {})
        for k, v in event_row.items():
            if k != "date":
                row[f"event_{k}"] = v
        for k, v in gkg_row.items():
            if k != "date":
                row[f"gkg_{k}"] = v
        joined.append(row)
    return joined


def _build_baseline_rows(joined_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not joined_rows:
        return []

    columns = [c for c in joined_rows[0].keys() if c != "date"]
    out: list[dict[str, object]] = []
    for col in columns:
        values: list[float] = []
        for row in joined_rows:
            val = row.get(col, 0.0)
            try:
                values.append(float(val or 0.0))
            except Exception:
                pass
        if not values:
            continue
        n = len(values)
        mean = sum(values) / n
        if n > 1:
            variance = sum((v - mean) ** 2 for v in values) / (n - 1)
            stdev = math.sqrt(max(variance, 0.0))
        else:
            stdev = 0.0
        out.append({
            "dataset": "gdelt_daily_join_ready",
            "column": col,
            "n_days": n,
            "mean": mean,
            "stdev": stdev,
            "min": min(values),
            "max": max(values),
        })
    return out


def _build_gdelt_caches(gdelt_corpus_dir: Path, working_csv_files_dir: Path, dry_run: bool = False) -> dict:
    registry_path = _gdelt_registry_path(working_csv_files_dir)
    registry = _load_registry(registry_path, entries_key="entries_by_relpath")
    scan = _scan_gdelt_tree(gdelt_corpus_dir)
    candidates = scan["candidates"]
    ignored = scan["ignored"]

    events_daily: dict[str, dict[str, float]] = {}
    gkg_daily: dict[str, dict[str, float]] = {}
    seen_shas: set[str] = set()
    duplicate_content_files: list[dict] = []
    chunk_cache_hits = 0
    chunk_parsed = 0
    unknown_layout = 0
    parsed_sources = 0
    event_sources = 0
    gkg_sources = 0
    errors: list[dict] = []

    entries_by_relpath: dict[str, dict] = {}
    sha_index: dict[str, list[str]] = defaultdict(list)

    for src in candidates:
        rel = _relpath_or_name(src, gdelt_corpus_dir)
        try:
            chunk, load_mode = _load_or_build_gdelt_chunk(src, working_csv_files_dir)
            parsed_sources += 1
            if load_mode == "cache":
                chunk_cache_hits += 1
            else:
                chunk_parsed += 1

            source_sha256 = str(chunk.get("source_sha256") or _sha256_file(src))
            sha_index[source_sha256].append(rel)

            if source_sha256 in seen_shas:
                duplicate_content_files.append({
                    "path": str(src),
                    "rel_path": rel,
                    "source_sha256": source_sha256,
                    "status": "skipped_duplicate_content",
                })
                entries_by_relpath[rel] = {
                    "source_path": str(src),
                    "rel_path": rel,
                    "source_sha256": source_sha256,
                    "source_size_bytes": src.stat().st_size,
                    "source_mtime_ns": src.stat().st_mtime_ns,
                    "status": "skipped_duplicate_content",
                    "detected_types": chunk.get("detected_types", []),
                    "chunk_cache_path": str(_chunk_cache_path(working_csv_files_dir, source_sha256)),
                    "member_summaries": chunk.get("member_summaries", []),
                    "processed_utc": _now_utc_iso(),
                }
                continue

            seen_shas.add(source_sha256)
            member_summaries = chunk.get("member_summaries", [])
            if not chunk.get("detected_types"):
                unknown_layout += 1

            if "events" in chunk.get("detected_types", []):
                event_sources += 1
                for day, contrib in (chunk.get("events_daily") or {}).items():
                    _merge_daily_metrics(events_daily, day, contrib, EVENT_METRIC_COLUMNS)
            if "gkg" in chunk.get("detected_types", []):
                gkg_sources += 1
                for day, contrib in (chunk.get("gkg_daily") or {}).items():
                    _merge_daily_metrics(gkg_daily, day, contrib, GKG_METRIC_COLUMNS)

            entries_by_relpath[rel] = {
                "source_path": str(src),
                "rel_path": rel,
                "source_sha256": source_sha256,
                "source_size_bytes": src.stat().st_size,
                "source_mtime_ns": src.stat().st_mtime_ns,
                "status": "processed",
                "detected_types": chunk.get("detected_types", []),
                "chunk_cache_path": str(_chunk_cache_path(working_csv_files_dir, source_sha256)),
                "member_summaries": member_summaries,
                "processed_utc": _now_utc_iso(),
            }
        except Exception as e:
            errors.append({"source": str(src), "rel_path": rel, "error": repr(e)})
            entries_by_relpath[rel] = {
                "source_path": str(src),
                "rel_path": rel,
                "status": "error",
                "error": repr(e),
                "processed_utc": _now_utc_iso(),
            }

    event_rows = _build_event_daily_rows(events_daily)
    gkg_rows = _build_gkg_daily_rows(gkg_daily)
    joined_rows = _build_join_ready_rows(event_rows, gkg_rows)
    baseline_rows = _build_baseline_rows(joined_rows)

    state_root = working_csv_files_dir / STATE_DIRNAME
    events_out = state_root / "gdelt_events_daily_aggregates.csv"
    gkg_out = state_root / "gdelt_gkg_daily_aggregates.csv"
    join_out = state_root / "gdelt_daily_join_ready.csv"
    baseline_out = state_root / "gdelt_normalization_baselines.csv"

    if not dry_run:
        _write_dict_rows_csv(events_out, event_rows)
        _write_dict_rows_csv(gkg_out, gkg_rows)
        _write_dict_rows_csv(join_out, joined_rows)
        _write_dict_rows_csv(baseline_out, baseline_rows)

        registry["entries_by_relpath"] = entries_by_relpath
        registry["sha_index"] = dict(sha_index)
        registry["last_run_summary"] = {
            "timestamp_utc": _now_utc_iso(),
            "candidate_file_count": len(candidates),
            "duplicate_content_count": len(duplicate_content_files),
            "events_daily_rows": len(event_rows),
            "gkg_daily_rows": len(gkg_rows),
            "join_ready_rows": len(joined_rows),
            "baseline_rows": len(baseline_rows),
        }
        _save_registry(registry_path, registry)

    return {
        "gdelt_corpus_dir": str(gdelt_corpus_dir),
        "registry_path": str(registry_path),
        "events_daily_aggregates_csv": str(events_out),
        "gkg_daily_aggregates_csv": str(gkg_out),
        "daily_join_ready_csv": str(join_out),
        "normalization_baselines_csv": str(baseline_out),
        "counts": {
            "candidate_files": len(candidates),
            "ignored_non_candidates": len(ignored),
            "parsed_sources": parsed_sources,
            "chunk_cache_hits": chunk_cache_hits,
            "chunk_parsed": chunk_parsed,
            "duplicate_content_files": len(duplicate_content_files),
            "unknown_layout_files": unknown_layout,
            "event_sources": event_sources,
            "gkg_sources": gkg_sources,
            "events_daily_rows": len(event_rows),
            "gkg_daily_rows": len(gkg_rows),
            "join_ready_rows": len(joined_rows),
            "baseline_rows": len(baseline_rows),
            "errors": len(errors),
        },
        "ignored_files": ignored,
        "duplicate_content_files": duplicate_content_files,
        "errors": errors,
    }


def preprocess_market_data(
    new_daily_files_dir: Path,
    gdelt_corpus_dir: Path,
    working_csv_files_dir: Path,
    dry_run: bool = False,
    global_cutoff_date: Optional[str] = None,
) -> dict:
    t0 = time.time()
    _safe_mkdir(working_csv_files_dir)

    market_registry_file = _market_registry_path(working_csv_files_dir)
    market_registry = _load_registry(market_registry_file, entries_key="entries")

    gdelt_snapshot = _folder_snapshot(gdelt_corpus_dir)
    scan = _scan_new_daily_tree(new_daily_files_dir)
    files = scan["candidates"]
    ignored_metadata = scan["ignored_metadata"]

    results: list[UpdateResult] = []
    errors: list[dict] = []
    processed = 0
    updated = 0
    total_appended = 0
    skipped_empty = 0
    skipped_already_processed = 0
    skipped_non_stooq = 0
    skipped_metadata = len(ignored_metadata)

    for src in files:
        processed += 1
        try:
            if src.stat().st_size == 0:
                skipped_empty += 1
                results.append(UpdateResult("", str(src), "", None, 0, "skipped_empty"))
                continue

            source_sha256 = _sha256_file(src)
            source_rel = _relpath_or_name(src, new_daily_files_dir)

            with src.open("r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.reader(f)
                _ = next(reader, None)
                rows = [r for r in reader if r and any(str(cell).strip() for cell in r) and len(r) >= 9]

            if not rows:
                skipped_non_stooq += 1
                results.append(UpdateResult("", str(src), "", None, 0, "skipped_no_data", source_sha256=source_sha256))
                continue

            ticker = _ticker_from_stooq_symbol(rows[0][0])
            if not ticker:
                skipped_non_stooq += 1
                results.append(UpdateResult("", str(src), "", None, 0, "skipped_bad_ticker", source_sha256=source_sha256))
                continue

            registry_signature = _signature_for_processed_source(source_sha256=source_sha256, ticker=ticker)
            existing_registry_entry = market_registry.get("entries", {}).get(registry_signature)
            out_csv = working_csv_files_dir / f"{ticker}.csv"
            if existing_registry_entry and out_csv.exists():
                skipped_already_processed += 1
                results.append(
                    UpdateResult(
                        ticker=ticker,
                        source=str(src),
                        out_csv=str(out_csv),
                        last_existing_date=_read_last_date_from_ohlcv_csv(out_csv),
                        rows_appended=0,
                        status="skipped_already_processed",
                        source_sha256=source_sha256,
                        registry_signature=registry_signature,
                    )
                )
                continue

            last_existing = global_cutoff_date or _read_last_date_from_ohlcv_csv(out_csv)

            appended_rows: list[tuple[str, str, str, str, str, str]] = []
            for r in rows:
                period = (r[1] or "").strip().upper()
                if period != "D":
                    continue
                iso = _stooq_date_yyyymmdd_to_iso(r[2])
                if not iso:
                    continue
                if last_existing and iso <= last_existing:
                    continue
                appended_rows.append((
                    iso,
                    str(r[4]).strip(),
                    str(r[5]).strip(),
                    str(r[6]).strip(),
                    str(r[7]).strip(),
                    str(r[8]).strip(),
                ))

            status = "no_change"
            if appended_rows:
                status = "would_update" if dry_run else "updated"
                if not dry_run:
                    existing_rows = _read_existing_rows(out_csv)
                    merged = existing_rows + appended_rows
                    deduped = {row[0]: row for row in merged}
                    ordered = [deduped[d] for d in sorted(deduped)]
                    _safe_mkdir(out_csv.parent)
                    _atomic_write_rows(out_csv, ordered)
                updated += 1
                total_appended += len(appended_rows)

            results.append(
                UpdateResult(
                    ticker=ticker,
                    source=str(src),
                    out_csv=str(out_csv),
                    last_existing_date=last_existing,
                    rows_appended=len(appended_rows),
                    status=status,
                    source_sha256=source_sha256,
                    registry_signature=registry_signature,
                )
            )

            if not dry_run:
                market_registry.setdefault("entries", {})[registry_signature] = {
                    "signature": registry_signature,
                    "ticker": ticker,
                    "source_path": str(src),
                    "source_rel_path": source_rel,
                    "source_sha256": source_sha256,
                    "source_size_bytes": src.stat().st_size,
                    "source_mtime_ns": src.stat().st_mtime_ns,
                    "working_csv": str(out_csv),
                    "status": status,
                    "rows_appended": len(appended_rows),
                    "last_existing_date": last_existing,
                    "processed_utc": _now_utc_iso(),
                }

        except Exception as e:
            errors.append({"source": str(src), "error": repr(e)})
            results.append(UpdateResult("", str(src), "", None, 0, "error", error=repr(e)))

    if not dry_run:
        _save_registry(market_registry_file, market_registry)

    gdelt_cache = _build_gdelt_caches(
        gdelt_corpus_dir=gdelt_corpus_dir,
        working_csv_files_dir=working_csv_files_dir,
        dry_run=dry_run,
    )

    return {
        "timestamp_utc": _now_utc_iso(),
        "new_daily_files_dir": str(new_daily_files_dir),
        "gdelt_corpus_dir": str(gdelt_corpus_dir),
        "working_csv_files_dir": str(working_csv_files_dir),
        "dry_run": dry_run,
        "global_cutoff_date": global_cutoff_date,
        "gdelt_corpus_snapshot": gdelt_snapshot,
        "market_registry_path": str(market_registry_file),
        "counts": {
            "files_found": len(files),
            "processed": processed,
            "updated_tickers": updated,
            "total_rows_appended": total_appended,
            "skipped_empty": skipped_empty,
            "skipped_non_stooq": skipped_non_stooq,
            "ignored_metadata_files": skipped_metadata,
            "skipped_already_processed": skipped_already_processed,
            "errors": len(errors),
        },
        "ignored_metadata_files": ignored_metadata,
        "results": [r.__dict__ for r in results],
        "errors": errors,
        "gdelt_cache": gdelt_cache,
        "elapsed_seconds": round(time.time() - t0, 3),
    }


# -----------------------------------------------------------------------------
# GUI
# -----------------------------------------------------------------------------
def _run_gui() -> None:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk

    root = tk.Tk()
    root.title(APP_TITLE)

    new_daily_var = tk.StringVar(value=str(HARDCODED_PATHS["new_daily_files"]))
    gdelt_var = tk.StringVar(value=str(HARDCODED_PATHS["gdelt_corpus"]))
    working_csv_var = tk.StringVar(value=str(HARDCODED_PATHS["working_csv_files"]))
    dryrun_var = tk.BooleanVar(value=False)
    cutoff_var = tk.StringVar()
    status_var = tk.StringVar(value="Idle.")

    def browse_dir(var: tk.StringVar) -> None:
        p = filedialog.askdirectory()
        if p:
            var.set(p)

    def append_log(msg: str) -> None:
        log.configure(state="normal")
        log.insert("end", msg + "\n")
        log.see("end")
        log.configure(state="disabled")

    def run_clicked() -> None:
        new_daily = new_daily_var.get().strip()
        gdelt = gdelt_var.get().strip()
        working = working_csv_var.get().strip()

        if not new_daily or not gdelt or not working:
            messagebox.showerror("Missing paths", "Set New Daily Files, GDELT Corpus, and Working .CSV Files.")
            return

        btn_run.configure(state="disabled")
        progress.start(10)
        status_var.set("Running...")

        def worker() -> None:
            try:
                manifest = preprocess_market_data(
                    new_daily_files_dir=Path(new_daily),
                    gdelt_corpus_dir=Path(gdelt),
                    working_csv_files_dir=Path(working),
                    dry_run=dryrun_var.get(),
                    global_cutoff_date=cutoff_var.get().strip() or None,
                )
                man_path = Path(working) / f"{MANIFEST_PREFIX}_{_now_stamp()}.json"
                _atomic_write_json(man_path, manifest)
                counts = manifest["counts"]
                gcounts = manifest["gdelt_cache"]["counts"]
                append_log(
                    "Market | "
                    f"files_found={counts['files_found']} processed={counts['processed']} updated_tickers={counts['updated_tickers']} "
                    f"rows_appended={counts['total_rows_appended']} already_processed={counts['skipped_already_processed']} "
                    f"ignored_metadata={counts['ignored_metadata_files']} errors={counts['errors']}"
                )
                append_log(
                    "GDELT | "
                    f"candidates={gcounts['candidate_files']} cache_hits={gcounts['chunk_cache_hits']} parsed={gcounts['chunk_parsed']} "
                    f"duplicate_content={gcounts['duplicate_content_files']} event_days={gcounts['events_daily_rows']} "
                    f"gkg_days={gcounts['gkg_daily_rows']} join_ready={gcounts['join_ready_rows']} baselines={gcounts['baseline_rows']} "
                    f"errors={gcounts['errors']}"
                )
                append_log(f"Manifest: {man_path}")
                append_log(f"Market registry: {manifest['market_registry_path']}")
                append_log(f"GDELT registry: {manifest['gdelt_cache']['registry_path']}")
                append_log(f"GDELT join-ready: {manifest['gdelt_cache']['daily_join_ready_csv']}")
                status_var.set("Complete")
            except Exception as e:
                append_log(f"ERROR: {e!r}")
                status_var.set("Failed")
            finally:
                progress.stop()
                btn_run.configure(state="normal")

        threading.Thread(target=worker, daemon=True).start()

    frm = ttk.Frame(root, padding=12)
    frm.grid(row=0, column=0, sticky="nsew")
    frm.columnconfigure(1, weight=1)
    frm.rowconfigure(6, weight=1)

    ttk.Label(frm, text="New Daily Files:").grid(row=0, column=0, sticky="w")
    ttk.Entry(frm, textvariable=new_daily_var, width=90).grid(row=0, column=1, sticky="ew")
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(new_daily_var)).grid(row=0, column=2)

    ttk.Label(frm, text="GDELT Corpus:").grid(row=1, column=0, sticky="w")
    ttk.Entry(frm, textvariable=gdelt_var, width=90).grid(row=1, column=1, sticky="ew")
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(gdelt_var)).grid(row=1, column=2)

    ttk.Label(frm, text="Working .CSV Files:").grid(row=2, column=0, sticky="w")
    ttk.Entry(frm, textvariable=working_csv_var, width=90).grid(row=2, column=1, sticky="ew")
    ttk.Button(frm, text="Browse", command=lambda: browse_dir(working_csv_var)).grid(row=2, column=2)

    opts = ttk.Frame(frm)
    opts.grid(row=3, column=0, columnspan=3, sticky="w")
    ttk.Checkbutton(opts, text="Dry run", variable=dryrun_var).grid(row=0, column=0, padx=(0, 12))
    ttk.Label(opts, text="Cutoff YYYY-MM-DD:").grid(row=0, column=1)
    ttk.Entry(opts, textvariable=cutoff_var, width=12).grid(row=0, column=2)

    btn_run = ttk.Button(frm, text="Run preprocessing", command=run_clicked)
    btn_run.grid(row=4, column=0, sticky="w")
    progress = ttk.Progressbar(frm)
    progress.grid(row=4, column=1, columnspan=2, sticky="ew")
    ttk.Label(frm, textvariable=status_var).grid(row=5, column=0, columnspan=3, sticky="w")

    log = tk.Text(frm, height=16, width=110, state="disabled")
    log.grid(row=6, column=0, columnspan=3, sticky="nsew")
    root.mainloop()


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Preprocess Stooq daily files into working OHLCV CSVs and build neutral GDELT daily aggregate caches "
            "without performing ticker-level attribution or investment scoring."
        )
    )
    parser.add_argument("--gui", action="store_true")
    parser.add_argument("--new-daily-files", default=str(HARDCODED_PATHS["new_daily_files"]))
    parser.add_argument("--gdelt-corpus", default=str(HARDCODED_PATHS["gdelt_corpus"]))
    parser.add_argument("--working-csv-files", default=str(HARDCODED_PATHS["working_csv_files"]))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cutoff", default="")
    args = parser.parse_args()

    if args.gui:
        _run_gui()
        return 0

    new_daily = Path(args.new_daily_files)
    gdelt = Path(args.gdelt_corpus)
    working = Path(args.working_csv_files)

    manifest = preprocess_market_data(
        new_daily_files_dir=new_daily,
        gdelt_corpus_dir=gdelt,
        working_csv_files_dir=working,
        dry_run=bool(args.dry_run),
        global_cutoff_date=args.cutoff.strip() or None,
    )
    man_path = working / f"{MANIFEST_PREFIX}_{_now_stamp()}.json"
    _safe_mkdir(working)
    _atomic_write_json(man_path, manifest)

    counts = manifest["counts"]
    gcounts = manifest["gdelt_cache"]["counts"]
    print(
        f"[market] files_found={counts['files_found']} processed={counts['processed']} updated_tickers={counts['updated_tickers']} "
        f"rows_appended={counts['total_rows_appended']} already_processed={counts['skipped_already_processed']} "
        f"ignored_metadata={counts['ignored_metadata_files']} skipped_empty={counts['skipped_empty']} errors={counts['errors']}"
    )
    print(
        f"[gdelt] candidates={gcounts['candidate_files']} cache_hits={gcounts['chunk_cache_hits']} parsed={gcounts['chunk_parsed']} "
        f"duplicate_content={gcounts['duplicate_content_files']} event_days={gcounts['events_daily_rows']} "
        f"gkg_days={gcounts['gkg_daily_rows']} join_ready={gcounts['join_ready_rows']} baselines={gcounts['baseline_rows']} errors={gcounts['errors']}"
    )
    print(f"[done] manifest={man_path}")
    print(f"[done] market_registry={manifest['market_registry_path']}")
    print(f"[done] gdelt_registry={manifest['gdelt_cache']['registry_path']}")
    print(f"[done] gdelt_join_ready={manifest['gdelt_cache']['daily_join_ready_csv']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
