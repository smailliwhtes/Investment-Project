from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    import pandas as pd

from market_monitor.hash_utils import hash_text

EVENTS_HEADER_COLUMNS = [
    "GlobalEventID",
    "SQLDATE",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_Fullname",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_Fullname",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_Fullname",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL",
]

GKG_HEADER_COLUMNS = [
    "GKGRECORDID",
    "DATE",
    "SourceCollectionIdentifier",
    "SourceCommonName",
    "DocumentIdentifier",
    "Counts",
    "V2Counts",
    "Themes",
    "V2Themes",
    "Locations",
    "V2Locations",
    "Persons",
    "V2Persons",
    "Organizations",
    "V2Organizations",
    "V2Tone",
    "V2Dates",
    "GCAM",
    "SharingImage",
    "RelatedImages",
    "SocialImageEmbeds",
    "SocialVideoEmbeds",
    "Quotations",
    "AllNames",
    "Amounts",
    "TranslationInfo",
    "Extras",
]

EVENTS_CANONICAL_COLUMNS = [
    "day",
    "event_id",
    "event_code",
    "event_base_code",
    "event_root_code",
    "quad_class",
    "goldstein_scale",
    "avg_tone",
    "num_mentions",
    "num_sources",
    "num_articles",
    "actor1_country_code",
    "actor2_country_code",
    "actiongeo_country_code",
    "source_url",
]

GKG_CANONICAL_COLUMNS = [
    "datetime",
    "document_identifier",
    "themes",
    "persons",
    "organizations",
    "locations",
    "tone",
]

EVENTS_REQUIRED_FIELDS = [
    "day",
    "event_code",
    "event_base_code",
    "event_root_code",
    "goldstein_scale",
    "avg_tone",
]

GKG_REQUIRED_FIELDS = ["datetime", "document_identifier"]

_EVENTS_ALIASES = {
    "day": ["sqldate", "day", "date", "event_date", "eventdate"],
    "event_id": ["globaleventid", "global_event_id", "event_id"],
    "event_code": ["eventcode", "event_code"],
    "event_base_code": ["eventbasecode", "event_base_code"],
    "event_root_code": ["eventrootcode", "event_root_code"],
    "quad_class": ["quadclass", "quad_class"],
    "goldstein_scale": ["goldsteinscale", "goldstein_scale", "goldstein"],
    "avg_tone": ["avgtone", "avg_tone", "tone"],
    "num_mentions": ["nummentions", "num_mentions", "mentions"],
    "num_sources": ["numsources", "num_sources", "sources"],
    "num_articles": ["numarticles", "num_articles", "articles"],
    "actor1_country_code": ["actor1countrycode", "actor1_country_code"],
    "actor2_country_code": ["actor2countrycode", "actor2_country_code"],
    "actiongeo_country_code": ["actiongeocountrycode", "actiongeo_country_code"],
    "source_url": ["sourceurl", "source_url"],
}

_GKG_ALIASES = {
    "datetime": ["date", "datetime", "publication_date"],
    "document_identifier": ["documentidentifier", "document_identifier", "doc_id"],
    "themes": ["themes", "v2themes"],
    "persons": ["persons", "v2persons"],
    "organizations": ["organizations", "v2organizations"],
    "locations": ["locations", "v2locations"],
    "tone": ["v2tone", "tone"],
}


@dataclass(frozen=True)
class FileSpec:
    path: Path
    delimiter: str
    has_header: bool
    columns: list[str]
    column_count: int


def _normalize_column_name(name: str) -> str:
    cleaned = []
    for char in name.strip().lower():
        if char.isalnum():
            cleaned.append(char)
        else:
            cleaned.append("_")
    normalized = "".join(cleaned)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def normalize_columns(columns: Iterable[str]) -> list[str]:
    return [_normalize_column_name(col) for col in columns]


def map_columns(columns: Iterable[str], aliases: dict[str, list[str]]) -> dict[str, str]:
    columns_list = list(columns)
    normalized = normalize_columns(columns_list)
    mapping = {}
    for canonical, name_options in aliases.items():
        for option in name_options:
            if option in normalized:
                mapping[canonical] = columns_list[normalized.index(option)]
                break
    return mapping


def detect_delimiter(sample: str) -> str:
    comma_count = sample.count(",")
    tab_count = sample.count("\t")
    pipe_count = sample.count("|")
    if tab_count > max(comma_count, pipe_count):
        return "\t"
    if pipe_count > comma_count:
        return "|"
    return ","


def detect_header(sample: str) -> bool:
    sample_lines = sample.splitlines()
    first_line = sample_lines[0] if sample_lines else ""
    delimiter = detect_delimiter(first_line)
    parts = [part.strip() for part in first_line.split(delimiter)]
    known_headers = set(normalize_columns(EVENTS_HEADER_COLUMNS + GKG_HEADER_COLUMNS))
    if set(normalize_columns(parts)) & known_headers:
        return True
    if len(sample_lines) < 2:
        return False
    try:
        return csv.Sniffer().has_header(sample)
    except csv.Error:
        alpha = sum(any(char.isalpha() for char in part) for part in parts)
        numeric_like = sum(part.replace(".", "").isdigit() for part in parts)
        return alpha > numeric_like


def analyze_file(path: Path) -> FileSpec:
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
            sample = "".join([handle.readline() for _ in range(5)])
    except OSError as exc:
        raise OSError(
            f"Unable to open {path}. Ensure the file exists and is not locked by sync tools (e.g., OneDrive)."
        ) from exc
    first_line = sample.splitlines()[0] if sample else ""
    delimiter = detect_delimiter(first_line)
    has_header = detect_header(sample)
    if has_header and first_line:
        reader = csv.reader([first_line], delimiter=delimiter)
        columns = next(reader)
    else:
        columns = []
    column_count = len(columns)
    if not has_header and first_line:
        column_count = len(first_line.split(delimiter))
    return FileSpec(
        path=path,
        delimiter=delimiter,
        has_header=has_header,
        columns=columns,
        column_count=column_count,
    )


def detect_schema_type(columns: list[str], column_count: int) -> str:
    normalized = set(normalize_columns(columns))
    if normalized & {"eventcode", "eventrootcode", "eventbasecode", "globaleventid"}:
        return "events"
    if normalized & {"themes", "v2themes", "v2tone", "persons", "organizations", "locations"}:
        return "gkg"
    if not columns and column_count >= 50:
        return "events"
    return "unknown"


def estimate_rows(path: Path, *, delimiter: str) -> tuple[int, bool]:
    size = path.stat().st_size
    if size == 0:
        return 0, False
    sample_size = min(size, 1024 * 1024)
    with path.open("rb") as handle:
        sample = handle.read(sample_size)
    line_count = sample.count(b"\n")
    if line_count == 0:
        return 0, False
    avg_line = sample_size / line_count
    estimated = int(size / avg_line)
    estimated = max(estimated, line_count)
    is_estimated = size > 10 * 1024 * 1024
    return estimated, is_estimated


def parse_day(series: "pd.Series") -> "pd.Series":
    import pandas as pd
    if series.dtype.kind in {"i", "u", "f"}:
        as_str = series.dropna().astype(int).astype(str)
        parsed = pd.to_datetime(as_str, format="%Y%m%d", errors="coerce")
        return parsed
    series = series.astype(str)
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.notna().any():
        return parsed
    parsed = pd.to_datetime(series, format="%Y%m%d", errors="coerce")
    return parsed


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_content_hash(payload: dict) -> str:
    return hash_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True))


def build_file_fingerprint(paths: Iterable[Path]) -> list[dict[str, str | int | float]]:
    fingerprint = []
    for path in paths:
        stat = path.stat()
        fingerprint.append(
            {
                "path": str(path),
                "mtime": stat.st_mtime,
                "size": stat.st_size,
            }
        )
    return fingerprint


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_event_root_code(value: "pd.Series") -> "pd.Series":
    import pandas as pd
    cleaned = value.astype(str).str.strip()
    cleaned = cleaned.replace({"nan": None, "None": None})
    return cleaned.apply(lambda item: item.zfill(2) if isinstance(item, str) and item.isdigit() and len(item) == 1 else item)


def coerce_numeric(series: "pd.Series") -> "pd.Series":
    import pandas as pd

    return pd.to_numeric(series, errors="coerce")


def list_files(raw_dir: Path, pattern: str) -> list[Path]:
    files = sorted(raw_dir.glob(pattern))
    return [path for path in files if path.is_file()]


def format_missing_rate(total: int, missing: int) -> float:
    if total == 0:
        return 1.0
    return missing / total
