from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from market_monitor.hash_utils import hash_file, hash_manifest
from market_monitor.time_utils import utc_now_iso
from market_monitor.taxonomy import parse_taxonomy_fields

CANONICAL_FIELDS = {
    "GlobalEventID": ["globaleventid", "eventid", "event_id"],
    "SQLDATE": ["sqldate", "event_date", "date", "eventdate"],
    "EventCode": ["eventcode", "event_code"],
    "EventRootCode": ["eventrootcode", "event_root_code", "rootcode", "root_code"],
    "QuadClass": ["quadclass", "quad_class"],
    "GoldsteinScale": ["goldsteinscale", "goldstein", "goldstein_scale"],
    "AvgTone": ["avgtone", "tone", "avg_tone"],
    "NumMentions": ["nummentions", "mentions", "num_mentions"],
    "NumSources": ["numsources", "sources", "num_sources"],
    "NumArticles": ["numarticles", "articles", "num_articles"],
    "Actor1CountryCode": ["actor1countrycode", "actor1_country", "actor1_countrycode"],
    "Actor2CountryCode": ["actor2countrycode", "actor2_country", "actor2_countrycode"],
    "ActionGeo_CountryCode": [
        "actiongeo_countrycode",
        "actiongeo_country",
        "actiongeo_country_code",
        "actiongeo_countrycode_alpha",
        "actiongeo_countrycode_alpha3",
        "actiongeo_country_code_alpha",
    ],
}

CANONICAL_DTYPES: dict[str, str] = {
    "GlobalEventID": "string",
    "SQLDATE": "string",
    "EventCode": "string",
    "EventRootCode": "string",
    "QuadClass": "Int64",
    "GoldsteinScale": "float64",
    "AvgTone": "float64",
    "NumMentions": "float64",
    "NumSources": "float64",
    "NumArticles": "float64",
    "Actor1CountryCode": "string",
    "Actor2CountryCode": "string",
    "ActionGeo_CountryCode": "string",
}

DATE_CANDIDATES = ["SQLDATE", "EventDate", "Event_Date", "Date", "eventdate"]


@dataclass(frozen=True)
class CorpusFileInfo:
    path: Path
    checksum: str
    rows: int
    min_date: str | None
    max_date: str | None
    columns: list[str]


@dataclass(frozen=True)
class CorpusRun:
    daily_features: pd.DataFrame | None
    context_columns: list[str]
    manifest: dict[str, Any]
    analogs_report: str | None
    event_impact: pd.DataFrame | None
    analogs: list[dict[str, Any]] | None
    analog_outcomes: list[dict[str, Any]] | None


@dataclass(frozen=True)
class CorpusSource:
    path: Path
    source_type: str


@dataclass
class DedupeStats:
    total_rows: int = 0
    kept_rows: int = 0
    duplicate_rows: int = 0
    duplicate_reasons: Counter[str] = None

    def __post_init__(self) -> None:
        if self.duplicate_reasons is None:
            self.duplicate_reasons = Counter()


@dataclass
class CorpusValidationReport:
    sources: list[CorpusFileInfo]
    dedupe_rate: float
    duplicate_reasons: list[dict[str, Any]]
    min_date: str | None
    max_date: str | None
    feature_flags: dict[str, bool]


def discover_corpus_files(root_dir: Path | None) -> list[Path]:
    if not root_dir or not root_dir.exists():
        return []
    return sorted([p for p in root_dir.glob("*.csv") if p.is_file()])


def discover_raw_event_zips(raw_dir: Path | None) -> list[Path]:
    if not raw_dir or not raw_dir.exists():
        return []
    return sorted([p for p in raw_dir.glob("*.zip") if p.is_file()])


def discover_corpus_sources(
    corpus_dir: Path | None,
    raw_dir: Path | None,
) -> list[CorpusSource]:
    sources = [CorpusSource(path=path, source_type="csv") for path in discover_corpus_files(corpus_dir)]
    sources.extend(
        CorpusSource(path=path, source_type="zip") for path in discover_raw_event_zips(raw_dir)
    )
    return sorted(sources, key=lambda source: source.path.as_posix())


def build_corpus_manifest(
    files: list[Path],
    raw_zips: list[Path] | None = None,
    *,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = [{"path": str(path), "sha256": hash_file(path)} for path in files]
    raw_payload = []
    for path in raw_zips or []:
        raw_payload.append({"path": str(path), "sha256": hash_file(path)})
    manifest = {"files": payload, "raw_event_zips": raw_payload}
    if settings is not None:
        manifest["settings"] = settings
    return manifest


def build_corpus_index(sources: list[CorpusSource], index_path: Path) -> dict[str, Any]:
    existing: dict[str, Any] = {}
    if index_path.exists():
        try:
            existing = json.loads(index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}
    existing_files = {entry["path"]: entry for entry in existing.get("files", [])}
    entries = []
    for source in sources:
        path = source.path
        checksum = hash_file(path)
        cached = existing_files.get(str(path))
        if cached and cached.get("sha256") == checksum:
            entries.append(cached)
            continue
        entries.append(
            {
                "path": str(path),
                "sha256": checksum,
                "rows": None,
                "min_date": None,
                "max_date": None,
                "source_type": source.source_type,
            }
        )
    payload = {"generated_at_utc": utc_now_iso(), "files": entries}
    with index_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True))
    return payload


def _checksum_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample)
        return dialect.delimiter
    except csv.Error:
        return ","


def _detect_encoding(sample: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            sample.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _read_csv_chunks(path: Path, *, chunk_size: int) -> Iterable[pd.DataFrame]:
    with path.open("rb") as handle:
        sample_bytes = handle.read(8192)
    encoding = _detect_encoding(sample_bytes)
    delimiter = _detect_delimiter(sample_bytes.decode(encoding, errors="ignore"))
    return pd.read_csv(
        path,
        sep=delimiter,
        encoding=encoding,
        chunksize=chunk_size,
        low_memory=False,
    )



def _normalize_name(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum() or ch == "_").replace("_", "")


def _map_columns(columns: list[str]) -> dict[str, str]:
    normalized = {_normalize_name(col): col for col in columns}
    mapping: dict[str, str] = {}
    for canonical, candidates in CANONICAL_FIELDS.items():
        for candidate in candidates:
            norm = _normalize_name(candidate)
            if norm in normalized:
                mapping[canonical] = normalized[norm]
                break
        if canonical not in mapping:
            for norm, original in normalized.items():
                if canonical.lower() in norm:
                    mapping[canonical] = original
                    break
    return mapping


def _resolve_date_column(columns: list[str]) -> str | None:
    mapping = _map_columns(columns)
    if "SQLDATE" in mapping:
        return mapping["SQLDATE"]
    normalized = {_normalize_name(col): col for col in columns}
    for candidate in DATE_CANDIDATES:
        norm = _normalize_name(candidate)
        if norm in normalized:
            return normalized[norm]
    for original in columns:
        if "date" in original.lower():
            return original
    return None


def _parse_event_date(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.to_datetime(series, errors="coerce")
    series_str = series.astype(str).str.replace(r"\.0$", "", regex=True)
    if series_str.str.match(r"^\d{8}$").any():
        return pd.to_datetime(series_str, format="%Y%m%d", errors="coerce", utc=True)
    return pd.to_datetime(series_str, errors="coerce", utc=True)


class _DedupeTracker:
    def __init__(self) -> None:
        self.seen: set[str] = set()


def _normalize_taxonomy_fields(df: pd.DataFrame) -> pd.DataFrame:
    event_codes = df.get("EventCode")
    root_codes = df.get("EventRootCode")
    quad_class = df.get("QuadClass")
    normalized_event = []
    normalized_root = []
    normalized_quad = []
    for event, root, quad in zip(
        event_codes.to_numpy(dtype=object),
        root_codes.to_numpy(dtype=object),
        quad_class.to_numpy(dtype=object),
    ):
        parsed = parse_taxonomy_fields(event, root, quad)
        normalized_event.append(parsed.event_code)
        normalized_root.append(parsed.root_code)
        normalized_quad.append(parsed.quad_class)
    df = df.copy()
    df["EventCode"] = pd.Series(normalized_event, dtype="string")
    df["EventRootCode"] = pd.Series(normalized_root, dtype="string")
    df["QuadClass"] = pd.Series(normalized_quad, dtype="Int64")
    return df


def _canonicalize_chunk(raw: pd.DataFrame, *, date_col: str | None) -> pd.DataFrame:
    mapping = _map_columns(list(raw.columns))
    canonical = pd.DataFrame()
    for target, source in mapping.items():
        canonical[target] = raw[source]
    if "SQLDATE" not in canonical and date_col:
        canonical["SQLDATE"] = raw[date_col]
    for field in CANONICAL_FIELDS:
        if field not in canonical:
            canonical[field] = np.nan
    canonical = canonical.astype({k: v for k, v in CANONICAL_DTYPES.items() if k in canonical}, errors="ignore")
    canonical["EventDate"] = _parse_event_date(canonical["SQLDATE"])
    canonical = canonical.dropna(subset=["EventDate"]).copy()
    canonical["EventDate"] = canonical["EventDate"].dt.tz_convert("UTC")
    canonical["Date"] = canonical["EventDate"].dt.date.astype(str)
    now_date = utc_now_iso().split("T", maxsplit=1)[0]
    canonical = canonical[canonical["Date"] <= now_date]
    canonical = _normalize_taxonomy_fields(canonical)
    return canonical


def _dedupe_chunk(
    df: pd.DataFrame,
    tracker: _DedupeTracker,
    stats: DedupeStats,
) -> pd.DataFrame:
    if df.empty:
        return df
    use_global = "GlobalEventID" in df.columns and df["GlobalEventID"].notna().any()
    if use_global:
        keys = df["GlobalEventID"].astype(str).fillna("")
        reason = "duplicate_global_event_id"
    else:
        fields = [
            "Date",
            "EventCode",
            "EventRootCode",
            "Actor1CountryCode",
            "Actor2CountryCode",
            "ActionGeo_CountryCode",
            "NumMentions",
            "NumSources",
            "NumArticles",
        ]
        available = [col for col in fields if col in df.columns]
        keys = df[available].fillna("").astype(str).agg("|".join, axis=1)
        keys = keys.apply(lambda value: _checksum_bytes(value.encode("utf-8")))
        reason = "duplicate_hash"

    keep_mask = []
    for key in keys:
        stats.total_rows += 1
        if key in tracker.seen:
            stats.duplicate_rows += 1
            stats.duplicate_reasons[reason] += 1
            keep_mask.append(False)
        else:
            tracker.seen.add(key)
            stats.kept_rows += 1
            keep_mask.append(True)
    return df.loc[keep_mask].copy()


def _iter_csv_chunks(path: Path, *, chunk_size: int) -> Iterable[pd.DataFrame]:
    for chunk in _read_csv_chunks(path, chunk_size=chunk_size):
        yield chunk


def _iter_zip_chunks(path: Path, *, chunk_size: int) -> Iterable[pd.DataFrame]:
    with zipfile.ZipFile(path) as archive:
        members = [m for m in archive.namelist() if not m.endswith("/")]
        if not members:
            return
        member = next((m for m in members if m.lower().endswith(".csv")), members[0])
        with archive.open(member) as handle:
            sample_bytes = handle.read(8192)
            encoding = _detect_encoding(sample_bytes)
            delimiter = _detect_delimiter(sample_bytes.decode(encoding, errors="ignore"))
        with archive.open(member) as handle:
            text_handle = io.TextIOWrapper(handle, encoding=encoding)
            for chunk in pd.read_csv(
                text_handle,
                sep=delimiter,
                chunksize=chunk_size,
                low_memory=False,
            ):
                yield chunk


def load_events(
    files: list[Path],
    raw_zips: list[Path] | None = None,
    *,
    chunk_size: int = 100_000,
) -> tuple[pd.DataFrame, list[CorpusFileInfo]]:
    frames = []
    infos: list[CorpusFileInfo] = []
    tracker = _DedupeTracker()
    stats = DedupeStats()
    sources = [CorpusSource(path=path, source_type="csv") for path in files]
    for path in raw_zips or []:
        sources.append(CorpusSource(path=path, source_type="zip"))

    for source in sources:
        rows = 0
        min_date = None
        max_date = None
        columns = []
        chunk_iter = (
            _iter_zip_chunks(source.path, chunk_size=chunk_size)
            if source.source_type == "zip"
            else _iter_csv_chunks(source.path, chunk_size=chunk_size)
        )
        date_col = None
        for raw in chunk_iter:
            if not columns:
                columns = list(raw.columns)
                date_col = _resolve_date_column(columns)
            canonical = _canonicalize_chunk(raw, date_col=date_col)
            canonical = _dedupe_chunk(canonical, tracker, stats)
            if canonical.empty:
                continue
            rows += len(canonical)
            min_date = min(min_date, canonical["Date"].min()) if min_date else canonical["Date"].min()
            max_date = max(max_date, canonical["Date"].max()) if max_date else canonical["Date"].max()
            frames.append(canonical)

        infos.append(
            CorpusFileInfo(
                path=source.path,
                checksum=hash_file(source.path),
                rows=rows,
                min_date=min_date,
                max_date=max_date,
                columns=columns,
            )
        )

    if not frames:
        return pd.DataFrame(), infos
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("EventDate")
    return combined.reset_index(drop=True), infos


def aggregate_daily_features(
    events: pd.DataFrame,
    *,
    rootcode_top_n: int,
    country_top_k: int,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    events = events.copy()
    available_quad = "QuadClass" in events and events["QuadClass"].notna().any()
    available_root = "EventRootCode" in events and events["EventRootCode"].notna().any()
    available_country = (
        "ActionGeo_CountryCode" in events and events["ActionGeo_CountryCode"].notna().any()
    )
    available_goldstein = "GoldsteinScale" in events and events["GoldsteinScale"].notna().any()
    available_tone = "AvgTone" in events and events["AvgTone"].notna().any()
    available_mentions = "NumMentions" in events and events["NumMentions"].notna().any()
    available_sources = "NumSources" in events and events["NumSources"].notna().any()
    available_articles = "NumArticles" in events and events["NumArticles"].notna().any()

    if "QuadClass" in events:
        events["QuadClass"] = pd.to_numeric(events["QuadClass"], errors="coerce")
    if "EventRootCode" in events:
        events["EventRootCode"] = events["EventRootCode"].astype(str).replace({"nan": np.nan})
    if "ActionGeo_CountryCode" in events:
        events["ActionGeo_CountryCode"] = events["ActionGeo_CountryCode"].astype(str).replace(
            {"nan": np.nan}
        )

    grouped = events.groupby("Date")
    daily = pd.DataFrame({"Date": sorted(events["Date"].unique())})
    daily["conflict_event_count_total"] = grouped.size().reindex(daily["Date"]).fillna(0).astype(int).values

    if available_quad:
        quad_counts = (
            events.groupby(["Date", "QuadClass"]).size().unstack(fill_value=0).sort_index(axis=1)
        )
        for quad in quad_counts.columns:
            daily[f"conflict_event_count_quadclass_{int(quad)}"] = (
                quad_counts[quad].reindex(daily["Date"]).fillna(0).astype(int).values
            )

    if available_root:
        root_counts = events.groupby(["Date", "EventRootCode"]).size().unstack(fill_value=0)
        root_totals = root_counts.sum(axis=0).sort_values(ascending=False)
        top_roots = [code for code in root_totals.index if code not in {"nan", "None"}][:rootcode_top_n]
        for code in top_roots:
            daily[f"conflict_event_count_rootcode_{code}"] = (
                root_counts.get(code, 0).reindex(daily["Date"]).fillna(0).astype(int).values
            )
        if top_roots:
            other = root_counts.drop(columns=top_roots, errors="ignore").sum(axis=1)
            daily["conflict_event_count_rootcode_other"] = (
                other.reindex(daily["Date"]).fillna(0).astype(int).values
            )

    if available_country:
        country_counts = events.groupby(["Date", "ActionGeo_CountryCode"]).size().unstack(fill_value=0)
        country_totals = country_counts.sum(axis=0).sort_values(ascending=False)
        top_countries = [
            code for code in country_totals.index if code not in {"nan", "None"}
        ][:country_top_k]
        for code in top_countries:
            daily[f"conflict_event_count_country_{code}"] = (
                country_counts.get(code, 0).reindex(daily["Date"]).fillna(0).astype(int).values
            )
        if top_countries:
            other = country_counts.drop(columns=top_countries, errors="ignore").sum(axis=1)
            daily["conflict_event_count_country_other"] = (
                other.reindex(daily["Date"]).fillna(0).astype(int).values
            )

    for col, target, available in [
        ("GoldsteinScale", "goldstein", available_goldstein),
        ("AvgTone", "tone", available_tone),
    ]:
        if available:
            series = pd.to_numeric(events[col], errors="coerce")
            daily[f"{target}_sum"] = grouped[series.name].sum().reindex(daily["Date"]).values
            daily[f"{target}_mean"] = grouped[series.name].mean().reindex(daily["Date"]).values

    for col, target, available in [
        ("NumMentions", "mentions", available_mentions),
        ("NumSources", "sources", available_sources),
        ("NumArticles", "articles", available_articles),
    ]:
        if available:
            series = pd.to_numeric(events[col], errors="coerce")
            daily[f"{target}_sum"] = grouped[series.name].sum().reindex(daily["Date"]).values

    daily = daily.fillna(0)
    return daily


class DailyFeatureAccumulator:
    def __init__(self) -> None:
        self.total_counts: Counter[str] = Counter()
        self.quad_counts: dict[str, Counter[int]] = defaultdict(Counter)
        self.root_counts: dict[str, Counter[str]] = defaultdict(Counter)
        self.country_counts: dict[str, Counter[str]] = defaultdict(Counter)
        self.goldstein_sum: Counter[str] = Counter()
        self.goldstein_count: Counter[str] = Counter()
        self.tone_sum: Counter[str] = Counter()
        self.tone_count: Counter[str] = Counter()
        self.mentions_sum: Counter[str] = Counter()
        self.sources_sum: Counter[str] = Counter()
        self.articles_sum: Counter[str] = Counter()
        self.root_totals: Counter[str] = Counter()
        self.country_totals: Counter[str] = Counter()
        self.flags: dict[str, bool] = {
            "quadclass": False,
            "rootcode": False,
            "country": False,
            "goldstein": False,
            "tone": False,
            "mentions": False,
            "sources": False,
            "articles": False,
        }

    def add_chunk(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        date_counts = df.groupby("Date").size()
        for date, count in date_counts.items():
            self.total_counts[date] += int(count)

        if "QuadClass" in df and df["QuadClass"].notna().any():
            self.flags["quadclass"] = True
            quad_counts = df.dropna(subset=["QuadClass"]).groupby(["Date", "QuadClass"]).size()
            for (date, quad), count in quad_counts.items():
                self.quad_counts[date][int(quad)] += int(count)

        if "EventRootCode" in df and df["EventRootCode"].notna().any():
            self.flags["rootcode"] = True
            root_counts = df.dropna(subset=["EventRootCode"]).groupby(["Date", "EventRootCode"]).size()
            for (date, code), count in root_counts.items():
                code_str = str(code)
                self.root_counts[date][code_str] += int(count)
                self.root_totals[code_str] += int(count)

        if "ActionGeo_CountryCode" in df and df["ActionGeo_CountryCode"].notna().any():
            self.flags["country"] = True
            country_counts = (
                df.dropna(subset=["ActionGeo_CountryCode"])
                .groupby(["Date", "ActionGeo_CountryCode"])
                .size()
            )
            for (date, code), count in country_counts.items():
                code_str = str(code)
                self.country_counts[date][code_str] += int(count)
                self.country_totals[code_str] += int(count)

        if "GoldsteinScale" in df and df["GoldsteinScale"].notna().any():
            self.flags["goldstein"] = True
            values = pd.to_numeric(df["GoldsteinScale"], errors="coerce")
            grouped = df.assign(GoldsteinScale=values).groupby("Date")["GoldsteinScale"]
            for date, value in grouped.sum().items():
                self.goldstein_sum[date] += float(value)
            for date, count in grouped.count().items():
                self.goldstein_count[date] += int(count)

        if "AvgTone" in df and df["AvgTone"].notna().any():
            self.flags["tone"] = True
            values = pd.to_numeric(df["AvgTone"], errors="coerce")
            grouped = df.assign(AvgTone=values).groupby("Date")["AvgTone"]
            for date, value in grouped.sum().items():
                self.tone_sum[date] += float(value)
            for date, count in grouped.count().items():
                self.tone_count[date] += int(count)

        if "NumMentions" in df and df["NumMentions"].notna().any():
            self.flags["mentions"] = True
            values = pd.to_numeric(df["NumMentions"], errors="coerce")
            grouped = df.assign(NumMentions=values).groupby("Date")["NumMentions"]
            for date, value in grouped.sum().items():
                self.mentions_sum[date] += float(value)

        if "NumSources" in df and df["NumSources"].notna().any():
            self.flags["sources"] = True
            values = pd.to_numeric(df["NumSources"], errors="coerce")
            grouped = df.assign(NumSources=values).groupby("Date")["NumSources"]
            for date, value in grouped.sum().items():
                self.sources_sum[date] += float(value)

        if "NumArticles" in df and df["NumArticles"].notna().any():
            self.flags["articles"] = True
            values = pd.to_numeric(df["NumArticles"], errors="coerce")
            grouped = df.assign(NumArticles=values).groupby("Date")["NumArticles"]
            for date, value in grouped.sum().items():
                self.articles_sum[date] += float(value)

    def build(self, *, rootcode_top_n: int, country_top_k: int) -> pd.DataFrame:
        dates = sorted(self.total_counts.keys())
        daily = pd.DataFrame({"Date": dates})
        daily["conflict_event_count_total"] = [self.total_counts[d] for d in dates]

        if self.flags["quadclass"]:
            quads = sorted({quad for counter in self.quad_counts.values() for quad in counter.keys()})
            for quad in quads:
                daily[f"conflict_event_count_quadclass_{quad}"] = [
                    self.quad_counts[date].get(quad, 0) for date in dates
                ]

        if self.flags["rootcode"]:
            top_roots = [
                code for code, _ in self.root_totals.most_common(rootcode_top_n)
                if code not in {"nan", "None"}
            ]
            for code in top_roots:
                daily[f"conflict_event_count_rootcode_{code}"] = [
                    self.root_counts[date].get(code, 0) for date in dates
                ]
            if top_roots:
                daily["conflict_event_count_rootcode_other"] = [
                    sum(
                        count
                        for code, count in self.root_counts[date].items()
                        if code not in top_roots
                    )
                    for date in dates
                ]

        if self.flags["country"]:
            top_countries = [
                code for code, _ in self.country_totals.most_common(country_top_k)
                if code not in {"nan", "None"}
            ]
            for code in top_countries:
                daily[f"conflict_event_count_country_{code}"] = [
                    self.country_counts[date].get(code, 0) for date in dates
                ]
            if top_countries:
                daily["conflict_event_count_country_other"] = [
                    sum(
                        count
                        for code, count in self.country_counts[date].items()
                        if code not in top_countries
                    )
                    for date in dates
                ]

        if self.flags["goldstein"]:
            daily["goldstein_sum"] = [self.goldstein_sum.get(date, 0.0) for date in dates]
            daily["goldstein_mean"] = [
                self.goldstein_sum.get(date, 0.0) / self.goldstein_count.get(date, 1)
                if self.goldstein_count.get(date, 0) > 0
                else 0.0
                for date in dates
            ]

        if self.flags["tone"]:
            daily["tone_sum"] = [self.tone_sum.get(date, 0.0) for date in dates]
            daily["tone_mean"] = [
                self.tone_sum.get(date, 0.0) / self.tone_count.get(date, 1)
                if self.tone_count.get(date, 0) > 0
                else 0.0
                for date in dates
            ]

        if self.flags["mentions"]:
            daily["mentions_sum"] = [self.mentions_sum.get(date, 0.0) for date in dates]

        if self.flags["sources"]:
            daily["sources_sum"] = [self.sources_sum.get(date, 0.0) for date in dates]

        if self.flags["articles"]:
            daily["articles_sum"] = [self.articles_sum.get(date, 0.0) for date in dates]

        daily = daily.fillna(0)
        return daily


def _read_md5_manifest(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    mapping: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        parts = line.strip().split()
        if len(parts) >= 2:
            checksum = parts[0]
            filename = parts[-1].lstrip("*")
            mapping[filename] = checksum
    return mapping


def _hash_md5_file(path: Path) -> str:
    hasher = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def verify_md5_for_zip(path: Path) -> tuple[bool, str | None]:
    sibling_md5 = path.with_suffix(path.suffix + ".md5")
    manifest = {}
    if sibling_md5.exists():
        manifest = _read_md5_manifest(sibling_md5)
    else:
        manifest_file = path.parent / "md5sums.txt"
        if manifest_file.exists():
            manifest = _read_md5_manifest(manifest_file)
    expected = manifest.get(path.name)
    if not expected:
        return True, None
    actual = _hash_md5_file(path)
    if actual.lower() != expected.lower():
        return False, f"md5 mismatch for {path.name}"
    return True, None


def build_daily_features_from_sources(
    sources: list[CorpusSource],
    *,
    rootcode_top_n: int,
    country_top_k: int,
    chunk_size: int = 100_000,
) -> tuple[pd.DataFrame, list[CorpusFileInfo], DedupeStats, dict[str, bool]]:
    tracker = _DedupeTracker()
    stats = DedupeStats()
    accumulator = DailyFeatureAccumulator()
    infos: list[CorpusFileInfo] = []

    for source in sources:
        rows = 0
        min_date = None
        max_date = None
        columns: list[str] = []
        chunk_iter = (
            _iter_zip_chunks(source.path, chunk_size=chunk_size)
            if source.source_type == "zip"
            else _iter_csv_chunks(source.path, chunk_size=chunk_size)
        )
        date_col = None
        for raw in chunk_iter:
            if not columns:
                columns = list(raw.columns)
                date_col = _resolve_date_column(columns)
            canonical = _canonicalize_chunk(raw, date_col=date_col)
            canonical = _dedupe_chunk(canonical, tracker, stats)
            if canonical.empty:
                continue
            rows += len(canonical)
            min_date = min(min_date, canonical["Date"].min()) if min_date else canonical["Date"].min()
            max_date = max(max_date, canonical["Date"].max()) if max_date else canonical["Date"].max()
            accumulator.add_chunk(canonical)

        infos.append(
            CorpusFileInfo(
                path=source.path,
                checksum=hash_file(source.path),
                rows=rows,
                min_date=min_date,
                max_date=max_date,
                columns=columns,
            )
        )

    daily_features = accumulator.build(rootcode_top_n=rootcode_top_n, country_top_k=country_top_k)
    return daily_features, infos, stats, accumulator.flags


def validate_corpus_sources(
    sources: list[CorpusSource],
    *,
    rootcode_top_n: int,
    country_top_k: int,
    chunk_size: int = 100_000,
) -> CorpusValidationReport:
    daily_features, infos, stats, flags = build_daily_features_from_sources(
        sources,
        rootcode_top_n=rootcode_top_n,
        country_top_k=country_top_k,
        chunk_size=chunk_size,
    )
    min_date = daily_features["Date"].min() if not daily_features.empty else None
    max_date = daily_features["Date"].max() if not daily_features.empty else None
    dedupe_rate = stats.duplicate_rows / stats.total_rows if stats.total_rows else 0.0
    reasons = [
        {"reason": reason, "count": count}
        for reason, count in stats.duplicate_reasons.most_common(5)
    ]
    return CorpusValidationReport(
        sources=infos,
        dedupe_rate=dedupe_rate,
        duplicate_reasons=reasons,
        min_date=min_date,
        max_date=max_date,
        feature_flags=flags,
    )


def _context_columns(daily_features: pd.DataFrame) -> list[str]:
    return [col for col in daily_features.columns if col != "Date"]


def _cosine_similarity(matrix: np.ndarray, vector: np.ndarray) -> np.ndarray:
    denom = np.linalg.norm(matrix, axis=1) * np.linalg.norm(vector)
    denom = np.where(denom == 0, 1.0, denom)
    return (matrix @ vector) / denom


def _compute_analogs(daily_features: pd.DataFrame, *, top_n: int) -> list[dict[str, Any]]:
    if daily_features.empty:
        return []
    features = daily_features.copy()
    feature_cols = _context_columns(features)
    matrix = features[feature_cols].to_numpy(dtype=float)
    mean = matrix.mean(axis=0)
    std = np.where(matrix.std(axis=0) == 0, 1.0, matrix.std(axis=0))
    standardized = (matrix - mean) / std
    target_vector = standardized[-1]
    historical = standardized[:-1]
    dates = features["Date"].tolist()
    if historical.size == 0:
        return []
    similarity = _cosine_similarity(historical, target_vector)
    ranked = np.argsort(similarity)[::-1][:top_n]
    return [
        {"rank": idx + 1, "date": dates[rank], "similarity": float(similarity[rank])}
        for idx, rank in enumerate(ranked)
    ]


def build_analogs_report(daily_features: pd.DataFrame, *, top_n: int) -> tuple[str, list[dict[str, Any]]]:
    analogs = _compute_analogs(daily_features, top_n=top_n)
    if daily_features.empty:
        return "No corpus features available.", analogs
    if not analogs:
        return "Insufficient history for analog search.", analogs
    target_date = daily_features["Date"].iloc[-1]
    lines = [
        "# Context Analogs",
        "",
        f"Target date: {target_date}",
        "",
        "| Rank | Date | Similarity |",
        "| --- | --- | --- |",
    ]
    for entry in analogs:
        lines.append(f"| {entry['rank']} | {entry['date']} | {entry['similarity']:.4f} |")
    return "\n".join(lines), analogs


def _load_symbol_returns(provider, symbol: str) -> pd.DataFrame | None:
    if not hasattr(provider, "load_symbol_data"):
        return None
    try:
        df, _ = provider.load_symbol_data(symbol)
    except Exception:
        return None
    if df.empty or "Date" not in df.columns:
        return None
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df = df.dropna(subset=["Date"]).sort_values("Date")
    return df


def _compute_forward_return(close: pd.Series, start_date: datetime.date, horizon: int) -> float | None:
    dates = close.index.to_list()
    if start_date not in close.index:
        future_dates = [d for d in dates if d >= start_date]
        if not future_dates:
            return None
        start_date = future_dates[0]
    start_idx = dates.index(start_date)
    end_idx = start_idx + int(horizon)
    if end_idx >= len(dates):
        return None
    start_price = close.iloc[start_idx]
    end_price = close.iloc[end_idx]
    if start_price and not np.isnan(start_price) and not np.isnan(end_price):
        return float(end_price / start_price - 1.0)
    return None


def build_analog_outcomes(
    analogs: list[dict[str, Any]],
    *,
    provider,
    symbols: list[str],
    forward_days: list[int],
) -> list[dict[str, Any]]:
    if not analogs:
        return []
    outcomes: list[dict[str, Any]] = []
    for symbol in symbols:
        df = _load_symbol_returns(provider, symbol)
        if df is None:
            continue
        close = df.set_index("Date")["Close"]
        for entry in analogs:
            event_date = datetime.strptime(entry["date"], "%Y-%m-%d").date()
            for horizon in forward_days:
                forward_return = _compute_forward_return(close, event_date, horizon)
                if forward_return is None:
                    continue
                outcomes.append(
                    {
                        "analog_date": entry["date"],
                        "symbol": symbol,
                        "forward_days": horizon,
                        "forward_return": forward_return,
                        "similarity": entry["similarity"],
                    }
                )
    return outcomes


def build_event_impact_library(
    daily_features: pd.DataFrame,
    *,
    provider,
    watchlist: list[str],
    spike_stddev: float,
    forward_days: list[int],
) -> pd.DataFrame:
    if daily_features.empty:
        return pd.DataFrame()
    series = daily_features["conflict_event_count_total"].to_numpy(dtype=float)
    threshold = float(np.nanmean(series) + spike_stddev * np.nanstd(series))
    spike_days = daily_features[daily_features["conflict_event_count_total"] >= threshold]["Date"].tolist()
    if not spike_days:
        return pd.DataFrame()

    symbols = ["SPY", "QQQ", "IWM"] + watchlist
    symbols = [s for s in dict.fromkeys(symbols) if s]
    rows = []
    for symbol in symbols:
        df = _load_symbol_returns(provider, symbol)
        if df is None:
            continue
        close = df.set_index("Date")["Close"]
        dates = close.index.to_list()
        for day in spike_days:
            event_date = datetime.strptime(day, "%Y-%m-%d").date()
            if event_date not in close.index:
                future_dates = [d for d in dates if d >= event_date]
                if not future_dates:
                    continue
                event_date = future_dates[0]
            start_idx = dates.index(event_date)
            for horizon in forward_days:
                end_idx = start_idx + int(horizon)
                if end_idx >= len(dates):
                    continue
                start_price = close.iloc[start_idx]
                end_price = close.iloc[end_idx]
                if start_price and not np.isnan(start_price) and not np.isnan(end_price):
                    rows.append(
                        {
                            "event_date": event_date.isoformat(),
                            "symbol": symbol,
                            "forward_days": horizon,
                            "forward_return": float(end_price / start_price - 1.0),
                            "conflict_event_count_total": float(
                                daily_features.loc[daily_features["Date"] == day][
                                    "conflict_event_count_total"
                                ].iloc[0]
                            ),
                        }
                    )
    return pd.DataFrame(rows)


def _load_cache_key(cache_path: Path) -> str | None:
    if not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload.get("cache_key")


def _write_cache_key(cache_path: Path, cache_key: str) -> None:
    payload = {"cache_key": cache_key, "generated_at_utc": utc_now_iso()}
    with cache_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True))


def build_corpus_daily_store(
    sources: list[CorpusSource],
    *,
    outputs_dir: Path,
    settings: dict[str, Any],
    logger,
    chunk_size: int = 100_000,
) -> tuple[pd.DataFrame, list[CorpusFileInfo], dict[str, Any], str, bool]:
    manifest_payload = build_corpus_manifest(
        [s.path for s in sources if s.source_type == "csv"],
        [s.path for s in sources if s.source_type == "zip"],
        settings=settings,
    )
    cache_key = hash_manifest(manifest_payload)
    cache_path = outputs_dir / "corpus_cache.json"
    daily_path = outputs_dir / "daily_features.csv"
    parquet_path = outputs_dir / "daily_features.parquet"
    cache_hit = _load_cache_key(cache_path) == cache_key and daily_path.exists()

    if cache_hit:
        daily_features = pd.read_csv(daily_path)
        infos = [
            CorpusFileInfo(
                path=Path(entry["path"]),
                checksum=entry["sha256"],
                rows=int(entry.get("rows") or 0),
                min_date=entry.get("min_date"),
                max_date=entry.get("max_date"),
                columns=[],
            )
            for entry in manifest_payload.get("files", []) + manifest_payload.get("raw_event_zips", [])
        ]
        logger.info("[corpus] cache hit; using existing daily features.")
        return daily_features, infos, manifest_payload, cache_key, True

    daily_features, infos, _, _ = build_daily_features_from_sources(
        sources,
        rootcode_top_n=int(settings.get("rootcode_top_n", 8)),
        country_top_k=int(settings.get("country_top_k", 8)),
        chunk_size=chunk_size,
    )
    daily_features.to_csv(daily_path, index=False)
    try:
        daily_features.to_parquet(parquet_path, index=False)
    except ImportError:
        logger.warning("[corpus] pyarrow missing; skipping daily_features.parquet output.")
    _write_cache_key(cache_path, cache_key)
    return daily_features, infos, manifest_payload, cache_key, False


def run_corpus_pipeline(
    *,
    corpus_dir: Path | None,
    raw_events_dir: Path | None,
    outputs_dir: Path,
    config: dict[str, Any],
    provider,
    watchlist: list[str],
    logger,
) -> CorpusRun:
    sources = discover_corpus_sources(corpus_dir, raw_events_dir)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    corpus_dir_str = str(corpus_dir) if corpus_dir else "unset"
    if not sources:
        manifest = {"files": [], "raw_event_zips": [], "corpus_dir": corpus_dir_str}
        return CorpusRun(
            daily_features=None,
            context_columns=[],
            manifest=manifest,
            analogs_report=None,
            event_impact=None,
            analogs=[],
            analog_outcomes=[],
        )

    features_cfg = config.get("corpus", {}).get("features", {})
    settings = {
        "rootcode_top_n": int(features_cfg.get("rootcode_top_n", 8)),
        "country_top_k": int(features_cfg.get("country_top_k", 8)),
    }
    daily_features, infos, manifest_payload, _, cache_hit = build_corpus_daily_store(
        sources,
        outputs_dir=outputs_dir,
        settings=settings,
        logger=logger,
    )

    index_path = outputs_dir / "corpus_index.json"
    index_payload = build_corpus_index(sources, index_path)

    manifest = {
        "corpus_dir": corpus_dir_str,
        "files": [
            {
                "path": str(info.path),
                "sha256": info.checksum,
                "rows": info.rows,
                "min_date": info.min_date,
                "max_date": info.max_date,
            }
            for info in infos
        ],
    }
    for entry in manifest["files"]:
        cached = next((item for item in index_payload["files"] if item["path"] == entry["path"]), None)
        if cached is not None and (entry["rows"] or entry["min_date"] or entry["max_date"] or not cache_hit):
            cached.update(
                {
                    "rows": entry["rows"],
                    "min_date": entry["min_date"],
                    "max_date": entry["max_date"],
                }
            )
    with index_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(index_payload, indent=2, sort_keys=True))

    manifest["raw_event_zips"] = manifest_payload.get("raw_event_zips", [])
    if "settings" in manifest_payload:
        manifest["settings"] = manifest_payload["settings"]
    manifest_path = outputs_dir / "corpus_manifest.json"
    with manifest_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(manifest, indent=2, sort_keys=True))

    analog_cfg = config.get("corpus", {}).get("analogs", {})
    analogs_report, analogs = build_analogs_report(
        daily_features,
        top_n=int(analog_cfg.get("top_n", 8)),
    )
    analogs_path = outputs_dir / "analogs_report.md"
    with analogs_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(analogs_report)

    forward_days = [int(d) for d in analog_cfg.get("forward_days", [1, 5, 20])]
    event_impact = build_event_impact_library(
        daily_features,
        provider=provider,
        watchlist=watchlist,
        spike_stddev=float(analog_cfg.get("spike_stddev", 2.0)),
        forward_days=forward_days,
    )
    if not event_impact.empty:
        event_impact_path = outputs_dir / "event_impact_library.csv"
        event_impact.to_csv(event_impact_path, index=False)

    analog_outcomes = build_analog_outcomes(
        analogs,
        provider=provider,
        symbols=["SPY", "QQQ", "IWM"],
        forward_days=forward_days,
    )

    return CorpusRun(
        daily_features=daily_features,
        context_columns=_context_columns(daily_features),
        manifest=manifest,
        analogs_report=analogs_report,
        event_impact=event_impact if not event_impact.empty else None,
        analogs=analogs,
        analog_outcomes=analog_outcomes,
    )
