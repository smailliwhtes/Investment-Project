from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from market_monitor.hash_utils import hash_file

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


def discover_corpus_files(root_dir: Path | None) -> list[Path]:
    if not root_dir or not root_dir.exists():
        return []
    return sorted([p for p in root_dir.glob("*.csv") if p.is_file()])


def build_corpus_manifest(files: list[Path]) -> dict[str, Any]:
    payload = []
    for path in files:
        payload.append({"path": str(path), "sha256": hash_file(path)})
    return {"files": payload}


def build_corpus_index(files: list[Path], index_path: Path) -> dict[str, Any]:
    existing: dict[str, Any] = {}
    if index_path.exists():
        try:
            existing = json.loads(index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}
    existing_files = {entry["path"]: entry for entry in existing.get("files", [])}
    entries = []
    for path in files:
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
            }
        )
    payload = {"generated_at_utc": datetime.now(timezone.utc).isoformat(), "files": entries}
    index_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def _checksum_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample)
        return dialect.delimiter
    except csv.Error:
        return ","


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    sample = path.read_text(encoding="utf-8", errors="ignore")[:4096]
    delimiter = _detect_delimiter(sample)
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, sep=delimiter, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, sep=delimiter, encoding="latin-1")


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


def _parse_event_date(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.to_datetime(series, errors="coerce")
    series_str = series.astype(str).str.replace(r"\.0$", "", regex=True)
    if series_str.str.match(r"^\d{8}$").any():
        return pd.to_datetime(series_str, format="%Y%m%d", errors="coerce", utc=True)
    return pd.to_datetime(series_str, errors="coerce", utc=True)


def load_events(files: list[Path]) -> tuple[pd.DataFrame, list[CorpusFileInfo]]:
    frames = []
    infos: list[CorpusFileInfo] = []
    for path in files:
        raw = _read_csv_with_fallback(path)
        mapping = _map_columns(list(raw.columns))
        canonical = pd.DataFrame()
        for target, source in mapping.items():
            canonical[target] = raw[source]
        for field in CANONICAL_FIELDS:
            if field not in canonical:
                canonical[field] = np.nan
        canonical["EventDate"] = _parse_event_date(canonical["SQLDATE"])
        canonical = canonical.dropna(subset=["EventDate"]).copy()
        canonical["EventDate"] = canonical["EventDate"].dt.tz_convert("UTC")
        canonical["Date"] = canonical["EventDate"].dt.date.astype(str)
        now_date = datetime.now(timezone.utc).date().isoformat()
        canonical = canonical[canonical["Date"] <= now_date]
        canonical = canonical.drop_duplicates()
        frames.append(canonical)

        min_date = canonical["Date"].min() if not canonical.empty else None
        max_date = canonical["Date"].max() if not canonical.empty else None
        infos.append(
            CorpusFileInfo(
                path=path,
                checksum=hash_file(path),
                rows=len(canonical),
                min_date=min_date,
                max_date=max_date,
                columns=list(raw.columns),
            )
        )

    if not frames:
        return pd.DataFrame(), infos
    combined = pd.concat(frames, ignore_index=True)
    combined = _dedupe_events(combined)
    combined = combined.sort_values("EventDate")
    return combined.reset_index(drop=True), infos


def _dedupe_events(df: pd.DataFrame) -> pd.DataFrame:
    if "GlobalEventID" in df.columns and df["GlobalEventID"].notna().any():
        return df.drop_duplicates(subset=["GlobalEventID"], keep="first")
    keys = df[
        [
            "Date",
            "EventCode",
            "EventRootCode",
            "Actor1CountryCode",
            "Actor2CountryCode",
            "ActionGeo_CountryCode",
        ]
    ].fillna("")
    hashes = keys.apply(lambda row: _checksum_bytes("|".join(row.astype(str)).encode("utf-8")), axis=1)
    df = df.copy()
    df["dedupe_key"] = hashes
    return df.drop_duplicates(subset=["dedupe_key"], keep="first").drop(columns=["dedupe_key"])


def aggregate_daily_features(
    events: pd.DataFrame,
    *,
    rootcode_top_n: int,
    country_top_k: int,
) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    events = events.copy()
    events["QuadClass"] = pd.to_numeric(events["QuadClass"], errors="coerce")
    events["EventRootCode"] = events["EventRootCode"].astype(str).replace({"nan": np.nan})
    events["ActionGeo_CountryCode"] = events["ActionGeo_CountryCode"].astype(str).replace({"nan": np.nan})

    grouped = events.groupby("Date")
    daily = pd.DataFrame({"Date": sorted(events["Date"].unique())})
    daily["conflict_event_count_total"] = grouped.size().reindex(daily["Date"]).fillna(0).astype(int).values

    quad_counts = (
        events.groupby(["Date", "QuadClass"]).size().unstack(fill_value=0).sort_index(axis=1)
    )
    for quad in quad_counts.columns:
        daily[f"conflict_event_count_quadclass_{int(quad)}"] = (
            quad_counts[quad].reindex(daily["Date"]).fillna(0).astype(int).values
        )

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

    country_counts = events.groupby(["Date", "ActionGeo_CountryCode"]).size().unstack(fill_value=0)
    country_totals = country_counts.sum(axis=0).sort_values(ascending=False)
    top_countries = [code for code in country_totals.index if code not in {"nan", "None"}][:country_top_k]
    for code in top_countries:
        daily[f"conflict_event_count_country_{code}"] = (
            country_counts.get(code, 0).reindex(daily["Date"]).fillna(0).astype(int).values
        )
    if top_countries:
        other = country_counts.drop(columns=top_countries, errors="ignore").sum(axis=1)
        daily["conflict_event_count_country_other"] = (
            other.reindex(daily["Date"]).fillna(0).astype(int).values
        )

    for col, target in [
        ("GoldsteinScale", "goldstein"),
        ("AvgTone", "tone"),
    ]:
        series = pd.to_numeric(events[col], errors="coerce")
        daily[f"{target}_sum"] = grouped[series.name].sum().reindex(daily["Date"]).values
        daily[f"{target}_mean"] = grouped[series.name].mean().reindex(daily["Date"]).values

    for col, target in [
        ("NumMentions", "mentions"),
        ("NumSources", "sources"),
        ("NumArticles", "articles"),
    ]:
        series = pd.to_numeric(events[col], errors="coerce")
        daily[f"{target}_sum"] = grouped[series.name].sum().reindex(daily["Date"]).values

    daily = daily.fillna(0)
    return daily


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


def run_corpus_pipeline(
    *,
    corpus_dir: Path | None,
    outputs_dir: Path,
    config: dict[str, Any],
    provider,
    watchlist: list[str],
    logger,
) -> CorpusRun:
    files = discover_corpus_files(corpus_dir)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    corpus_dir_str = str(corpus_dir) if corpus_dir else "unset"
    if not files:
        manifest = {"files": [], "corpus_dir": corpus_dir_str}
        return CorpusRun(
            daily_features=None,
            context_columns=[],
            manifest=manifest,
            analogs_report=None,
            event_impact=None,
            analogs=[],
            analog_outcomes=[],
        )

    index_path = outputs_dir / "corpus_index.json"
    index_payload = build_corpus_index(files, index_path)

    events, infos = load_events(files)
    features_cfg = config.get("corpus", {}).get("features", {})
    daily_features = aggregate_daily_features(
        events,
        rootcode_top_n=int(features_cfg.get("rootcode_top_n", 8)),
        country_top_k=int(features_cfg.get("country_top_k", 8)),
    )
    daily_path = outputs_dir / "daily_features.csv"
    daily_features.to_csv(daily_path, index=False)

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
        if cached is not None:
            cached.update(
                {
                    "rows": entry["rows"],
                    "min_date": entry["min_date"],
                    "max_date": entry["max_date"],
                }
            )
    index_path.write_text(json.dumps(index_payload, indent=2, sort_keys=True), encoding="utf-8")

    (outputs_dir / "corpus_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    analog_cfg = config.get("corpus", {}).get("analogs", {})
    analogs_report, analogs = build_analogs_report(
        daily_features,
        top_n=int(analog_cfg.get("top_n", 8)),
    )
    analogs_path = outputs_dir / "analogs_report.md"
    analogs_path.write_text(analogs_report, encoding="utf-8")

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
