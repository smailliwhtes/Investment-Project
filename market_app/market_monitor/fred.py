from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd


class FredError(RuntimeError):
    pass


def _resolve_date_column(columns: list[str]) -> str | None:
    candidates = {column.lower(): column for column in columns}
    for key in ("date", "observation_date", "timestamp"):
        if key in candidates:
            return candidates[key]
    return None


def _resolve_value_column(columns: list[str]) -> str | None:
    candidates = {column.lower(): column for column in columns}
    for key in ("value", "close", "observation_value"):
        if key in candidates:
            return candidates[key]
    for column in columns:
        if column.lower() != "date":
            return column
    return None


def load_fred_cache(cache_dir: Path) -> pd.DataFrame:
    if not cache_dir.exists():
        raise FredError(f"FRED cache directory not found: {cache_dir}")

    frames: list[pd.DataFrame] = []
    for path in sorted(cache_dir.glob("*.csv")):
        frame = pd.read_csv(path)
        if frame.empty:
            continue
        date_col = _resolve_date_column(list(frame.columns))
        value_col = _resolve_value_column(list(frame.columns))
        if date_col is None or value_col is None:
            continue
        series_id = path.stem.upper()
        normalized = pd.DataFrame(
            {
                "Date": pd.to_datetime(frame[date_col], errors="coerce"),
                series_id: pd.to_numeric(frame[value_col], errors="coerce"),
            }
        ).dropna(subset=["Date"])
        frames.append(normalized)

    if not frames:
        return pd.DataFrame(columns=["Date"])

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="Date", how="outer")
    return merged.sort_values("Date").reset_index(drop=True)


def latest_value_at_or_before(frame: pd.DataFrame, series_id: str, as_of_date: str) -> float | None:
    if frame.empty or series_id not in frame.columns:
        return None
    ts = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(ts):
        return None
    eligible = frame[pd.to_datetime(frame["Date"], errors="coerce") <= ts]
    if eligible.empty:
        return None
    value = pd.to_numeric(eligible[series_id], errors="coerce").dropna()
    if value.empty:
        return None
    return float(value.iloc[-1])


def fetch_fred_series(
    series_id: str,
    cache_dir: Path,
    *,
    api_key: str | None = None,
    allow_network: bool = False,
) -> pd.DataFrame:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"{series_id.upper()}.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path)
    if not allow_network:
        raise FredError(f"FRED series cache missing and network disabled: {cache_path}")
    if not api_key:
        raise FredError(f"FRED API key is required to fetch {series_id}.")

    import requests

    response = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
        },
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    observations = payload.get("observations", [])
    with cache_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "value"])
        writer.writeheader()
        for entry in observations:
            writer.writerow({"date": entry.get("date"), "value": entry.get("value")})
    return pd.read_csv(cache_path)
