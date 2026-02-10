from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from market_app.manifest_local import hash_file, hash_text


@dataclass(frozen=True)
class GeopoliticsResult:
    frame: pd.DataFrame
    cache_path: Path | None
    input_hash: str | None
    input_files: list[Path]


ROOT_CODES = [f"{idx:02d}" for idx in range(1, 21)]
QUAD_CLASSES = ["1", "2", "3", "4"]


def _collect_inputs(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.exists():
        return []
    files = sorted(list(path.glob("**/*.csv")) + list(path.glob("**/*.parquet")))
    return files


def _load_frame(files: Iterable[Path]) -> pd.DataFrame:
    frames = []
    for file in files:
        if file.suffix.lower() == ".parquet":
            frames.append(pd.read_parquet(file))
        else:
            frames.append(pd.read_csv(file))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _hash_inputs(files: list[Path]) -> str:
    payload = {str(path): hash_file(path) for path in files}
    return hash_text(json.dumps(payload, sort_keys=True))


def _infer_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    lower = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def build_geopolitics_features(
    *,
    geopolitics_path: Path,
    output_dir: Path,
) -> GeopoliticsResult:
    files = _collect_inputs(geopolitics_path)
    if not files:
        return GeopoliticsResult(
            frame=pd.DataFrame(),
            cache_path=None,
            input_hash=None,
            input_files=[],
        )
    input_hash = _hash_inputs(files)
    cache_dir = output_dir / "cache" / "geopolitics"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / f"gdelt_daily_{input_hash[:10]}.parquet"
    if cache_path.exists():
        return GeopoliticsResult(
            frame=pd.read_parquet(cache_path),
            cache_path=cache_path,
            input_hash=input_hash,
            input_files=files,
        )

    raw = _load_frame(files)
    if raw.empty:
        empty = pd.DataFrame()
        empty.to_parquet(cache_path, index=False)
        return GeopoliticsResult(
            frame=empty,
            cache_path=cache_path,
            input_hash=input_hash,
            input_files=files,
        )

    day_col = _infer_column(raw.columns, ["SQLDATE", "Day", "Date"])
    if not day_col:
        raise ValueError("GDELT data missing a recognizable date column (SQLDATE/Date).")
    root_col = _infer_column(raw.columns, ["EventRootCode", "RootEventCode"])
    quad_col = _infer_column(raw.columns, ["QuadClass"])
    tone_col = _infer_column(raw.columns, ["AvgTone"])
    gold_col = _infer_column(raw.columns, ["GoldsteinScale"])
    mention_col = _infer_column(raw.columns, ["NumMentions"])
    source_col = _infer_column(raw.columns, ["NumSources"])
    article_col = _infer_column(raw.columns, ["NumArticles"])

    df = raw.copy()
    df["day"] = pd.to_datetime(df[day_col], errors="coerce")
    df = df.dropna(subset=["day"]).copy()
    df["day"] = df["day"].dt.strftime("%Y-%m-%d")

    df["root_code"] = (
        df[root_col].astype(str).str.zfill(2) if root_col else "00"
    )
    df["quad_class"] = df[quad_col].astype(str) if quad_col else "0"
    df["avg_tone"] = pd.to_numeric(df[tone_col], errors="coerce") if tone_col else 0.0
    df["goldstein"] = pd.to_numeric(df[gold_col], errors="coerce") if gold_col else 0.0
    df["mentions"] = pd.to_numeric(df[mention_col], errors="coerce").fillna(0.0) if mention_col else 0.0
    df["sources"] = pd.to_numeric(df[source_col], errors="coerce").fillna(0.0) if source_col else 0.0
    df["articles"] = pd.to_numeric(df[article_col], errors="coerce").fillna(0.0) if article_col else 0.0

    grouped = df.groupby("day")
    daily = grouped.agg(
        events_count=("day", "size"),
        tone_mean=("avg_tone", "mean"),
        tone_std=("avg_tone", "std"),
        goldstein_mean=("goldstein", "mean"),
        mentions_sum=("mentions", "sum"),
        sources_sum=("sources", "sum"),
        articles_sum=("articles", "sum"),
    )

    root_counts = (
        pd.crosstab(df["day"], df["root_code"])
        .reindex(columns=ROOT_CODES, fill_value=0)
        .add_prefix("root_")
    )
    quad_counts = (
        pd.crosstab(df["day"], df["quad_class"])
        .reindex(columns=QUAD_CLASSES, fill_value=0)
        .add_prefix("quad_")
    )

    features = pd.concat([daily, root_counts, quad_counts], axis=1).reset_index()
    features = features.fillna(0.0)
    features = features.sort_values("day").reset_index(drop=True)
    features.to_parquet(cache_path, index=False)

    return GeopoliticsResult(
        frame=features,
        cache_path=cache_path,
        input_hash=input_hash,
        input_files=files,
    )


def lag_geopolitics_features(frame: pd.DataFrame, lag_days: int) -> pd.DataFrame:
    if frame.empty:
        return frame
    df = frame.copy()
    df["day"] = pd.to_datetime(df["day"], errors="coerce")
    df = df.dropna(subset=["day"])
    df["day"] = df["day"] + pd.Timedelta(days=lag_days)
    df["day"] = df["day"].dt.strftime("%Y-%m-%d")
    return df.sort_values("day").reset_index(drop=True)
