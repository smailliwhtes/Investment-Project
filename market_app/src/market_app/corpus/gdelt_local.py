from __future__ import annotations

from pathlib import Path

import pandas as pd

_THEME_BUCKETS = {
    "defense": ("defense", "military", "conflict", "security"),
    "tech": ("tech", "ai", "semiconductor", "cyber", "quantum"),
    "metals": ("metal", "mining", "lithium", "uranium", "copper", "nickel"),
}


def build_local_corpus_features(corpus_dir: Path) -> pd.DataFrame:
    files = sorted(corpus_dir.glob("*.csv"))
    if not files:
        return pd.DataFrame()

    frames = []
    for file in files:
        frame = pd.read_csv(file)
        lowered = {c.lower(): c for c in frame.columns}
        date_col = lowered.get("date") or lowered.get("day")
        if not date_col:
            continue
        theme_col = lowered.get("theme") or lowered.get("bucket")
        sentiment_col = lowered.get("sentiment")
        work = pd.DataFrame({"date": pd.to_datetime(frame[date_col], errors="coerce")})
        if theme_col:
            work["theme"] = frame[theme_col].astype(str).str.lower()
        else:
            work["theme"] = ""
        if sentiment_col:
            work["sentiment"] = pd.to_numeric(frame[sentiment_col], errors="coerce")
        frames.append(work.dropna(subset=["date"]))

    if not frames:
        return pd.DataFrame()

    data = pd.concat(frames, ignore_index=True)
    data["date"] = data["date"].dt.date.astype(str)

    rows = []
    for day, day_df in data.groupby("date", sort=True):
        row = {"date": day, "context_events_total": int(len(day_df))}
        for bucket, keywords in _THEME_BUCKETS.items():
            mask = day_df["theme"].apply(lambda text: any(k in text for k in keywords))
            row[f"context_{bucket}_count"] = int(mask.sum())
        if "sentiment" in day_df.columns:
            row["context_sentiment_mean"] = float(day_df["sentiment"].mean())
        rows.append(row)

    out = pd.DataFrame(rows).sort_values("date", kind="mergesort").reset_index(drop=True)
    for col in [c for c in out.columns if c.endswith("_count") or c == "context_events_total"]:
        out[f"{col}_roll7"] = out[col].rolling(7, min_periods=1).sum()
        out[f"{col}_roll30"] = out[col].rolling(30, min_periods=1).sum()
    return out
