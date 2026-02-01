from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from market_monitor.gdelt.utils import (
    build_content_hash,
    build_file_fingerprint,
    ensure_dir,
    normalize_event_root_code,
    utc_now_iso,
)
from market_monitor.gdelt.doctor import warn_if_unusable


@dataclass
class FeaturesResult:
    output_path: Path
    manifest_path: Path
    rows: int
    min_day: str | None
    max_day: str | None


def _detect_partition_files(day_dir: Path) -> list[Path]:
    parquet_files = sorted(day_dir.glob("*.parquet"))
    if parquet_files:
        return parquet_files
    return sorted(day_dir.glob("*.csv"))


def _load_day_frame(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    if files[0].suffix == ".parquet":
        return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True)
    return pd.concat([pd.read_csv(path) for path in files], ignore_index=True)


def _aggregate_events(df: pd.DataFrame) -> dict[str, Any]:
    features: dict[str, Any] = {"total_event_count": int(len(df))}
    if "event_root_code" in df.columns:
        root_codes = normalize_event_root_code(df["event_root_code"]).fillna("unknown")
        counts = root_codes.value_counts()
        for code, count in counts.items():
            features[f"event_root_code_{code}"] = int(count)
    if "quad_class" in df.columns:
        quad = pd.to_numeric(df["quad_class"], errors="coerce")
        features["cooperation_event_count"] = int(quad.isin([1, 2]).sum())
        features["conflict_event_count"] = int(quad.isin([3, 4]).sum())
    if "goldstein_scale" in df.columns:
        goldstein = pd.to_numeric(df["goldstein_scale"], errors="coerce")
        features["goldstein_mean"] = float(goldstein.mean()) if goldstein.notna().any() else None
        features["goldstein_sum"] = float(goldstein.sum()) if goldstein.notna().any() else None
    if "avg_tone" in df.columns:
        tone = pd.to_numeric(df["avg_tone"], errors="coerce")
        features["tone_mean"] = float(tone.mean()) if tone.notna().any() else None
    for column, name in [
        ("num_articles", "num_articles_sum"),
        ("num_sources", "num_sources_sum"),
        ("num_mentions", "num_mentions_sum"),
    ]:
        if column in df.columns:
            values = pd.to_numeric(df[column], errors="coerce")
            features[name] = float(values.sum()) if values.notna().any() else None
    return features


def build_daily_features(
    *,
    gdelt_dir: Path,
    out_path: Path,
    by_country: bool = False,
) -> FeaturesResult:
    events_root = gdelt_dir / "events"
    if not events_root.exists():
        raise FileNotFoundError(
            f"GDELT events directory not found at {events_root}. "
            "Remediation: run the ingest step with --format events."
        )

    day_dirs = sorted([path for path in events_root.glob("day=*") if path.is_dir()])
    records: list[dict[str, Any]] = []

    for day_dir in day_dirs:
        day = day_dir.name.split("day=")[-1]
        files = _detect_partition_files(day_dir)
        if not files:
            continue
        df = _load_day_frame(files)
        if df.empty:
            continue
        if by_country and "actiongeo_country_code" in df.columns:
            df["actiongeo_country_code"] = df["actiongeo_country_code"].fillna("UNKNOWN")
            grouped = df.groupby("actiongeo_country_code")
            for country, group in grouped:
                features = _aggregate_events(group)
                features["day"] = day
                features["country"] = country
                records.append(features)
        else:
            features = _aggregate_events(df)
            features["day"] = day
            records.append(features)

    result_df = pd.DataFrame(records)
    if result_df.empty:
        raise ValueError(
            "No daily features were generated. Remediation: ensure events partitions contain valid rows."
        )
    result_df = result_df.sort_values(by=["day"] + (["country"] if "country" in result_df.columns else []))

    ensure_dir(out_path.parent)
    if out_path.suffix == ".parquet":
        import importlib.util

        if importlib.util.find_spec("pyarrow") is None:
            raise ImportError("pyarrow is required for parquet output. Use a .csv path instead.")
        result_df.to_parquet(out_path, index=False)
    else:
        result_df.to_csv(out_path, index=False)

    manifest_dir = gdelt_dir / "features"
    ensure_dir(manifest_dir)
    manifest_path = manifest_dir / "manifest.json"
    columns = list(result_df.columns)
    manifest = {
        "schema_version": 1,
        "created_utc": utc_now_iso(),
        "coverage": {
            "min_day": result_df["day"].min(),
            "max_day": result_df["day"].max(),
            "n_days": int(result_df["day"].nunique()),
        },
        "row_counts": {"total_rows": int(len(result_df))},
        "columns": columns,
        "content_hash": build_content_hash(
            {
                "gdelt_dir": str(gdelt_dir),
                "out_path": str(out_path),
                "by_country": by_country,
                "inputs": build_file_fingerprint([path for path in events_root.glob("**/*") if path.is_file()]),
            }
        ),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return FeaturesResult(
        output_path=out_path,
        manifest_path=manifest_path,
        rows=len(result_df),
        min_day=result_df["day"].min(),
        max_day=result_df["day"].max(),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build daily GDELT-derived features.")
    parser.add_argument("--gdelt-dir", required=True, help="Root GDELT cache directory.")
    parser.add_argument("--out", required=True, help="Output file path for daily features.")
    parser.add_argument("--by-country", action="store_true", help="Group features by actiongeo country code.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    env_raw = os.getenv("MARKET_APP_GDELT_RAW_DIR") or os.getenv("MARKET_APP_GDELT_EVENTS_RAW_DIR")
    if env_raw:
        warn_if_unusable(Path(env_raw).expanduser(), file_glob="*.csv", context="gdelt.features_daily")
    try:
        result = build_daily_features(
            gdelt_dir=Path(args.gdelt_dir).expanduser(),
            out_path=Path(args.out).expanduser(),
            by_country=args.by_country,
        )
    except (FileNotFoundError, ValueError, ImportError) as exc:
        print(f"[gdelt.features_daily] {exc}")
        return 2
    print(f"[gdelt.features_daily] wrote: {result.output_path}")
    print(f"[gdelt.features_daily] manifest: {result.manifest_path}")
    print(f"[gdelt.features_daily] coverage: {result.min_day} -> {result.max_day}")
    print(f"[gdelt.features_daily] rows: {result.rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
