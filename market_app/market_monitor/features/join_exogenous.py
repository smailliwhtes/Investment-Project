from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from market_monitor.gdelt.utils import build_content_hash, build_file_fingerprint, ensure_dir, utc_now_iso


@dataclass(frozen=True)
class JoinResult:
    output_dir: Path
    manifest_path: Path
    rows: int
    min_day: str | None
    max_day: str | None
    partitions: int


def _collect_input_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if not path.exists():
        raise FileNotFoundError(f"Input path not found: {path}")
    partitioned = sorted(path.glob("day=*/part-00000.*"))
    if partitioned:
        return partitioned
    parquet_files = sorted(path.glob("**/*.parquet"))
    csv_files = sorted(path.glob("**/*.csv"))
    if parquet_files:
        return parquet_files
    return csv_files


def _load_frame(path: Path) -> pd.DataFrame:
    files = _collect_input_files(path)
    if not files:
        raise ValueError(f"No input files found under {path}")
    if files[0].suffix == ".parquet":
        return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    return pd.concat([pd.read_csv(file) for file in files], ignore_index=True)


def _infer_column(columns: Iterable[str], candidates: Iterable[str]) -> str | None:
    lower = {col.lower(): col for col in columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def _normalize_day(df: pd.DataFrame, column: str, *, label: str) -> pd.DataFrame:
    normalized = pd.to_datetime(df[column], errors="coerce")
    if normalized.isna().all():
        raise ValueError(f"{label} day column '{column}' could not be parsed to dates")
    result = df.copy()
    result["day"] = normalized.dt.strftime("%Y-%m-%d")
    return result


def _is_count_column(name: str) -> bool:
    lowered = name.lower()
    return (
        lowered.endswith("_count")
        or lowered.endswith("_sum")
        or lowered.startswith("num_")
        or "mentions" in lowered
    )


def _parse_lags(lags: Iterable[int] | None) -> list[int]:
    if not lags:
        return []
    unique = sorted({int(lag) for lag in lags if int(lag) > 0})
    return unique


def _build_gdelt_features(
    df: pd.DataFrame,
    *,
    lags: list[int],
    rolling_window: int | None,
    rolling_mean: bool,
    rolling_sum: bool,
    count_columns: list[str] | None,
    rolling_min_periods: int | None,
    include_raw: bool,
) -> pd.DataFrame:
    frame = df.copy()
    frame = frame.sort_values("day")
    frame = frame.set_index("day", drop=False)
    numeric_cols = [col for col in frame.columns if col != "day" and pd.api.types.is_numeric_dtype(frame[col])]

    if lags:
        for lag in lags:
            for col in numeric_cols:
                frame[f"{col}_lag_{lag}"] = frame[col].shift(lag)

    if rolling_window and (rolling_mean or rolling_sum):
        if count_columns is None:
            count_columns = [col for col in numeric_cols if _is_count_column(col)]
        min_periods = rolling_min_periods if rolling_min_periods is not None else rolling_window
        if rolling_mean:
            for col in count_columns:
                frame[f"{col}_roll{rolling_window}_mean"] = (
                    frame[col].rolling(window=rolling_window, min_periods=min_periods).mean()
                )
        if rolling_sum:
            for col in count_columns:
                frame[f"{col}_roll{rolling_window}_sum"] = (
                    frame[col].rolling(window=rolling_window, min_periods=min_periods).sum()
                )

    if not include_raw:
        derived_cols = [
            col
            for col in frame.columns
            if col == "day" or col.endswith(tuple([f"_lag_{lag}" for lag in lags])) or "_roll" in col
        ]
        frame = frame[list(dict.fromkeys(derived_cols))]

    return frame.reset_index(drop=True)


def build_joined_features(
    *,
    market_path: Path,
    gdelt_path: Path,
    out_dir: Path,
    lags: Iterable[int] | None = (1, 3, 7),
    rolling_window: int | None = None,
    rolling_mean: bool = False,
    rolling_sum: bool = False,
    count_columns: list[str] | None = None,
    market_day_column: str | None = None,
    market_symbol_column: str = "symbol",
    gdelt_day_column: str | None = None,
    output_format: str = "parquet",
    rolling_min_periods: int | None = None,
    include_raw_gdelt: bool = False,
) -> JoinResult:
    market_df = _load_frame(market_path)
    gdelt_df = _load_frame(gdelt_path)

    if market_day_column is None:
        market_day_column = _infer_column(market_df.columns, ["day", "date", "as_of_date", "Date"])
    if not market_day_column:
        raise ValueError("Market data missing a recognizable day column")
    if gdelt_day_column is None:
        gdelt_day_column = _infer_column(gdelt_df.columns, ["day", "date", "Date"])
    if not gdelt_day_column:
        raise ValueError("GDELT data missing a recognizable day column")

    market_df = _normalize_day(market_df, market_day_column, label="market")
    gdelt_df = _normalize_day(gdelt_df, gdelt_day_column, label="gdelt")

    if market_symbol_column not in market_df.columns:
        raise ValueError(f"Market data missing symbol column '{market_symbol_column}'")
    if market_symbol_column != "symbol":
        market_df = market_df.rename(columns={market_symbol_column: "symbol"})

    if gdelt_day_column != "day":
        gdelt_df = gdelt_df.drop(columns=[gdelt_day_column], errors="ignore")

    lag_list = _parse_lags(lags)
    if not include_raw_gdelt and not lag_list and not (rolling_window and (rolling_mean or rolling_sum)):
        raise ValueError("No GDELT features selected: enable lags/rolling or set include_raw_gdelt.")
    gdelt_features = _build_gdelt_features(
        gdelt_df,
        lags=lag_list,
        rolling_window=rolling_window,
        rolling_mean=rolling_mean,
        rolling_sum=rolling_sum,
        count_columns=count_columns,
        rolling_min_periods=rolling_min_periods,
        include_raw=include_raw_gdelt,
    )

    if market_day_column != "day":
        market_df = market_df.drop(columns=[market_day_column], errors="ignore")

    joined = market_df.merge(gdelt_features, on="day", how="left")
    if joined.empty:
        raise ValueError("Joined features are empty; check inputs and join keys")

    joined = joined.sort_values(["day", "symbol"])
    market_columns = [col for col in market_df.columns if col not in {"day", "symbol"}]
    gdelt_columns = [col for col in joined.columns if col not in {"day", "symbol", *market_columns}]
    joined = joined[["day", "symbol", *market_columns, *gdelt_columns]]

    ensure_dir(out_dir)
    output_ext = ".parquet" if output_format == "parquet" else ".csv"

    unique_days = sorted(joined["day"].dropna().unique())
    for day in unique_days:
        day_dir = out_dir / f"day={day}"
        ensure_dir(day_dir)
        day_frame = joined[joined["day"] == day]
        out_path = day_dir / f"part-00000{output_ext}"
        if output_format == "parquet":
            day_frame.to_parquet(out_path, index=False)
        else:
            day_frame.to_csv(out_path, index=False)

    manifest_path = out_dir / "manifest.json"
    rows_per_day = {day: int((joined["day"] == day).sum()) for day in unique_days}
    market_files = _collect_input_files(market_path)
    gdelt_files = _collect_input_files(gdelt_path)
    manifest_payload = {
        "schema_version": 1,
        "created_utc": utc_now_iso(),
        "coverage": {
            "min_day": str(joined["day"].min()),
            "max_day": str(joined["day"].max()),
            "n_days": int(len(unique_days)),
        },
        "row_counts": {
            "total_rows": int(len(joined)),
            "rows_per_day": rows_per_day,
        },
        "columns": list(joined.columns),
        "inputs": {
            "market_path": str(market_path),
            "gdelt_path": str(gdelt_path),
            "market_files": build_file_fingerprint(market_files),
            "gdelt_files": build_file_fingerprint(gdelt_files),
        },
        "config": {
            "lags": lag_list,
            "rolling_window": rolling_window,
            "rolling_mean": rolling_mean,
            "rolling_sum": rolling_sum,
            "count_columns": count_columns,
            "rolling_min_periods": rolling_min_periods,
            "output_format": output_format,
            "include_raw_gdelt": include_raw_gdelt,
        },
    }
    manifest_payload["content_hash"] = build_content_hash(manifest_payload)
    manifest_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")

    return JoinResult(
        output_dir=out_dir,
        manifest_path=manifest_path,
        rows=len(joined),
        min_day=str(joined["day"].min()),
        max_day=str(joined["day"].max()),
        partitions=len(unique_days),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Join GDELT daily features to market daily features.")
    parser.add_argument("--market-path", required=True, help="Market daily feature table path (file or dir).")
    parser.add_argument("--gdelt-path", required=True, help="GDELT daily feature table path (file or dir).")
    parser.add_argument("--out-dir", default="data/features/joined", help="Output directory root.")
    parser.add_argument(
        "--lags",
        default="1,3,7",
        help="Comma-separated lag days to apply to GDELT features.",
    )
    parser.add_argument("--rolling-window", type=int, default=None, help="Rolling window for counts.")
    parser.add_argument("--rolling-mean", action="store_true", help="Enable rolling mean for counts.")
    parser.add_argument("--rolling-sum", action="store_true", help="Enable rolling sum for counts.")
    parser.add_argument(
        "--count-columns",
        default=None,
        help="Comma-separated list of count columns (defaults to heuristic).",
    )
    parser.add_argument(
        "--rolling-min-periods",
        type=int,
        default=None,
        help="Min periods for rolling stats (defaults to window size).",
    )
    parser.add_argument(
        "--market-day-column",
        default=None,
        help="Override market day column name (defaults to inferred).",
    )
    parser.add_argument(
        "--market-symbol-column",
        default="symbol",
        help="Market symbol column name.",
    )
    parser.add_argument(
        "--gdelt-day-column",
        default=None,
        help="Override GDELT day column name (defaults to inferred).",
    )
    parser.add_argument(
        "--output-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output file format for partitions.",
    )
    parser.add_argument(
        "--include-raw-gdelt",
        action="store_true",
        help="Include same-day GDELT columns in output (may introduce leakage).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    lags = []
    if args.lags:
        lags = [int(value.strip()) for value in args.lags.split(",") if value.strip()]
    count_columns = None
    if args.count_columns:
        count_columns = [value.strip() for value in args.count_columns.split(",") if value.strip()]
    try:
        result = build_joined_features(
            market_path=Path(args.market_path).expanduser(),
            gdelt_path=Path(args.gdelt_path).expanduser(),
            out_dir=Path(args.out_dir).expanduser(),
            lags=lags,
            rolling_window=args.rolling_window,
            rolling_mean=args.rolling_mean,
            rolling_sum=args.rolling_sum,
            count_columns=count_columns,
            market_day_column=args.market_day_column,
            market_symbol_column=args.market_symbol_column,
            gdelt_day_column=args.gdelt_day_column,
            output_format=args.output_format,
            rolling_min_periods=args.rolling_min_periods,
            include_raw_gdelt=args.include_raw_gdelt,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"[features.join_exogenous] {exc}")
        return 2

    print(f"[features.join_exogenous] wrote: {result.output_dir}")
    print(f"[features.join_exogenous] manifest: {result.manifest_path}")
    print(f"[features.join_exogenous] coverage: {result.min_day} -> {result.max_day}")
    print(f"[features.join_exogenous] rows: {result.rows} partitions: {result.partitions}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
