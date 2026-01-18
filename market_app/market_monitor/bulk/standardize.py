from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class StandardizeResult:
    input_path: Path
    output_path: Path
    rows: int
    warnings: list[str]


def standardize_ohlcv_csv(input_path: Path, output_path: Path) -> StandardizeResult:
    df = pd.read_csv(input_path)
    warnings: list[str] = []
    column_map = _map_columns(df.columns)

    if "Date" not in column_map.values():
        raise ValueError(f"Missing date column in {input_path}")
    df = df.rename(columns=column_map)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")

    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            warnings.append(f"Missing column {col}")

    df = df.dropna(subset=["Close"]) if "Close" in df.columns else df
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    return StandardizeResult(input_path=input_path, output_path=output_path, rows=len(df), warnings=warnings)


def standardize_timeseries_csv(
    input_path: Path,
    output_path: Path,
    *,
    value_column: str | None = None,
) -> StandardizeResult:
    df = pd.read_csv(input_path)
    warnings: list[str] = []
    column_map = _map_columns(df.columns)

    if "Date" not in column_map.values():
        raise ValueError(f"Missing date column in {input_path}")
    df = df.rename(columns=column_map)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")

    if value_column is None:
        candidates = [col for col in df.columns if col not in {"Date"}]
        if not candidates:
            raise ValueError(f"No value column found in {input_path}")
        value_column = candidates[0]
    if value_column not in df.columns:
        raise ValueError(f"Value column {value_column} not found in {input_path}")

    df["Value"] = pd.to_numeric(df[value_column], errors="coerce")
    df = df.dropna(subset=["Value"])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df[["Date", "Value"]].to_csv(output_path, index=False)
    return StandardizeResult(input_path=input_path, output_path=output_path, rows=len(df), warnings=warnings)


def standardize_directory(
    input_dir: Path,
    output_dir: Path,
    *,
    mode: str,
    value_column: str | None = None,
) -> list[StandardizeResult]:
    results: list[StandardizeResult] = []
    for path in sorted(input_dir.glob("*.csv")):
        output_path = output_dir / path.name
        if mode == "ohlcv":
            results.append(standardize_ohlcv_csv(path, output_path))
        elif mode == "timeseries":
            results.append(
                standardize_timeseries_csv(path, output_path, value_column=value_column)
            )
        else:
            raise ValueError("mode must be 'ohlcv' or 'timeseries'")
    return results


def _map_columns(columns: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for col in columns:
        key = col.strip().lower()
        if key in {"date", "timestamp"}:
            mapping[col] = "Date"
        elif key in {"open"}:
            mapping[col] = "Open"
        elif key in {"high"}:
            mapping[col] = "High"
        elif key in {"low"}:
            mapping[col] = "Low"
        elif key in {"close", "adj close", "adjusted_close", "adjusted close"}:
            mapping[col] = "Close"
        elif key in {"volume"}:
            mapping[col] = "Volume"
    return mapping
