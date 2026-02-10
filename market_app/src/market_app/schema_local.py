from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


SCHEMA_VERSION = "1.0.0"

SCHEMAS: dict[str, list[str]] = {
    "universe.csv": [
        "symbol",
        "name",
        "exchange",
        "asset_type",
        "is_etf",
        "is_test_issue",
        "is_leveraged",
        "is_inverse",
        "country",
        "source_file",
    ],
    "classified.csv": [
        "symbol",
        "name",
        "themes",
        "theme_confidence",
        "theme_evidence",
        "theme_uncertain",
    ],
    "features.csv": [
        "symbol",
        "as_of_date",
        "history_days",
        "return_1m",
        "return_3m",
        "return_6m",
        "return_12m",
        "sma20",
        "sma50",
        "sma200",
        "close_to_sma20",
        "close_to_sma50",
        "close_to_sma200",
        "pct_days_above_sma200_6m",
        "volatility_20d",
        "volatility_60d",
        "downside_volatility",
        "worst_5d_return_6m",
        "max_drawdown_6m",
        "adv20_usd",
        "zero_volume_fraction_60d",
        "missing_data",
        "stale_data",
        "split_suspect",
        "volume_missing",
    ],
    "eligible.csv": ["symbol", "eligible", "gate_fail_reasons"],
    "scored.csv": [
        "symbol",
        "monitor_score",
        "total_score",
        "risk_flags",
        "risk_level",
        "themes",
        "theme_confidence",
        "predicted_risk_signal",
        "model_id",
        "model_schema_version",
    ],
}


@dataclass(frozen=True)
class SchemaValidationResult:
    ok: bool
    missing: dict[str, list[str]]


def validate_output_schema(outputs: dict[str, pd.DataFrame]) -> SchemaValidationResult:
    missing: dict[str, list[str]] = {}
    for name, expected in SCHEMAS.items():
        frame = outputs.get(name)
        if frame is None:
            missing[name] = expected
            continue
        absent = [col for col in expected if col not in frame.columns]
        if absent:
            missing[name] = absent
    return SchemaValidationResult(ok=not missing, missing=missing)


def assert_output_schema(outputs: dict[str, pd.DataFrame]) -> None:
    result = validate_output_schema(outputs)
    if not result.ok:
        raise RuntimeError(f"Output schema validation failed: {result.missing}")
