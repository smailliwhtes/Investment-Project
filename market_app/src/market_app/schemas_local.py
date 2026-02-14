from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class DataSchema:
    name: str
    required_columns: tuple[str, ...]

    def validate(self, frame: pd.DataFrame) -> None:
        missing = [col for col in self.required_columns if col not in frame.columns]
        if missing:
            raise RuntimeError(f"{self.name} missing required columns: {missing}")


UNIVERSE_SCHEMA = DataSchema(
    name="Universe",
    required_columns=("symbol", "name", "exchange", "asset_type", "is_etf"),
)

OHLCV_SCHEMA = DataSchema(
    name="OHLCV",
    required_columns=("date", "open", "high", "low", "close", "volume"),
)

DATA_QUALITY_SCHEMA = DataSchema(
    name="DataQuality",
    required_columns=(
        "symbol",
        "last_date",
        "as_of_date",
        "lag_days",
        "lag_bin",
        "n_rows",
        "missing_days",
        "zero_volume_fraction",
        "bad_ohlc_count",
        "stale_data",
        "stale",
        "has_volume",
        "dq_flags",
    ),
)

FEATURES_SCHEMA = DataSchema(
    name="Features",
    required_columns=(
        "symbol",
        "return_1m",
        "return_3m",
        "return_6m",
        "return_12m",
        "close_to_sma20",
        "close_to_sma50",
        "close_to_sma200",
        "volatility_20d",
        "volatility_60d",
        "max_drawdown_6m",
        "adv20_usd",
    ),
)

SCORE_SCHEMA = DataSchema(
    name="Score",
    required_columns=("symbol", "monitor_score", "total_score", "risk_flags", "last_date", "lag_days", "lag_bin"),
)
