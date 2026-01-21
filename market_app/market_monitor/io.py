from pathlib import Path

import pandas as pd

BASE_FEATURE_COLUMNS = [
    "run_id",
    "run_timestamp_utc",
    "config_hash",
    "provider_name",
    "as_of_date",
    "adjusted_mode",
    "symbol",
    "name",
    "history_days",
    "data_freshness_days",
    "missing_frac",
    "missing_day_rate",
    "zero_volume_frac",
    "volume_available",
    "data_status",
    "data_reason_codes",
    "ret_1m",
    "ret_3m",
    "ret_6m",
    "ret_12m",
    "mom_12_1",
    "mom_6_1",
    "accel_1m_vs_6m",
    "sma20_ratio",
    "sma50_ratio",
    "sma200_ratio",
    "pct_days_above_sma200",
    "trend_slope_3m",
    "trend_r2_3m",
    "trend_quality_3m",
    "trend_slope_6m",
    "trend_r2_6m",
    "trend_quality_6m",
    "trend_slope_12m",
    "trend_r2_12m",
    "trend_quality_12m",
    "distance_to_63d_high",
    "distance_to_52w_high",
    "vol20_ann",
    "vol60_ann",
    "vol_of_vol",
    "downside_vol_ann",
    "worst_5d_return",
    "max_drawdown_6m",
    "cvar_60d",
    "ulcer_index_60d",
    "drawdown_duration_60d",
    "gap_atr",
    "range_expansion",
    "big_day_freq",
    "close_to_high",
    "volume_z",
    "adv20_dollar",
    "adv20_volume",
    "stale_price_flag",
    "corp_action_suspect",
    "theme_tags",
    "theme_confidence",
    "theme_unknown",
    "scenario_defense",
    "scenario_tech",
    "scenario_metals",
    "silver_ret_1m",
    "silver_ret_3m",
    "silver_vol_3m",
    "silver_max_drawdown_6m",
    "silver_trend_slope_6m",
    "silver_trend_r2_6m",
    "silver_trend_quality_6m",
]

BASE_SCORED_COLUMNS = BASE_FEATURE_COLUMNS + [
    "eligible",
    "gate_fail_codes",
    "risk_level",
    "risk_red_codes",
    "risk_amber_codes",
    "notes",
    "confidence_score",
    "trend",
    "momentum",
    "liquidity",
    "quality",
    "vol_penalty",
    "dd_penalty",
    "tail_penalty",
    "attention",
    "theme_bonus",
    "volume_missing_penalty",
    "raw_score",
    "decile",
    "monitor_score_1_10",
    "monitor_priority_1_10",
    "score_components",
]

ELIGIBLE_COLUMNS = [
    "symbol",
    "name",
    "eligible",
    "gate_fail_codes",
    "notes",
]


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df[columns]


def write_csv(df: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = _ensure_columns(df.copy(), columns)
    float_cols = cleaned.select_dtypes(include=["float", "float32", "float64"]).columns
    if len(float_cols):
        cleaned[float_cols] = cleaned[float_cols].round(6)
    cleaned.to_csv(path, index=False)


def build_feature_columns(extra_columns: list[str] | None = None) -> list[str]:
    extras = sorted(extra_columns or [])
    return BASE_FEATURE_COLUMNS + extras


def build_scored_columns(extra_columns: list[str] | None = None) -> list[str]:
    extras = sorted(extra_columns or [])
    return BASE_SCORED_COLUMNS + extras
