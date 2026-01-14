from pathlib import Path

import pandas as pd

FEATURE_COLUMNS = [
    "run_id",
    "run_timestamp_utc",
    "config_hash",
    "provider_name",
    "adjusted_mode",
    "symbol",
    "name",
    "history_days",
    "data_freshness_days",
    "missing_frac",
    "zero_volume_frac",
    "data_status",
    "data_reason_codes",
    "ret_1m",
    "ret_3m",
    "ret_6m",
    "ret_12m",
    "sma20_ratio",
    "sma50_ratio",
    "sma200_ratio",
    "pct_days_above_sma200",
    "vol20_ann",
    "vol60_ann",
    "downside_vol_ann",
    "worst_5d_return",
    "max_drawdown_6m",
    "adv20_dollar",
    "theme_tags",
    "theme_purity_score",
    "scenario_defense",
    "scenario_tech",
    "scenario_metals",
]

SCORED_COLUMNS = FEATURE_COLUMNS + [
    "eligible",
    "gate_fail_codes",
    "risk_red_codes",
    "risk_amber_codes",
    "notes",
    "trend",
    "momentum",
    "liquidity",
    "vol_penalty",
    "dd_penalty",
    "tail_penalty",
    "theme_bonus",
    "raw_score",
    "decile",
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
    _ensure_columns(df, columns).to_csv(path, index=False)
