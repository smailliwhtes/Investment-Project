from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pandas as pd

from market_monitor.cache import get_or_fetch
from market_monitor.features import compute_features
from market_monitor.gates import apply_gates
from market_monitor.providers.base import HistoryProvider, ProviderError, ProviderLimitError
from market_monitor.risk import assess_risk
from market_monitor.scenarios import scenario_scores
from market_monitor.themes import tag_themes


def _history_fetch(provider: HistoryProvider, symbol: str, days: int) -> pd.DataFrame:
    return provider.get_history(symbol, days)


def _compute_data_status(history_days: int, min_days: int) -> tuple[str, list[str]]:
    if history_days <= 0:
        return "DATA_UNAVAILABLE", ["NO_HISTORY"]
    if history_days < min_days:
        return "INSUFFICIENT_HISTORY", ["INSUFFICIENT_HISTORY"]
    return "OK", []


def _apply_adjusted(df: pd.DataFrame, provider: HistoryProvider) -> tuple[pd.DataFrame, str]:
    if provider.capabilities.supports_adjusted and "Adjusted_Close" in df.columns:
        df = df.copy()
        df["Close"] = df["Adjusted_Close"]
        return df, "ADJUSTED"
    return df, "UNADJUSTED"


def stage_pipeline(
    symbols: pd.DataFrame,
    provider: HistoryProvider,
    cache_dir: Path,
    max_cache_age_days: float,
    config: dict[str, Any],
    run_meta: dict[str, Any],
    logger,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    staging_cfg = config["staging"]
    gates_cfg = config["gates"]
    themes_cfg = config.get("themes", {})
    stage1_survivors: list[dict[str, Any]] = []
    stage2_survivors: list[dict[str, Any]] = []
    stage3_rows: list[dict[str, Any]] = []

    def fetch_cached(symbol: str, days: int):
        return get_or_fetch(
            cache_dir=cache_dir,
            provider_name=provider.name,
            symbol=symbol,
            adjusted_mode="UNADJUSTED",
            max_cache_age_days=max_cache_age_days,
            fetch_fn=lambda: _history_fetch(provider, symbol, days),
            delta_days=10,
        )

    logger.info("Stage 1: micro-history gate")
    for _, row in symbols.iterrows():
        symbol = row["symbol"]
        name = row.get("name") or symbol
        try:
            cache_res = fetch_cached(symbol, staging_cfg["stage1_micro_days"])
            df, adjusted_mode = _apply_adjusted(cache_res.df, provider)
            if df.empty:
                status = "DATA_UNAVAILABLE"
                stage1_survivors.append(
                    {
                        **run_meta,
                        "symbol": symbol,
                        "name": name,
                        "data_status": status,
                        "data_reason_codes": "NO_HISTORY",
                    }
                )
                continue
            last_price = float(df["Close"].iloc[-1])
            features = compute_features(df)
            features["last_price"] = last_price
            if last_price > gates_cfg["price_max"]:
                continue
            stage1_survivors.append(
                {
                    **run_meta,
                    "symbol": symbol,
                    "name": name,
                    "last_price": last_price,
                }
            )
        except ProviderLimitError:
            stage1_survivors.append(
                {
                    **run_meta,
                    "symbol": symbol,
                    "name": name,
                    "data_status": "DATA_UNAVAILABLE",
                    "data_reason_codes": "LIMIT_EXCEEDED",
                }
            )
        except ProviderError as exc:
            stage1_survivors.append(
                {
                    **run_meta,
                    "symbol": symbol,
                    "name": name,
                    "data_status": "DATA_UNAVAILABLE",
                    "data_reason_codes": str(exc),
                }
            )

    logger.info("Stage 2: short-history gates")
    for row in stage1_survivors:
        symbol = row["symbol"]
        name = row.get("name") or symbol
        try:
            cache_res = fetch_cached(symbol, staging_cfg["stage2_short_days"])
            df, adjusted_mode = _apply_adjusted(cache_res.df, provider)
            features = compute_features(df)
            features["last_price"] = float(df["Close"].iloc[-1]) if not df.empty else math.nan
            status, reason_codes = _compute_data_status(
                int(features.get("history_days", 0)), staging_cfg["history_min_days"]
            )
            eligible, gate_fail = apply_gates(
                features,
                gates_cfg["price_max"],
                gates_cfg["min_adv20_dollar"],
                gates_cfg["max_zero_volume_frac"],
                staging_cfg["history_min_days"],
            )
            if eligible:
                stage2_survivors.append(
                    {
                        **row,
                        "name": name,
                    }
                )
            row.update(
                {
                    "data_status": status,
                    "data_reason_codes": ";".join(reason_codes),
                    "eligible": eligible,
                    "gate_fail_codes": ";".join(gate_fail),
                }
            )
        except ProviderLimitError:
            row.update(
                {
                    "data_status": "DATA_UNAVAILABLE",
                    "data_reason_codes": "LIMIT_EXCEEDED",
                    "eligible": False,
                    "gate_fail_codes": "LIMIT_EXCEEDED",
                }
            )
        except ProviderError as exc:
            row.update(
                {
                    "data_status": "DATA_UNAVAILABLE",
                    "data_reason_codes": str(exc),
                    "eligible": False,
                    "gate_fail_codes": "DATA_UNAVAILABLE",
                }
            )

    logger.info("Stage 3: deep history features + scoring")
    for row in stage2_survivors:
        symbol = row["symbol"]
        name = row.get("name") or symbol
        try:
            cache_res = fetch_cached(symbol, staging_cfg["stage3_deep_days"])
            df, adjusted_mode = _apply_adjusted(cache_res.df, provider)
            features = compute_features(df)
            features["last_price"] = float(df["Close"].iloc[-1]) if not df.empty else math.nan
            status, reason_codes = _compute_data_status(
                int(features.get("history_days", 0)), staging_cfg["history_min_days"]
            )
            theme_tags, purity = tag_themes(symbol, name, themes_cfg)
            scenario = scenario_scores(theme_tags)
            red, amber = assess_risk(features, adjusted_mode=adjusted_mode)
            notes = "Eligible for monitoring" if status == "OK" else "Data issue"
            stage3_rows.append(
                {
                    **run_meta,
                    "symbol": symbol,
                    "name": name,
                    "data_status": status,
                    "data_reason_codes": ";".join(reason_codes),
                    "data_freshness_days": cache_res.data_freshness_days,
                    "adjusted_mode": adjusted_mode,
                    "theme_tags": ";".join(theme_tags),
                    "theme_purity_score": purity,
                    "risk_red_codes": ";".join(red),
                    "risk_amber_codes": ";".join(amber),
                    "eligible": True,
                    "gate_fail_codes": "",
                    "notes": notes,
                    **features,
                    **scenario,
                }
            )
        except ProviderLimitError:
            stage3_rows.append(
                {
                    **run_meta,
                    "symbol": symbol,
                    "name": name,
                    "data_status": "DATA_UNAVAILABLE",
                    "data_reason_codes": "LIMIT_EXCEEDED",
                }
            )
        except ProviderError as exc:
            stage3_rows.append(
                {
                    **run_meta,
                    "symbol": symbol,
                    "name": name,
                    "data_status": "DATA_UNAVAILABLE",
                    "data_reason_codes": str(exc),
                }
            )

    summary = {
        "universe": len(symbols),
        "stage1": len(stage1_survivors),
        "stage2": len(stage2_survivors),
        "stage3": len(stage3_rows),
    }

    stage1_df = pd.DataFrame(stage1_survivors)
    stage2_df = pd.DataFrame(stage2_survivors)
    stage3_df = pd.DataFrame(stage3_rows)
    return stage1_df, stage2_df, stage3_df, summary
