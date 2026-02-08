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
from market_monitor.timebase import parse_as_of_date, parse_now_utc, today_utc


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
    *,
    as_of_date: str | None = None,
    now_utc: str | None = None,
    silver_macro: dict[str, float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    staging_cfg = config["staging"]
    gates_cfg = config["gates"]
    themes_cfg = config.get("themes", {})
    stage1_survivors: list[dict[str, Any]] = []
    stage2_survivors: list[dict[str, Any]] = []
    stage3_rows: list[dict[str, Any]] = []
    cache_hits = 0
    cache_misses = 0
    anchor_date = None
    if as_of_date:
        anchor_date = parse_as_of_date(as_of_date)
    elif now_utc:
        anchor_date = parse_now_utc(now_utc).date()

    def fetch_cached(symbol: str, days: int):
        nonlocal cache_hits, cache_misses
        if getattr(provider, "supports_history_cache", False):
            cache_res = provider.get_history_with_cache(
                symbol, days, max_cache_age_days=max_cache_age_days
            )
            cache_hits += int(cache_res.used_cache)
            cache_misses += int(not cache_res.used_cache)
            if days > 0:
                cache_res = cache_res.__class__(
                    df=cache_res.df.tail(days).copy(),
                    data_freshness_days=cache_res.data_freshness_days,
                    cache_path=cache_res.cache_path,
                    used_cache=cache_res.used_cache,
                )
            return cache_res

        cache_res = get_or_fetch(
            cache_dir=cache_dir,
            provider_name=provider.name,
            symbol=symbol,
            adjusted_mode="UNADJUSTED",
            max_cache_age_days=max_cache_age_days,
            fetch_fn=lambda: _history_fetch(provider, symbol, days),
            delta_days=10,
        )
        cache_hits += int(cache_res.used_cache)
        cache_misses += int(not cache_res.used_cache)
        return cache_res

    def _filter_as_of(df: pd.DataFrame) -> pd.DataFrame:
        if anchor_date is None or "Date" not in df.columns:
            return df
        filtered = df.copy()
        filtered["Date"] = pd.to_datetime(filtered["Date"], errors="coerce")
        filtered = filtered.dropna(subset=["Date"])
        filtered = filtered[filtered["Date"] <= pd.to_datetime(anchor_date)]
        return filtered.sort_values("Date").reset_index(drop=True)

    def _data_freshness_days(df: pd.DataFrame) -> int:
        if df.empty or "Date" not in df.columns:
            return 0
        dates = pd.to_datetime(df["Date"], errors="coerce").dropna()
        if dates.empty:
            return 0
        last_date = dates.max().date()
        anchor = anchor_date or today_utc()
        return max((anchor - last_date).days, 0)

    logger.info("Stage 1: micro-history gate")
    for _, row in symbols.iterrows():
        symbol = row["symbol"]
        name = row.get("name") or symbol
        try:
            cache_res = fetch_cached(symbol, staging_cfg["stage1_micro_days"])
            df, adjusted_mode = _apply_adjusted(_filter_as_of(cache_res.df), provider)
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
            eligible_by_price, _ = apply_gates(
                features,
                gates_cfg.get("price_min"),
                gates_cfg.get("price_max"),
            )
            if not eligible_by_price:
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
            df, adjusted_mode = _apply_adjusted(_filter_as_of(cache_res.df), provider)
            features = compute_features(df)
            features["last_price"] = float(df["Close"].iloc[-1]) if not df.empty else math.nan
            status, reason_codes = _compute_data_status(
                int(features.get("history_days", 0)), staging_cfg["history_min_days"]
            )
            logger.info(
                "Stage 2: %s lookback=%s rows=%s history_days=%s min_history=%s status=%s",
                symbol,
                staging_cfg["stage2_short_days"],
                len(df),
                int(features.get("history_days", 0)),
                staging_cfg["history_min_days"],
                status,
            )
            eligible_by_price, gate_fail = apply_gates(
                features,
                gates_cfg.get("price_min"),
                gates_cfg.get("price_max"),
            )
            eligible = bool(status != "DATA_UNAVAILABLE" and eligible_by_price)
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
            df, adjusted_mode = _apply_adjusted(_filter_as_of(cache_res.df), provider)
            features = compute_features(df)
            features["last_price"] = float(df["Close"].iloc[-1]) if not df.empty else math.nan
            status, reason_codes = _compute_data_status(
                int(features.get("history_days", 0)), staging_cfg["history_min_days"]
            )
            logger.info(
                "Stage 3: %s lookback=%s rows=%s history_days=%s min_history=%s status=%s",
                symbol,
                staging_cfg["stage3_deep_days"],
                len(df),
                int(features.get("history_days", 0)),
                staging_cfg["history_min_days"],
                status,
            )
            theme_tags, theme_confidence, theme_unknown = tag_themes(symbol, name, themes_cfg)
            scenario = scenario_scores(theme_tags)
            risk_level, red, amber = assess_risk(
                features,
                adjusted_mode=adjusted_mode,
                risk_cfg=gates_cfg.get("risk_flags", {}),
            )
            notes = "Eligible for monitoring" if status == "OK" else "Data issue"
            confidence_score = _confidence_score(features, theme_confidence)
            silver_payload = _maybe_attach_silver(theme_tags, symbol, silver_macro)
            last_data_date = None
            if "Date" in df.columns and not df.empty:
                last_data_date = pd.to_datetime(df["Date"].iloc[-1], errors="coerce")
            stage3_rows.append(
                {
                    **run_meta,
                    "symbol": symbol,
                    "name": name,
                    "data_status": status,
                    "data_reason_codes": ";".join(reason_codes),
                    "data_freshness_days": _data_freshness_days(df),
                    "adjusted_mode": adjusted_mode,
                    "theme_tags": ";".join(theme_tags),
                    "theme_confidence": theme_confidence,
                    "theme_unknown": theme_unknown,
                    "risk_level": risk_level,
                    "risk_red_codes": ";".join(red),
                    "risk_amber_codes": ";".join(amber),
                    "eligible": True,
                    "gate_fail_codes": "",
                    "notes": notes,
                    "confidence_score": confidence_score,
                    "as_of_date": last_data_date.strftime("%Y-%m-%d")
                    if last_data_date is not None
                    else None,
                    **features,
                    **scenario,
                    **silver_payload,
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
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
    }

    stage1_df = pd.DataFrame(stage1_survivors)
    stage2_df = pd.DataFrame(stage2_survivors)
    stage3_df = pd.DataFrame(stage3_rows)
    return stage1_df, stage2_df, stage3_df, summary


def _confidence_score(features: dict[str, float], theme_confidence: float) -> float:
    history_days = features.get("history_days") or 0.0
    missing_rate = features.get("missing_day_rate") or 0.0
    volume_available = features.get("volume_available", 1.0)
    history_score = min(history_days / 252.0, 1.0)
    completeness = max(1.0 - missing_rate, 0.0)
    volume_score = 1.0 if volume_available else 0.7
    score = 0.4 * history_score + 0.3 * completeness + 0.2 * volume_score + 0.1 * theme_confidence
    return float(max(min(score, 1.0), 0.0))


def _maybe_attach_silver(
    theme_tags: list[str], symbol: str, silver_macro: dict[str, float] | None
) -> dict[str, float]:
    if not silver_macro:
        return {}
    silver_symbols = {"SLV", "SIL", "SILJ", "AG", "PAAS", "HL"}
    if "metals" in theme_tags or symbol.upper() in silver_symbols:
        return silver_macro
    return {}
