from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class EligibilityResult:
    eligible: pd.DataFrame
    gate_summary: dict[str, int]


def apply_gates(features: pd.DataFrame, config: dict[str, Any]) -> EligibilityResult:
    gates = config.get("gates", {})
    min_history = int(gates.get("min_history_days", 252))
    min_adv = float(gates.get("min_adv20_usd", 1_000_000))
    price_floor = float(gates.get("price_floor", 1.0))
    zero_vol_max = float(gates.get("zero_volume_max_frac", 0.2))

    gate_reasons = []
    eligible = []
    for _, row in features.iterrows():
        reasons = []
        history_days = float(row.get("history_days", 0))
        if history_days < min_history:
            reasons.append("insufficient_history")
        price = float(row.get("close_to_sma20", float("nan"))) * float(
            row.get("sma20", float("nan"))
        )
        if np.isfinite(price) and price < price_floor:
            reasons.append("price_below_min")
        adv = row.get("adv20_usd")
        if np.isfinite(adv) and adv < min_adv:
            reasons.append("adv20_below_min")
        zero_fraction = pd.to_numeric(row.get("zero_volume_fraction_60d"), errors="coerce")
        if np.isfinite(zero_fraction) and zero_fraction > zero_vol_max:
            reasons.append("zero_volume_high")
        if row.get("missing_data"):
            reasons.append("missing_data")
        eligible.append(len(reasons) == 0)
        gate_reasons.append(";".join(reasons))

    eligible_df = pd.DataFrame(
        {
            "symbol": features["symbol"],
            "eligible": eligible,
            "gate_fail_reasons": gate_reasons,
        }
    )
    summary = {
        "eligible": int(eligible_df["eligible"].sum()),
        "ineligible": int((~eligible_df["eligible"]).sum()),
    }
    return EligibilityResult(eligible=eligible_df, gate_summary=summary)


def score_symbols(
    features: pd.DataFrame,
    classified: pd.DataFrame,
    config: dict[str, Any],
) -> pd.DataFrame:
    weights = config.get("scoring", {}).get("weights", {})
    merged = features.merge(
        classified[["symbol", "themes", "theme_confidence", "theme_uncertain"]],
        on="symbol",
        how="left",
    )
    trend = _zscore(
        merged[["return_3m", "return_6m", "return_12m", "close_to_sma200"]]
        .mean(axis=1)
        .fillna(0.0)
    )
    liquidity = _zscore(
        np.log1p(merged["adv20_usd"].fillna(0.0).clip(lower=0.0))
    )
    risk_penalty = _zscore(merged["volatility_60d"].fillna(0.0)) * -1.0
    drawdown_penalty = _zscore(merged["max_drawdown_6m"].fillna(0.0)) * -1.0
    theme_purity = merged["theme_confidence"].fillna(0.0)
    volume_missing = merged["volume_missing"].fillna(False)

    total_score = (
        weights.get("trend", 0.35) * trend
        + weights.get("liquidity", 0.2) * liquidity
        + weights.get("risk_penalty", 0.2) * risk_penalty
        + weights.get("drawdown_penalty", 0.15) * drawdown_penalty
        + weights.get("theme_purity", 0.1) * theme_purity
    )
    total_score = total_score - weights.get("volume_missing_penalty", 0.05) * volume_missing.astype(
        float
    )
    monitor_score = _bucket_score(total_score, merged["symbol"])
    risk_flags, risk_level = build_risk_flags(merged, config)

    scored = pd.DataFrame(
        {
            "symbol": merged["symbol"],
            "monitor_score": monitor_score,
            "total_score": total_score,
            "risk_flags": risk_flags,
            "risk_level": risk_level,
            "themes": merged["themes"].fillna(""),
            "theme_confidence": merged["theme_confidence"].fillna(0.0),
            "forecast_return_21d": merged["return_1m"].fillna(0.0),
            "last_date": merged["last_date"].fillna(""),
            "lag_days": pd.array(merged["lag_days"], dtype="Int64"),
            "lag_bin": merged.get("lag_bin", pd.Series(["unknown"] * len(merged))).fillna("unknown"),
        }
    )
    return scored.sort_values(["monitor_score", "symbol"], ascending=[False, True]).reset_index(
        drop=True
    )


def build_risk_flags(features: pd.DataFrame, config: dict[str, Any]) -> tuple[pd.Series, pd.Series]:
    thresholds = config.get("risk_thresholds", {})
    flags_list = []
    levels = []
    for _, row in features.iterrows():
        flags = []
        adv = row.get("adv20_usd")
        if np.isfinite(adv) and adv < config["gates"]["min_adv20_usd"]:
            flags.append("adv20_below_min")
        if row.get("history_days", 0) < config["gates"]["min_history_days"]:
            flags.append("insufficient_history")
        price = float(row.get("close_to_sma20", float("nan"))) * float(
            row.get("sma20", float("nan"))
        )
        if np.isfinite(price) and price < config["gates"]["price_floor"]:
            flags.append("price_below_min")
        vol = row.get("volatility_60d")
        if np.isfinite(vol) and vol > thresholds.get(
            "extreme_volatility", 0.6
        ):
            flags.append("extreme_volatility")
        max_dd = row.get("max_drawdown_6m")
        if np.isfinite(max_dd) and max_dd < thresholds.get(
            "deep_drawdown", -0.4
        ):
            flags.append("deep_drawdown")
        worst_5d = row.get("worst_5d_return_6m")
        if np.isfinite(worst_5d) and worst_5d < thresholds.get(
            "tail_risk", -0.2
        ):
            flags.append("tail_risk")
        if row.get("stale_data"):
            flags.append("stale_data")
        if row.get("theme_uncertain"):
            flags.append("theme_uncertain")
        if row.get("volume_missing"):
            flags.append("missing_volume")

        level = _risk_level(flags)
        flags_list.append(";".join(flags))
        levels.append(level)
    return pd.Series(flags_list), pd.Series(levels)


def _risk_level(flags: list[str]) -> str:
    if any(flag in {"extreme_volatility", "deep_drawdown", "tail_risk"} for flag in flags):
        return "RED"
    if flags:
        return "AMBER"
    return "GREEN"


def _zscore(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std(ddof=0)
    if std == 0 or np.isnan(std):
        return series * 0.0
    z = (series - mean) / std
    return z.clip(-3, 3)


def _bucket_score(scores: pd.Series, symbols: pd.Series) -> pd.Series:
    df = pd.DataFrame({"score": scores, "symbol": symbols})
    if df.empty:
        return pd.Series(dtype=int)
    ordered = df.sort_values(["score", "symbol"], ascending=[True, True])
    ranks = np.linspace(1, 10, len(ordered)).round().astype(int)
    ordered = ordered.assign(bucket=ranks)
    bucket_map = ordered["bucket"]
    bucket_map.index = ordered.index
    return bucket_map.reindex(df.index).astype(int)
