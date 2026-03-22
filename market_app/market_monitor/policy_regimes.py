from __future__ import annotations

from typing import Any

import pandas as pd

from market_monitor.fred import latest_value_at_or_before


def _series_pct_change(frame: pd.DataFrame, series_id: str, as_of_date: str, periods: int) -> float | None:
    if frame.empty or series_id not in frame.columns:
        return None
    ts = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(ts):
        return None
    subset = frame[pd.to_datetime(frame["Date"], errors="coerce") <= ts][["Date", series_id]].copy()
    subset[series_id] = pd.to_numeric(subset[series_id], errors="coerce")
    subset = subset.dropna().sort_values("Date")
    if len(subset) <= periods:
        return None
    latest = float(subset[series_id].iloc[-1])
    baseline = float(subset[series_id].iloc[-(periods + 1)])
    if baseline == 0:
        return None
    return latest / baseline - 1.0


def _series_delta(frame: pd.DataFrame, series_id: str, as_of_date: str, periods: int) -> float | None:
    if frame.empty or series_id not in frame.columns:
        return None
    ts = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(ts):
        return None
    subset = frame[pd.to_datetime(frame["Date"], errors="coerce") <= ts][["Date", series_id]].copy()
    subset[series_id] = pd.to_numeric(subset[series_id], errors="coerce")
    subset = subset.dropna().sort_values("Date")
    if len(subset) <= periods:
        return None
    return float(subset[series_id].iloc[-1] - subset[series_id].iloc[-(periods + 1)])


def classify_policy_regime(
    as_of_date: str,
    macro_frame: pd.DataFrame,
    gdelt_frame: pd.DataFrame | None,
) -> dict[str, Any]:
    fed_funds = latest_value_at_or_before(macro_frame, "FEDFUNDS", as_of_date)
    unemployment = latest_value_at_or_before(macro_frame, "UNRATE", as_of_date)
    cpi_yoy = _series_pct_change(macro_frame, "CPIAUCSL", as_of_date, periods=12)
    industrial_change = _series_pct_change(macro_frame, "INDPRO", as_of_date, periods=6)
    fed_delta_6m = _series_delta(macro_frame, "FEDFUNDS", as_of_date, periods=6)

    if cpi_yoy is None:
        inflation_regime = "unknown"
    elif cpi_yoy >= 0.03:
        inflation_regime = "inflationary"
    elif cpi_yoy <= 0.015:
        inflation_regime = "disinflationary"
    else:
        inflation_regime = "stable_prices"

    if unemployment is not None and unemployment >= 5.25:
        macro_regime = "contraction"
    elif industrial_change is not None and industrial_change < -0.01:
        macro_regime = "contraction"
    else:
        macro_regime = "expansion"

    if fed_funds is not None and fed_funds >= 4.0:
        policy_stance = "tightening"
    elif fed_delta_6m is not None and fed_delta_6m >= 0.5:
        policy_stance = "tightening"
    elif fed_funds is not None and fed_funds <= 2.0:
        policy_stance = "supportive"
    else:
        policy_stance = "neutral"

    conflict_level = None
    energy_stress = None
    conflict_regime = "normal"
    if gdelt_frame is not None and not gdelt_frame.empty and "Date" in gdelt_frame.columns:
        gdelt = gdelt_frame.copy()
        gdelt["Date"] = pd.to_datetime(gdelt["Date"], errors="coerce")
        gdelt = gdelt.dropna(subset=["Date"]).sort_values("Date")
        as_of_ts = pd.to_datetime(as_of_date, errors="coerce")
        eligible = gdelt[gdelt["Date"] <= as_of_ts]
        if not eligible.empty:
            if "conflict_event_count_total" in eligible.columns:
                conflict_series = pd.to_numeric(
                    eligible["conflict_event_count_total"], errors="coerce"
                ).dropna()
                if not conflict_series.empty:
                    conflict_level = float(conflict_series.iloc[-1])
                    baseline = float(conflict_series.tail(30).median())
                    spread = float(conflict_series.tail(30).std(ddof=0) or 0.0)
                    if conflict_level > baseline + max(spread, 1.0):
                        conflict_regime = "elevated"
            if "energy_stress_score" in eligible.columns:
                values = pd.to_numeric(eligible["energy_stress_score"], errors="coerce").dropna()
                if not values.empty:
                    energy_stress = float(values.iloc[-1])

    tags = [macro_regime, inflation_regime, policy_stance, conflict_regime]
    if energy_stress is not None and energy_stress >= 0.6:
        tags.append("energy_stress")

    return {
        "as_of_date": as_of_date,
        "macro_regime": macro_regime,
        "inflation_regime": inflation_regime,
        "policy_stance": policy_stance,
        "conflict_regime": conflict_regime,
        "tags": sorted(dict.fromkeys(tag for tag in tags if tag and tag != "unknown")),
        "snapshot": {
            "fed_funds": fed_funds,
            "unemployment": unemployment,
            "cpi_yoy": cpi_yoy,
            "industrial_change_6m": industrial_change,
            "fed_funds_delta_6m": fed_delta_6m,
            "conflict_event_count_total": conflict_level,
            "energy_stress_score": energy_stress,
        },
    }
