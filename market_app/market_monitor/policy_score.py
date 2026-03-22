from __future__ import annotations

from math import log10

import numpy as np
import pandas as pd


def _clip01(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _average_dollar_volume(provider, symbol: str, as_of_date: str) -> float:
    if not hasattr(provider, "load_symbol_data"):
        return 0.0
    try:
        frame, _ = provider.load_symbol_data(symbol)
    except Exception:
        return 0.0
    if frame.empty or "Date" not in frame.columns or "Close" not in frame.columns:
        return 0.0
    data = frame.copy()
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
    data = data[data["Date"] <= pd.to_datetime(as_of_date, errors="coerce")]
    if data.empty:
        return 0.0
    tail = data.tail(20)
    close = pd.to_numeric(tail["Close"], errors="coerce")
    volume = pd.to_numeric(tail.get("Volume"), errors="coerce").fillna(0.0)
    return float((close * volume).mean(skipna=True) or 0.0)


def rank_policy_impacts(
    simulation_summary: pd.DataFrame,
    event_study: pd.DataFrame,
    *,
    provider,
    as_of_date: str,
    average_dollar_volume_floor: float,
    analog_count: int,
    top_n_analogs: int,
) -> pd.DataFrame:
    if simulation_summary.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for _, row in simulation_summary.iterrows():
        symbol = str(row.get("symbol", "")).upper().strip()
        studies = event_study[event_study["symbol"].astype(str).str.upper() == symbol]
        raw_effective_sample_count = row.get("effective_sample_count_20d")
        if raw_effective_sample_count is None or pd.isna(raw_effective_sample_count):
            effective_sample_count = int(_safe_float(row.get("analog_count")))
        else:
            effective_sample_count = int(_safe_float(raw_effective_sample_count))
        simulation_basis = str(row.get("simulation_basis", "empirical"))

        sensitivity = 0.0
        fragility = 0.0
        if not studies.empty:
            mean_abs_car = studies["cumulative_abnormal_return"].abs().mean()
            vol_proxy = studies["window_volatility"].replace(0, np.nan).dropna().mean()
            sensitivity = _clip01(float(mean_abs_car / max(vol_proxy or 0.02, 0.02)))

            short = studies[studies["horizon_days"] == 1]["cumulative_abnormal_return"].mean()
            medium = studies[studies["horizon_days"] == 20]["cumulative_abnormal_return"].mean()
            base = max(abs(short) or 0.01, 0.01)
            fragility = _clip01(abs((medium or 0.0) - (short or 0.0)) / base)

        impact_magnitude = _clip01(abs(_safe_float(row.get("median_return_20d"))) / 0.15)
        tail_risk = _clip01(abs(min(_safe_float(row.get("q10_return_20d")), 0.0)) / 0.25)

        spread_20d = _safe_float(row.get("q90_return_20d")) - _safe_float(row.get("q10_return_20d"))
        if simulation_basis != "empirical" or effective_sample_count <= 0:
            confidence = 0.0
        else:
            confidence = _clip01(
                min(effective_sample_count / max(top_n_analogs, 1), 1.0)
                * (1.0 - min(spread_20d / 0.5, 1.0))
            )

        adv20 = _average_dollar_volume(provider, symbol, as_of_date)
        if adv20 <= 0 or average_dollar_volume_floor <= 0:
            tradability = 0.0
        else:
            tradability = _clip01(log10(1 + adv20 / average_dollar_volume_floor))

        impact_score = _clip01(
            0.26 * sensitivity
            + 0.14 * fragility
            + 0.18 * tradability
            + 0.18 * confidence
            + 0.16 * impact_magnitude
            + 0.08 * tail_risk
        )

        rows.append(
            {
                **row.to_dict(),
                "policy_sensitivity_score": round(sensitivity, 6),
                "fragility_score": round(fragility, 6),
                "tradability_score": round(tradability, 6),
                "tail_risk_score": round(tail_risk, 6),
                "confidence_score": round(confidence, 6),
                "effective_sample_count": effective_sample_count,
                "simulation_basis": simulation_basis,
                "adv20_dollar": round(adv20, 2),
                "scenario_impact_score": round(impact_score, 6),
            }
        )

    ranked = pd.DataFrame(rows).sort_values(
        ["scenario_impact_score", "median_return_20d", "symbol"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    ranked["rank"] = range(1, len(ranked) + 1)
    return ranked
