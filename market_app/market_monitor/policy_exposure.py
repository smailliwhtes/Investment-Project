from __future__ import annotations

from typing import Any

import pandas as pd

from market_monitor.policy_event_schema import PolicyScenarioTemplate

DEFAULT_EVENT_THEME_MAP: dict[str, tuple[str, ...]] = {
    "tariff": ("metals", "industrials", "supply_chain"),
    "sanction": ("energy", "defense", "commodities"),
    "executive_order": ("regulation", "industrials", "labor"),
    "chips": ("tech", "semiconductors", "ai"),
    "energy_policy": ("energy", "utilities", "grid"),
    "trade_deal": ("metals", "trade", "critical_minerals"),
    "subsidy": ("industrials", "clean_energy", "infrastructure"),
}


def resolve_policy_exposure(
    template: PolicyScenarioTemplate,
    holdings: pd.DataFrame,
    *,
    available_symbols: set[str] | None = None,
) -> dict[str, Any]:
    desired_themes = set(template.sectors)
    desired_themes.update(DEFAULT_EVENT_THEME_MAP.get(template.event_type, ()))

    ordered_symbols: list[str] = []
    seen: set[str] = set()

    for symbol in template.tickers:
        if available_symbols is not None and symbol not in available_symbols:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        ordered_symbols.append(symbol)

    matched_rows = pd.DataFrame(columns=holdings.columns)
    if not holdings.empty:
        candidates = holdings.copy()
        theme_match = candidates["theme"].isin(desired_themes) | candidates["sector"].isin(desired_themes)
        etf_match = candidates["etf_symbol"].isin(set(template.linked_etfs) | set(template.tickers))
        matched_rows = candidates[theme_match | etf_match].copy()
        matched_rows = matched_rows.sort_values(
            ["weight", "etf_symbol", "constituent_symbol"],
            ascending=[False, True, True],
        )
        for symbol in matched_rows["constituent_symbol"].astype(str):
            ticker = symbol.strip().upper()
            if not ticker:
                continue
            if available_symbols is not None and ticker not in available_symbols:
                continue
            if ticker in seen:
                continue
            seen.add(ticker)
            ordered_symbols.append(ticker)

    return {
        "symbols": ordered_symbols,
        "themes": sorted(desired_themes),
        "matched_holdings": matched_rows.reset_index(drop=True),
    }
