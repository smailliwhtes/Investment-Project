from __future__ import annotations

import pandas as pd

from market_monitor.policy_event_schema import PolicyEvent
from market_monitor.policy_regimes import classify_policy_regime


def build_policy_event_frame(
    events: list[PolicyEvent],
    *,
    macro_frame: pd.DataFrame,
    gdelt_frame: pd.DataFrame | None,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for event in events:
        regime = classify_policy_regime(event.event_date, macro_frame, gdelt_frame)
        rows.append(
            {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "source": event.source,
                "agency": event.agency,
                "event_date": event.event_date,
                "title": event.title,
                "summary": event.summary,
                "sectors": "|".join(event.sectors),
                "tickers": "|".join(event.tickers),
                "countries": "|".join(event.countries),
                "codes": "|".join(event.codes),
                "severity": event.severity,
                "sector_count": len(event.sectors),
                "ticker_count": len(event.tickers),
                "country_count": len(event.countries),
                "macro_regime": regime["macro_regime"],
                "inflation_regime": regime["inflation_regime"],
                "policy_stance": regime["policy_stance"],
                "conflict_regime": regime["conflict_regime"],
                "regime_tags": "|".join(regime["tags"]),
            }
        )
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(
            columns=[
                "event_id",
                "event_type",
                "source",
                "agency",
                "event_date",
                "title",
                "summary",
                "sectors",
                "tickers",
                "countries",
                "codes",
                "severity",
                "sector_count",
                "ticker_count",
                "country_count",
                "macro_regime",
                "inflation_regime",
                "policy_stance",
                "conflict_regime",
                "regime_tags",
            ]
        )
    return frame.sort_values(["event_date", "event_id"]).reset_index(drop=True)
