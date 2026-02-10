from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ThemeResult:
    symbol: str
    themes: str
    theme_confidence: float
    theme_evidence: str
    theme_uncertain: bool


def classify_theme(symbol: str, name: str, config: dict[str, Any]) -> ThemeResult:
    text = f"{symbol} {name}".lower()
    themes_cfg = config.get("themes", {})
    matched = {}
    for theme, rules in themes_cfg.items():
        keywords = [kw.lower() for kw in rules.get("keywords", [])]
        tickers = [tk.lower() for tk in rules.get("tickers", [])]
        hits = [kw for kw in keywords if kw in text]
        if symbol.lower() in tickers:
            hits.append(symbol.lower())
        if hits:
            matched[theme] = sorted(set(hits))

    themes_list = sorted(matched.keys())
    confidence = _confidence_from_hits(matched)
    evidence = json.dumps(matched, sort_keys=True)
    theme_uncertain = confidence < 0.4
    return ThemeResult(
        symbol=symbol,
        themes=";".join(themes_list),
        theme_confidence=confidence,
        theme_evidence=evidence,
        theme_uncertain=theme_uncertain,
    )


def _confidence_from_hits(matched: dict[str, list[str]]) -> float:
    if not matched:
        return 0.0
    total_hits = sum(len(items) for items in matched.values())
    if total_hits >= 3:
        return 1.0
    if total_hits == 2:
        return 0.7
    return 0.4
