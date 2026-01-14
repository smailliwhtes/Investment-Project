from typing import Dict, List, Tuple


def tag_themes(symbol: str, name: str, theme_config: Dict[str, Dict[str, List[str]]]) -> Tuple[List[str], float]:
    tags: List[str] = []
    symbol_upper = symbol.upper()
    name_lower = (name or "").lower()
    for theme, rules in theme_config.items():
        symbols = [s.upper() for s in rules.get("symbols", [])]
        keywords = [k.lower() for k in rules.get("keywords", [])]
        if symbol_upper in symbols:
            tags.append(theme)
            continue
        if any(keyword in name_lower for keyword in keywords):
            tags.append(theme)

    purity = min(len(tags) / max(len(theme_config), 1), 1.0)
    return tags, purity
