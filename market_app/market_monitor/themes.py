def tag_themes(
    symbol: str, name: str, theme_config: dict[str, dict[str, list[str]]]
) -> tuple[list[str], float, str]:
    tags: list[str] = []
    symbol_upper = symbol.upper()
    name_lower = (name or "").lower()
    evidence = 0
    for theme, rules in theme_config.items():
        symbols = [s.upper() for s in rules.get("symbols", [])]
        keywords = [k.lower() for k in rules.get("keywords", [])]
        hit = False
        if symbol_upper in symbols:
            hit = True
        if any(keyword in name_lower for keyword in keywords):
            hit = True
        if hit:
            tags.append(theme)
            evidence += 1

    tags = sorted(set(tags))
    if tags:
        confidence = min(0.6 + 0.2 * evidence, 1.0)
        unknown = ""
    else:
        confidence = 0.1
        unknown = "UNKNOWN_THEME"
    return tags, confidence, unknown
