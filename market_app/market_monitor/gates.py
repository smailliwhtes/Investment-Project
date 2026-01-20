def apply_gates(
    features: dict[str, float],
    price_max: float,
    min_adv20_dollar: float,
    max_zero_volume_frac: float,
    history_min_days: int,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    last_price = features.get("last_price")
    if last_price is not None and last_price > price_max:
        reasons.append("PRICE_MAX")

    volume_available = bool(features.get("volume_available", True))

    adv20 = features.get("adv20_dollar")
    if volume_available:
        if adv20 is None or adv20 < min_adv20_dollar:
            reasons.append("MIN_ADV20")

    zero_frac = features.get("zero_volume_frac")
    if volume_available:
        if zero_frac is not None and zero_frac > max_zero_volume_frac:
            reasons.append("MAX_ZERO_VOLUME")

    history_days = features.get("history_days")
    if history_days is None or history_days < history_min_days:
        reasons.append("INSUFFICIENT_HISTORY")

    return len(reasons) == 0, reasons
