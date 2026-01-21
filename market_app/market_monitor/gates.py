def apply_gates(
    features: dict[str, float],
    price_min: float | None,
    price_max: float | None,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    last_price = features.get("last_price")
    if price_min is not None and last_price is not None and last_price < price_min:
        reasons.append("PRICE_MIN")
    if price_max is not None and last_price is not None and last_price > price_max:
        reasons.append("PRICE_MAX")

    return len(reasons) == 0, reasons
