def assess_risk(features: dict[str, float], adjusted_mode: str) -> tuple[str, list[str], list[str]]:
    red: list[str] = []
    amber: list[str] = []

    if adjusted_mode == "UNADJUSTED":
        amber.append("UNADJUSTED_HISTORY")

    vol60 = features.get("vol60_ann")
    if vol60 is not None and not _is_nan(vol60) and vol60 > 1.2:
        red.append("HIGH_VOL")
    elif vol60 is not None and not _is_nan(vol60) and vol60 > 0.8:
        amber.append("ELEVATED_VOL")

    max_dd = features.get("max_drawdown_6m")
    if max_dd is not None and not _is_nan(max_dd) and max_dd < -0.6:
        red.append("SEVERE_DRAWDOWN")
    elif max_dd is not None and not _is_nan(max_dd) and max_dd < -0.4:
        amber.append("DEEP_DRAWDOWN")

    adv20 = features.get("adv20_dollar")
    if adv20 is not None and not _is_nan(adv20) and adv20 < 250_000:
        amber.append("LOW_LIQUIDITY")

    missing_rate = features.get("missing_day_rate")
    if missing_rate is not None and not _is_nan(missing_rate) and missing_rate > 0.2:
        amber.append("MISSING_DAYS")

    if features.get("stale_price_flag", 0.0) == 1.0:
        amber.append("STALE_PRICE")

    if features.get("corp_action_suspect", 0.0) == 1.0:
        amber.append("CORP_ACTION_SUSPECT")

    if features.get("volume_available", 1.0) == 0.0:
        amber.append("VOLUME_MISSING")

    risk_level = "GREEN"
    if red:
        risk_level = "RED"
    elif amber:
        risk_level = "AMBER"
    return risk_level, red, amber


def _is_nan(value: float | None) -> bool:
    return value is None or value != value
