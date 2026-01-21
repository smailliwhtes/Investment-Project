def assess_risk(
    features: dict[str, float],
    adjusted_mode: str,
    risk_cfg: dict[str, float] | None = None,
) -> tuple[str, list[str], list[str]]:
    red: list[str] = []
    amber: list[str] = []
    cfg = risk_cfg or {}

    if adjusted_mode == "UNADJUSTED":
        amber.append("UNADJUSTED_HISTORY")

    vol60 = features.get("vol60_ann")
    extreme_vol = cfg.get("extreme_vol_annual", 1.2)
    if vol60 is not None and not _is_nan(vol60) and vol60 > extreme_vol:
        red.append("EXTREME_VOL_LIKE")
    elif vol60 is not None and not _is_nan(vol60) and vol60 > extreme_vol * 0.7:
        amber.append("ELEVATED_VOL")

    max_dd = features.get("max_drawdown_6m")
    if max_dd is not None and not _is_nan(max_dd) and max_dd < -0.6:
        red.append("SEVERE_DRAWDOWN")
    elif max_dd is not None and not _is_nan(max_dd) and max_dd < -0.4:
        amber.append("DEEP_DRAWDOWN")

    adv20 = features.get("adv20_dollar")
    illiquid = cfg.get("illiquid_adv20_dollar")
    if illiquid is not None and adv20 is not None and not _is_nan(adv20) and adv20 < illiquid:
        amber.append("ILLIQUID_LIKE")

    last_price = features.get("last_price")
    penny_like = cfg.get("penny_like")
    if penny_like is not None and last_price is not None and not _is_nan(last_price):
        if last_price <= penny_like:
            amber.append("PENNY_LIKE")

    history_days = features.get("history_days")
    short_history = cfg.get("short_history_days")
    if short_history is not None and history_days is not None and not _is_nan(history_days):
        if history_days < short_history:
            amber.append("SHORT_HISTORY_LIKE")

    gap_atr = features.get("gap_atr")
    gap_threshold = cfg.get("gap_atr")
    if gap_threshold is not None and gap_atr is not None and not _is_nan(gap_atr):
        if abs(gap_atr) >= gap_threshold:
            amber.append("GAP_RISK_LIKE")

    missing_rate = features.get("missing_day_rate")
    missing_threshold = cfg.get("missing_day_rate")
    if missing_threshold is not None and missing_rate is not None and not _is_nan(missing_rate):
        if missing_rate > missing_threshold:
            amber.append("MISSING_DAYS")

    zero_volume = features.get("zero_volume_frac")
    zero_threshold = cfg.get("zero_volume_frac")
    if zero_threshold is not None and zero_volume is not None and not _is_nan(zero_volume):
        if zero_volume > zero_threshold:
            amber.append("ZERO_VOLUME_LIKE")

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
