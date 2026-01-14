from typing import Dict, List, Tuple


def assess_risk(features: Dict[str, float], adjusted_mode: str) -> Tuple[List[str], List[str]]:
    red: List[str] = []
    amber: List[str] = []

    if adjusted_mode == "UNADJUSTED":
        amber.append("UNADJUSTED_HISTORY")

    vol60 = features.get("vol60_ann")
    if vol60 is not None and vol60 > 1.2:
        red.append("HIGH_VOL")
    elif vol60 is not None and vol60 > 0.8:
        amber.append("ELEVATED_VOL")

    max_dd = features.get("max_drawdown_6m")
    if max_dd is not None and max_dd < -0.6:
        red.append("SEVERE_DRAWDOWN")
    elif max_dd is not None and max_dd < -0.4:
        amber.append("DEEP_DRAWDOWN")

    adv20 = features.get("adv20_dollar")
    if adv20 is not None and adv20 < 250_000:
        amber.append("LOW_LIQUIDITY")

    return red, amber
