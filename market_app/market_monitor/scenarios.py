from typing import Dict, List


def scenario_scores(theme_tags: List[str]) -> Dict[str, float]:
    return {
        "scenario_defense": 1.0 if "defense" in theme_tags else 0.0,
        "scenario_tech": 1.0 if "tech" in theme_tags else 0.0,
        "scenario_metals": 1.0 if "metals" in theme_tags else 0.0,
    }
