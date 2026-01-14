from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def score_frame(df: pd.DataFrame, weights: Dict[str, float]) -> pd.DataFrame:
    score_components = []
    for idx, row in df.iterrows():
        trend = row.get("sma50_ratio", 0.0)
        momentum = row.get("ret_6m", 0.0)
        liquidity = np.log10(max(row.get("adv20_dollar", 1.0), 1.0))
        vol_penalty = -abs(row.get("vol60_ann", 0.0))
        dd_penalty = row.get("max_drawdown_6m", 0.0)
        tail_penalty = row.get("worst_5d_return", 0.0)
        theme_bonus = 0.1 if row.get("theme_tags") else 0.0

        raw_score = (
            weights.get("trend", 0.0) * trend
            + weights.get("momentum", 0.0) * momentum
            + weights.get("liquidity", 0.0) * liquidity
            + weights.get("vol_penalty", 0.0) * vol_penalty
            + weights.get("dd_penalty", 0.0) * dd_penalty
            + weights.get("tail_penalty", 0.0) * tail_penalty
            + weights.get("theme_bonus", 0.0) * theme_bonus
        )
        score_components.append({
            "trend": trend,
            "momentum": momentum,
            "liquidity": liquidity,
            "vol_penalty": vol_penalty,
            "dd_penalty": dd_penalty,
            "tail_penalty": tail_penalty,
            "theme_bonus": theme_bonus,
            "raw_score": raw_score,
        })

    components_df = pd.DataFrame(score_components)
    df = pd.concat([df.reset_index(drop=True), components_df], axis=1)
    if len(df) > 1:
        df["decile"] = pd.qcut(df["raw_score"], 10, labels=False, duplicates="drop")
    else:
        df["decile"] = 0
    df["monitor_priority_1_10"] = (df["decile"].fillna(0).astype(int) + 1).clip(1, 10)
    df["score_components"] = df[["trend", "momentum", "liquidity", "vol_penalty", "dd_penalty", "tail_penalty", "theme_bonus"]].to_dict(orient="records")
    return df
