import numpy as np
import pandas as pd


def score_frame(df: pd.DataFrame, weights: dict[str, float]) -> pd.DataFrame:
    score_components = []
    for _idx, row in df.iterrows():
        trend = _safe_nanmean(
            [
                row.get("trend_quality_6m"),
                row.get("trend_quality_12m"),
                row.get("sma50_ratio"),
            ]
        )
        momentum = _safe_nanmean([row.get("mom_12_1"), row.get("mom_6_1"), row.get("ret_6m")])
        liquidity = np.log10(max(row.get("adv20_dollar", 1.0), 1.0))
        if row.get("volume_available", 1.0) == 0.0:
            liquidity = 0.0
        quality = _safe_nanmean(
            [
                row.get("trend_r2_6m"),
                row.get("pct_days_above_sma200"),
                -row.get("ulcer_index_60d") if row.get("ulcer_index_60d") is not None else np.nan,
                -(row.get("missing_day_rate") or 0.0),
                -(row.get("stale_price_flag") or 0.0),
                -(row.get("corp_action_suspect") or 0.0),
            ]
        )
        vol_penalty = -abs(row.get("vol60_ann", 0.0))
        dd_penalty = row.get("max_drawdown_6m", 0.0)
        tail_penalty = _safe_nanmean([row.get("worst_5d_return"), row.get("cvar_60d")])
        attention = _safe_nanmean(
            [
                row.get("gap_atr"),
                row.get("range_expansion"),
                row.get("big_day_freq"),
                row.get("close_to_high"),
            ]
        )
        theme_bonus = 0.1 if row.get("theme_tags") else 0.0
        volume_missing_penalty = -1.0 if row.get("volume_available", 1.0) == 0.0 else 0.0

        trend = _nan_to_num(trend)
        momentum = _nan_to_num(momentum)
        liquidity = _nan_to_num(liquidity)
        quality = _nan_to_num(quality)
        vol_penalty = _nan_to_num(vol_penalty)
        dd_penalty = _nan_to_num(dd_penalty)
        tail_penalty = _nan_to_num(tail_penalty)
        attention = _nan_to_num(attention)
        theme_bonus = _nan_to_num(theme_bonus)
        volume_missing_penalty = _nan_to_num(volume_missing_penalty)

        raw_score = (
            weights.get("trend", 0.0) * trend
            + weights.get("momentum", 0.0) * momentum
            + weights.get("liquidity", 0.0) * liquidity
            + weights.get("quality", 0.0) * quality
            + weights.get("vol_penalty", 0.0) * vol_penalty
            + weights.get("dd_penalty", 0.0) * dd_penalty
            + weights.get("tail_penalty", 0.0) * tail_penalty
            + weights.get("attention", 0.0) * attention
            + weights.get("theme_bonus", 0.0) * theme_bonus
            + weights.get("volume_missing_penalty", 0.0) * volume_missing_penalty
        )
        score_components.append(
            {
                "trend": trend,
                "momentum": momentum,
                "liquidity": liquidity,
                "quality": quality,
                "vol_penalty": vol_penalty,
                "dd_penalty": dd_penalty,
                "tail_penalty": tail_penalty,
                "attention": attention,
                "theme_bonus": theme_bonus,
                "volume_missing_penalty": volume_missing_penalty,
                "raw_score": raw_score,
            }
        )

    components_df = pd.DataFrame(score_components)
    df = pd.concat([df.reset_index(drop=True), components_df], axis=1)
    if len(df) > 1:
        df["decile"] = pd.qcut(df["raw_score"], 10, labels=False, duplicates="drop")
    else:
        df["decile"] = 0
    df["monitor_score_1_10"] = (df["decile"].fillna(0).astype(int) + 1).clip(1, 10)
    df["monitor_priority_1_10"] = df["monitor_score_1_10"]
    df["score_components"] = df[
        [
            "trend",
            "momentum",
            "liquidity",
            "quality",
            "vol_penalty",
            "dd_penalty",
            "tail_penalty",
            "attention",
            "theme_bonus",
            "volume_missing_penalty",
        ]
    ].to_dict(orient="records")
    return df


def _nan_to_num(value: float) -> float:
    if value is None or np.isnan(value):
        return 0.0
    return float(value)


def _safe_nanmean(values: list[float | None]) -> float:
    clean = [v for v in values if v is not None and not np.isnan(v)]
    if not clean:
        return np.nan
    return float(np.mean(clean))
