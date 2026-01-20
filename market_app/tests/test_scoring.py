import pandas as pd

from market_monitor.scoring import score_frame


def test_scoring_deciles():
    df = pd.DataFrame(
        {
            "trend_quality_6m": [0.1, 0.2, 0.05, 0.3, 0.15],
            "trend_quality_12m": [0.08, 0.18, 0.04, 0.25, 0.12],
            "sma50_ratio": [0.1, 0.2, 0.3, 0.4, 0.5],
            "mom_12_1": [0.05, 0.06, 0.02, 0.01, 0.07],
            "mom_6_1": [0.03, 0.04, 0.01, 0.02, 0.05],
            "ret_6m": [0.05, 0.06, 0.02, 0.01, 0.07],
            "adv20_dollar": [1e6, 2e6, 3e6, 4e6, 5e6],
            "volume_available": [1.0, 1.0, 1.0, 1.0, 1.0],
            "trend_r2_6m": [0.4, 0.5, 0.3, 0.6, 0.35],
            "pct_days_above_sma200": [0.6, 0.7, 0.5, 0.8, 0.55],
            "ulcer_index_60d": [0.2, 0.1, 0.3, 0.15, 0.25],
            "missing_day_rate": [0.0, 0.02, 0.01, 0.0, 0.03],
            "stale_price_flag": [0.0, 0.0, 0.0, 0.0, 0.0],
            "corp_action_suspect": [0.0, 0.0, 0.0, 0.0, 0.0],
            "vol60_ann": [0.5, 0.6, 0.4, 0.7, 0.3],
            "max_drawdown_6m": [-0.2, -0.1, -0.3, -0.25, -0.15],
            "worst_5d_return": [-0.1, -0.08, -0.12, -0.2, -0.05],
            "cvar_60d": [-0.08, -0.06, -0.1, -0.12, -0.04],
            "gap_atr": [0.5, 0.2, 0.1, 0.4, 0.3],
            "range_expansion": [1.2, 1.0, 0.9, 1.1, 1.3],
            "big_day_freq": [0.1, 0.2, 0.05, 0.15, 0.12],
            "close_to_high": [0.8, 0.7, 0.6, 0.9, 0.75],
            "theme_tags": ["defense", "", "", "tech", ""],
        }
    )
    scored = score_frame(
        df,
        {
            "trend": 0.22,
            "momentum": 0.2,
            "liquidity": 0.12,
            "quality": 0.14,
            "vol_penalty": 0.12,
            "dd_penalty": 0.1,
            "tail_penalty": 0.05,
            "attention": 0.05,
            "theme_bonus": 0.05,
            "volume_missing_penalty": 0.05,
        },
    )
    assert "monitor_priority_1_10" in scored.columns
    assert scored["monitor_priority_1_10"].between(1, 10).all()
