import pandas as pd

from market_monitor.scoring import score_frame


def test_scoring_deciles():
    df = pd.DataFrame({
        "sma50_ratio": [0.1, 0.2, 0.3, 0.4, 0.5],
        "ret_6m": [0.05, 0.06, 0.02, 0.01, 0.07],
        "adv20_dollar": [1e6, 2e6, 3e6, 4e6, 5e6],
        "vol60_ann": [0.5, 0.6, 0.4, 0.7, 0.3],
        "max_drawdown_6m": [-0.2, -0.1, -0.3, -0.25, -0.15],
        "worst_5d_return": [-0.1, -0.08, -0.12, -0.2, -0.05],
        "theme_tags": ["defense", "", "", "tech", ""],
    })
    scored = score_frame(df, {
        "trend": 0.25,
        "momentum": 0.25,
        "liquidity": 0.15,
        "vol_penalty": 0.15,
        "dd_penalty": 0.1,
        "tail_penalty": 0.05,
        "theme_bonus": 0.05,
    })
    assert "monitor_priority_1_10" in scored.columns
    assert scored["monitor_priority_1_10"].between(1, 10).all()
