from __future__ import annotations

import numpy as np
import pytest

from market_monitor.quant_math import (
    TRADING_DAYS_PER_YEAR,
    annualized_volatility,
    beta,
    binary_log_loss,
    brier_score,
    cvar,
    downside_volatility,
    expected_calibration_error,
    information_ratio,
    log_returns,
    max_drawdown_from_prices,
    max_drawdown_from_returns,
    sharpe_ratio,
    simple_returns,
    sortino_ratio,
    trend_slope_r2,
    wilder_atr,
    wilder_rsi,
)


def test_return_helpers_are_consistent() -> None:
    prices = np.array([100.0, 101.0, 99.0, 102.0])
    r_simple = simple_returns(prices)
    r_log = log_returns(prices)

    assert r_simple.shape == (3,)
    assert r_log.shape == (3,)
    assert r_simple[0] == pytest.approx(0.01, rel=1e-9)
    assert np.exp(np.sum(r_log)) - 1.0 == pytest.approx(prices[-1] / prices[0] - 1.0, rel=1e-9)


def test_risk_metrics_follow_nyse_trading_day_convention() -> None:
    returns = np.array([0.01, -0.005, 0.007, -0.002, 0.004, -0.003, 0.006, 0.002])

    vol = annualized_volatility(returns)
    down = downside_volatility(returns)
    tail = cvar(returns, alpha=0.95)

    assert vol > 0
    assert down >= 0
    assert tail <= np.quantile(returns, 0.05)


def test_drawdown_helpers() -> None:
    prices = np.array([100, 110, 108, 90, 95, 120], dtype=float)
    dd_prices = max_drawdown_from_prices(prices)

    returns = simple_returns(prices)
    dd_returns = max_drawdown_from_returns(returns)

    assert dd_prices == pytest.approx(-0.1818181818, rel=1e-6)
    assert dd_returns == pytest.approx(dd_prices, rel=1e-6)


def test_wilder_indicators_and_trend_quality() -> None:
    close = np.linspace(100, 130, 40)
    noise = np.sin(np.arange(40) / 3.0)
    close = close + noise
    high = close * 1.01
    low = close * 0.99

    rsi = wilder_rsi(close, period=14)
    atr = wilder_atr(high, low, close, period=14)
    slope, r2 = trend_slope_r2(close)

    assert 0 <= rsi <= 100
    assert atr > 0
    assert slope > 0
    assert 0 <= r2 <= 1


def test_ratio_and_calibration_metrics() -> None:
    portfolio = np.array([0.01, 0.015, -0.002, 0.007, 0.003, 0.009, -0.001])
    benchmark = np.array([0.008, 0.012, -0.003, 0.005, 0.002, 0.006, -0.002])

    b = beta(portfolio, benchmark)
    sr = sharpe_ratio(portfolio, risk_free_rate_annual=0.02)
    so = sortino_ratio(portfolio, risk_free_rate_annual=0.02)
    ir = information_ratio(portfolio, benchmark)

    y_true = np.array([1, 0, 1, 1, 0, 0, 1], dtype=float)
    y_prob = np.array([0.81, 0.32, 0.63, 0.72, 0.4, 0.2, 0.77], dtype=float)

    bs = brier_score(y_true, y_prob)
    ll = binary_log_loss(y_true, y_prob)
    ece = expected_calibration_error(y_true, y_prob, n_bins=5)

    assert np.isfinite(b)
    assert np.isfinite(sr)
    assert np.isfinite(so)
    assert np.isfinite(ir)
    assert 0 <= bs <= 1
    assert ll >= 0
    assert 0 <= ece <= 1
    assert TRADING_DAYS_PER_YEAR == 252
