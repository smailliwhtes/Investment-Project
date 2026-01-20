from market_monitor.gates import apply_gates


def test_gates_pass():
    features = {
        "last_price": 5.0,
        "adv20_dollar": 2_000_000,
        "zero_volume_frac": 0.0,
        "history_days": 300,
    }
    eligible, codes = apply_gates(features, 10.0, 1_000_000, 0.1, 252)
    assert eligible
    assert codes == []


def test_gates_fail_price():
    features = {
        "last_price": 12.0,
        "adv20_dollar": 2_000_000,
        "zero_volume_frac": 0.0,
        "history_days": 300,
    }
    eligible, codes = apply_gates(features, 10.0, 1_000_000, 0.1, 252)
    assert not eligible
    assert "PRICE_MAX" in codes


def test_gates_fail_zero_volume_and_history():
    features = {
        "last_price": 5.0,
        "adv20_dollar": 2_000_000,
        "zero_volume_frac": 0.25,
        "history_days": 100,
    }
    eligible, codes = apply_gates(features, 10.0, 1_000_000, 0.1, 252)
    assert not eligible
    assert "MAX_ZERO_VOLUME" in codes
    assert "INSUFFICIENT_HISTORY" in codes


def test_gates_skip_liquidity_when_volume_missing():
    features = {
        "last_price": 5.0,
        "adv20_dollar": None,
        "zero_volume_frac": None,
        "history_days": 300,
        "volume_available": False,
    }
    eligible, codes = apply_gates(features, 10.0, 1_000_000, 0.1, 252)
    assert eligible
    assert "MIN_ADV20" not in codes
