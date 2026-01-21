from market_monitor.gates import apply_gates


def test_gates_pass():
    features = {
        "last_price": 5.0,
        "adv20_dollar": 2_000_000,
        "zero_volume_frac": 0.0,
        "history_days": 300,
    }
    eligible, codes = apply_gates(features, None, None)
    assert eligible
    assert codes == []


def test_gates_fail_price():
    features = {
        "last_price": 12.0,
        "adv20_dollar": 2_000_000,
        "zero_volume_frac": 0.0,
        "history_days": 300,
    }
    eligible, codes = apply_gates(features, None, 10.0)
    assert not eligible
    assert "PRICE_MAX" in codes


def test_gates_price_floor():
    features = {
        "last_price": 0.5,
    }
    eligible, codes = apply_gates(features, 1.0, None)
    assert not eligible
    assert "PRICE_MIN" in codes


def test_gates_defaults_do_not_exclude():
    features = {
        "last_price": 5.0,
        "adv20_dollar": None,
        "zero_volume_frac": None,
        "history_days": 300,
        "volume_available": False,
    }
    eligible, codes = apply_gates(features, None, None)
    assert eligible
    assert codes == []
