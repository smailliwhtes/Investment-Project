from market_monitor.taxonomy import (
    normalize_event_code,
    normalize_quad_class,
    normalize_root_code,
    parse_taxonomy_fields,
)


def test_taxonomy_normalization_examples() -> None:
    assert normalize_event_code("190") == "190"
    assert normalize_event_code("190.0") == "190"
    assert normalize_root_code("1") == "01"
    assert normalize_root_code("19") == "19"
    assert normalize_quad_class("4") == 4
    assert normalize_quad_class(2) == 2


def test_taxonomy_parse_fallbacks() -> None:
    parsed = parse_taxonomy_fields(event_code="173", root_code=None, quad_class="3")
    assert parsed.event_code == "173"
    assert parsed.root_code == "17"
    assert parsed.quad_class == 3
