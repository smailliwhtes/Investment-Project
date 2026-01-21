from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any


_EVENT_CODE_RE = re.compile(r"^\d{2,4}$")
_ROOT_CODE_RE = re.compile(r"^\d{2}$")


@dataclass(frozen=True)
class TaxonomyParseResult:
    event_code: str | None
    root_code: str | None
    quad_class: int | None


def normalize_event_code(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(".0", "")
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    if not _EVENT_CODE_RE.match(digits):
        return None
    return digits


def normalize_root_code(value: Any, *, fallback_event_code: str | None = None) -> str | None:
    if value is not None and not (isinstance(value, float) and math.isnan(value)):
        text = str(value).strip()
        if text:
            text = text.replace(".0", "")
            digits = "".join(ch for ch in text if ch.isdigit())
            if digits:
                return digits[:2].zfill(2)
    root = normalize_event_code(value)
    if root:
        return root[:2].zfill(2)
    if fallback_event_code:
        return fallback_event_code[:2].zfill(2)
    return None


def normalize_quad_class(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        quad = int(float(text))
    except ValueError:
        return None
    if quad not in {1, 2, 3, 4}:
        return None
    return quad


def parse_taxonomy_fields(
    event_code: Any,
    root_code: Any,
    quad_class: Any,
) -> TaxonomyParseResult:
    normalized_event = normalize_event_code(event_code)
    normalized_root = normalize_root_code(root_code, fallback_event_code=normalized_event)
    normalized_quad = normalize_quad_class(quad_class)
    return TaxonomyParseResult(
        event_code=normalized_event,
        root_code=normalized_root,
        quad_class=normalized_quad,
    )


def is_valid_event_code(value: Any) -> bool:
    return normalize_event_code(value) is not None


def is_valid_root_code(value: Any) -> bool:
    value = normalize_root_code(value)
    return value is not None and bool(_ROOT_CODE_RE.match(value))


def is_valid_quad_class(value: Any) -> bool:
    return normalize_quad_class(value) is not None
