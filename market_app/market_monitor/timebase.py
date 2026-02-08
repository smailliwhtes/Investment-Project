from __future__ import annotations

from datetime import date, datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def today_utc() -> date:
    return utcnow().date()


def parse_as_of_date(value: str) -> date:
    if not value:
        raise ValueError("as_of_date is required")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid as_of_date '{value}'. Expected YYYY-MM-DD.") from exc


def parse_now_utc(value: str) -> datetime:
    if not value:
        raise ValueError("now_utc is required")
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(
            f"Invalid now_utc '{value}'. Expected YYYY-MM-DDTHH:MM:SSZ."
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
