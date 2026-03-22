from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_lower_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = value.replace(";", ",").replace("|", ",").split(",")
    else:
        items = list(value)
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _normalize_text(item).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _normalize_upper_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = value.replace(";", ",").replace("|", ",").split(",")
    else:
        items = list(value)
    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _normalize_text(item).upper()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


@dataclass(frozen=True)
class PolicyEvent:
    event_id: str
    event_type: str
    source: str
    agency: str
    event_date: str
    title: str
    summary: str
    sectors: tuple[str, ...] = ()
    tickers: tuple[str, ...] = ()
    countries: tuple[str, ...] = ()
    codes: tuple[str, ...] = ()
    severity: float = 0.5
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "PolicyEvent":
        event_id = _normalize_text(
            payload.get("event_id")
            or payload.get("id")
            or payload.get("slug")
            or payload.get("title")
        )
        if not event_id:
            raise ValueError("Policy event is missing event_id/title.")

        event_type = _normalize_text(
            payload.get("event_type") or payload.get("type") or "unknown"
        ).lower()
        event_date = _normalize_text(
            payload.get("event_date") or payload.get("date") or payload.get("published_at")
        )
        if not event_date:
            raise ValueError(f"Policy event '{event_id}' is missing event_date/date.")

        severity_raw = payload.get("severity", 0.5)
        try:
            severity = float(severity_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Policy event '{event_id}' has invalid severity: {severity_raw}") from exc

        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        return cls(
            event_id=event_id,
            event_type=event_type,
            source=_normalize_text(payload.get("source") or "local_cache"),
            agency=_normalize_text(payload.get("agency") or payload.get("issuer") or "unknown"),
            event_date=event_date,
            title=_normalize_text(payload.get("title") or event_id),
            summary=_normalize_text(payload.get("summary") or payload.get("description")),
            sectors=_normalize_lower_list(payload.get("sectors") or payload.get("themes")),
            tickers=_normalize_upper_list(payload.get("tickers") or payload.get("symbols")),
            countries=_normalize_upper_list(payload.get("countries") or payload.get("country_codes")),
            codes=_normalize_upper_list(payload.get("codes") or payload.get("policy_codes")),
            severity=max(0.0, min(severity, 1.0)),
            metadata=dict(metadata),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "agency": self.agency,
            "event_date": self.event_date,
            "title": self.title,
            "summary": self.summary,
            "sectors": list(self.sectors),
            "tickers": list(self.tickers),
            "countries": list(self.countries),
            "codes": list(self.codes),
            "severity": self.severity,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class PolicyScenarioTemplate:
    name: str
    description: str
    event_type: str
    severity: float
    sectors: tuple[str, ...]
    tickers: tuple[str, ...]
    countries: tuple[str, ...] = ()
    linked_etfs: tuple[str, ...] = ()
    direction_bias: int = 0
    benchmark_symbol: str = "SPY"

    @classmethod
    def from_mapping(cls, name: str, payload: Mapping[str, Any]) -> "PolicyScenarioTemplate":
        direction_raw = payload.get("direction_bias", 0)
        if isinstance(direction_raw, str):
            direction_text = direction_raw.strip().lower()
            if direction_text in {"negative", "down", "adverse", "-1"}:
                direction_bias = -1
            elif direction_text in {"positive", "up", "supportive", "1"}:
                direction_bias = 1
            else:
                direction_bias = 0
        else:
            direction_bias = int(direction_raw or 0)
            direction_bias = -1 if direction_bias < 0 else 1 if direction_bias > 0 else 0

        severity_raw = payload.get("severity", 0.5)
        try:
            severity = float(severity_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Scenario '{name}' has invalid severity: {severity_raw}") from exc

        return cls(
            name=name,
            description=_normalize_text(payload.get("description") or name),
            event_type=_normalize_text(payload.get("event_type") or "unknown").lower(),
            severity=max(0.0, min(severity, 1.0)),
            sectors=_normalize_lower_list(payload.get("sectors") or payload.get("themes")),
            tickers=_normalize_upper_list(payload.get("tickers")),
            countries=_normalize_upper_list(payload.get("countries")),
            linked_etfs=_normalize_upper_list(payload.get("linked_etfs") or payload.get("etfs")),
            direction_bias=direction_bias,
            benchmark_symbol=_normalize_text(payload.get("benchmark_symbol") or "SPY").upper(),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "event_type": self.event_type,
            "severity": self.severity,
            "sectors": list(self.sectors),
            "tickers": list(self.tickers),
            "countries": list(self.countries),
            "linked_etfs": list(self.linked_etfs),
            "direction_bias": self.direction_bias,
            "benchmark_symbol": self.benchmark_symbol,
        }
