from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from market_monitor.policy_event_schema import PolicyEvent


class PolicyEventsError(RuntimeError):
    pass


def load_policy_source_catalog(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise PolicyEventsError(f"Policy source catalog not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise PolicyEventsError(f"Policy source catalog must be a mapping: {path}")
    return payload


def load_policy_events(path: Path) -> list[PolicyEvent]:
    if not path.exists():
        raise PolicyEventsError(f"Policy events file not found: {path}")

    suffix = path.suffix.lower()
    records: list[dict[str, Any]]
    if suffix == ".jsonl":
        records = []
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    elif suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = payload.get("events", [])
        if not isinstance(payload, list):
            raise PolicyEventsError(f"Policy events JSON must contain a list: {path}")
        records = payload
    elif suffix == ".csv":
        frame = pd.read_csv(path)
        records = frame.fillna("").to_dict(orient="records")
    else:
        raise PolicyEventsError(f"Unsupported policy events format: {path}")

    events = [PolicyEvent.from_mapping(record) for record in records]
    events.sort(key=lambda event: (event.event_date, event.event_id))
    return events


def write_policy_events_cache(path: Path, events: list[PolicyEvent]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for event in events:
            handle.write(json.dumps(event.to_mapping(), sort_keys=True))
            handle.write("\n")


def load_cached_or_fetch_json(url: str, cache_path: Path, *, allow_network: bool = False) -> Any:
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))
    if not allow_network:
        raise PolicyEventsError(
            f"Cached policy source missing and network disabled: {cache_path} ({url})"
        )

    import requests

    response = requests.get(url, timeout=20)
    response.raise_for_status()
    payload = response.json()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload
