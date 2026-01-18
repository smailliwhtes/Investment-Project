from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from market_monitor.bulk.models import BulkDownloadTask


@dataclass(frozen=True)
class BulkManifest:
    created_at_utc: str
    tasks: list[BulkDownloadTask]

    @staticmethod
    def create(tasks: list[BulkDownloadTask]) -> "BulkManifest":
        timestamp = datetime.now(timezone.utc).isoformat()
        return BulkManifest(created_at_utc=timestamp, tasks=tasks)


def write_manifest(path: Path, manifest: BulkManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_at_utc": manifest.created_at_utc,
        "tasks": [
            {
                "source_name": task.source_name,
                "url": task.url,
                "destination": str(task.destination),
                "symbol": task.symbol,
                "is_archive": task.is_archive,
            }
            for task in manifest.tasks
        ],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def read_manifest(path: Path) -> BulkManifest:
    payload = json.loads(path.read_text(encoding="utf-8"))
    tasks = [
        BulkDownloadTask(
            source_name=item["source_name"],
            url=item["url"],
            destination=Path(item["destination"]),
            symbol=item.get("symbol"),
            is_archive=bool(item.get("is_archive")),
        )
        for item in payload.get("tasks", [])
    ]
    return BulkManifest(created_at_utc=payload["created_at_utc"], tasks=tasks)
