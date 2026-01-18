from __future__ import annotations

from typing import Any

from market_monitor.bulk.models import BulkSource


def load_bulk_sources(config: dict[str, Any]) -> list[BulkSource]:
    sources_cfg = config.get("bulk", {}).get("sources", [])
    sources: list[BulkSource] = []
    seen_names: set[str] = set()

    for entry in sources_cfg:
        name = entry["name"]
        if name in seen_names:
            raise ValueError(f"Duplicate bulk source name: {name}")
        seen_names.add(name)
        sources.append(
            BulkSource(
                name=name,
                base_url=entry["base_url"],
                symbol_template=entry.get("symbol_template"),
                supports_bulk_archive=bool(entry.get("supports_bulk_archive", False)),
                archive_path=entry.get("archive_path"),
                static_path=entry.get("static_path"),
                file_extension=entry.get("file_extension", ".csv"),
            )
        )

    return sources
