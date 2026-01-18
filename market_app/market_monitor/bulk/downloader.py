from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile

import requests

from market_monitor.bulk.models import BulkDownloadTask
from market_monitor.providers.http import RetryConfig, request_with_backoff


@dataclass(frozen=True)
class DownloadSummary:
    planned: int
    downloaded: int
    skipped: int
    failed: int


def download_tasks(
    tasks: Iterable[BulkDownloadTask],
    *,
    retry_config: RetryConfig | None = None,
    timeout_s: float = 60,
    dry_run: bool = False,
    extract_archives: bool = False,
    session: requests.Session | None = None,
    logger=None,
) -> DownloadSummary:
    planned = 0
    downloaded = 0
    skipped = 0
    failed = 0
    session = session or requests.Session()

    for task in tasks:
        planned += 1
        if logger:
            logger.info(f"[bulk] {task.kind} -> {task.destination}")
        if dry_run:
            skipped += 1
            continue

        task.destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            response = request_with_backoff(
                task.url,
                session=session,
                retry=retry_config,
                timeout=timeout_s,
                headers={"User-Agent": "market-monitor-bulk/1.0"},
            )
            response.raise_for_status()
            task.destination.write_bytes(response.content)
            downloaded += 1

            if extract_archives and task.is_archive:
                _extract_archive(task.destination)
        except requests.RequestException:
            failed += 1

    return DownloadSummary(
        planned=planned,
        downloaded=downloaded,
        skipped=skipped,
        failed=failed,
    )


def _extract_archive(path: Path) -> None:
    if path.suffix.lower() != ".zip":
        return
    extract_dir = path.with_suffix("")
    extract_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(path, "r") as archive:
        archive.extractall(extract_dir)
