from __future__ import annotations

from pathlib import Path

import pandas as pd

from market_monitor.bulk.models import BulkDownloadTask, BulkSource


def build_download_plan(
    sources: list[BulkSource],
    symbols: pd.Series,
    destination_root: Path,
    use_archives: bool = False,
) -> list[BulkDownloadTask]:
    tasks: list[BulkDownloadTask] = []
    normalized_symbols = [sym.strip().upper() for sym in symbols if str(sym).strip()]

    for source in sources:
        source_dir = destination_root / source.name
        if use_archives and source.supports_bulk_archive:
            archive_name = f"{source.name}_archive"
            if source.archive_path:
                archive_name = Path(source.archive_path).stem
            destination = source_dir / f"{archive_name}.zip"
            tasks.append(
                BulkDownloadTask(
                    source_name=source.name,
                    url=source.build_archive_url(),
                    destination=destination,
                    symbol=None,
                    is_archive=True,
                    kind="archive",
                )
            )
            continue

        if source.static_path:
            destination = source_dir / Path(source.static_path).name
            tasks.append(
                BulkDownloadTask(
                    source_name=source.name,
                    url=source.build_static_url(),
                    destination=destination,
                    symbol=None,
                    is_archive=False,
                    kind="static",
                )
            )
            continue

        for symbol in normalized_symbols:
            destination = source_dir / f"{symbol}{source.file_extension}"
            tasks.append(
                BulkDownloadTask(
                    source_name=source.name,
                    url=source.build_symbol_url(symbol),
                    destination=destination,
                    symbol=symbol,
                    is_archive=False,
                    kind="symbol",
                )
            )

    return tasks
