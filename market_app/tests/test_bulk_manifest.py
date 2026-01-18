from pathlib import Path

from market_monitor.bulk.manifest import BulkManifest, read_manifest, write_manifest
from market_monitor.bulk.models import BulkDownloadTask


def test_manifest_round_trip(tmp_path: Path):
    tasks = [
        BulkDownloadTask(
            source_name="stooq",
            url="https://stooq.pl/q/d/l/?s=aapl.us&i=d",
            destination=tmp_path / "stooq" / "AAPL.csv",
            symbol="AAPL",
            is_archive=False,
        )
    ]
    manifest = BulkManifest.create(tasks)
    path = tmp_path / "manifests" / "bulk_manifest.json"

    write_manifest(path, manifest)
    loaded = read_manifest(path)

    assert loaded.created_at_utc == manifest.created_at_utc
    assert loaded.tasks == manifest.tasks
