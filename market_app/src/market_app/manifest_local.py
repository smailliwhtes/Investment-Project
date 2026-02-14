from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def build_manifest(
    *,
    run_id: str,
    config: dict[str, Any],
    config_hash: str,
    git_sha: str | None,
    symbol_files: list[Path],
    ohlcv_files: list[Path],
    output_dir: Path,
    schema_versions: dict[str, str],
    as_of_date: str,
    cli_args: list[str],
    counts: dict[str, int],
) -> dict[str, Any]:
    from datetime import datetime, timezone

    ohlcv_manifest = {}
    for path in sorted(ohlcv_files):
        ohlcv_manifest[str(path)] = hash_file(path)
    manifest = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "as_of_date": as_of_date,
        "git_sha": git_sha,
        "config_hash": config_hash,
        "cli_args": cli_args,
        "counts": counts,
        "config": config,
        "inputs": {
            "symbols": {str(path): hash_file(path) for path in symbol_files},
            "ohlcv": ohlcv_manifest,
        },
        "schema_versions": schema_versions,
    }
    output_hashes = {}
    for path in sorted(output_dir.glob("*.csv")):
        output_hashes[str(path.name)] = hash_file(path)
    if (output_dir / "report.md").exists():
        output_hashes["report.md"] = hash_file(output_dir / "report.md")
    manifest["outputs"] = output_hashes
    return manifest


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
