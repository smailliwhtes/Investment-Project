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
    geopolitics_files: list[Path] | None = None,
    geopolitics_hash: str | None = None,
    model_id: str | None = None,
    model_manifest_hash: str | None = None,
    output_dir: Path,
    schema_versions: dict[str, str],
) -> dict[str, Any]:
    manifest = {
        "run_id": run_id,
        "git_sha": git_sha,
        "config_hash": config_hash,
        "config": config,
        "inputs": {
            "symbols": {str(path): hash_file(path) for path in symbol_files},
            "ohlcv_sample": {str(path): hash_file(path) for path in ohlcv_files},
            "ohlcv_hash_strategy": "first_5_sorted",
            "geopolitics_inputs": {str(path): hash_file(path) for path in geopolitics_files or []},
            "geopolitics_hash": geopolitics_hash,
        },
        "model": {
            "model_id": model_id,
            "model_manifest_hash": model_manifest_hash,
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
