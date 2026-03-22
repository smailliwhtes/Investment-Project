from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from market_monitor.hash_utils import hash_file
from market_monitor.version import __version__


def _hash_path(path: Path) -> dict[str, Any]:
    if path.is_file():
        return {
            "path": str(path),
            "hash_sha256": hash_file(path),
        }
    if path.is_dir():
        entries = []
        for item in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
            entries.append(
                {
                    "path": str(item),
                    "hash_sha256": hash_file(item),
                }
            )
        return {
            "path": str(path),
            "entries": entries,
        }
    return {"path": str(path), "missing": True}


def build_policy_manifest(
    *,
    run_id: str,
    scenario_name: str,
    created_at: str,
    as_of_date: str,
    seed: int,
    config_path: Path,
    config_hash: str,
    git_commit: str | None,
    regime_snapshot: dict[str, Any],
    input_paths: dict[str, Path],
    counts: dict[str, int],
    artifact_paths: list[Path],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "created_at": created_at,
        "as_of_date": as_of_date,
        "scenario_name": scenario_name,
        "seed": seed,
        "app": {
            "name": "market_monitor",
            "version": __version__,
            "git_commit": git_commit,
        },
        "config": {
            "path": str(config_path),
            "hash_sha256": config_hash,
        },
        "regime": regime_snapshot,
        "counts": counts,
        "inputs": {
            name: _hash_path(path)
            for name, path in sorted(input_paths.items(), key=lambda item: item[0])
        },
        "artifacts": [
            {
                "path": str(path),
                "hash_sha256": hash_file(path) if path.exists() and path.is_file() else None,
            }
            for path in artifact_paths
        ],
    }


def write_policy_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
