from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING

import pandas as pd

from market_monitor.hash_utils import hash_file, hash_manifest, hash_text

if TYPE_CHECKING:
    from market_monitor.preflight import PreflightReport


def generate_run_id(
    *,
    timestamp: datetime,
    config_hash: str,
    watchlist_hash: str,
    corpus_manifest_hash: str | None = None,
) -> str:
    stamp = timestamp.strftime("%Y%m%d_%H%M%S")
    digest = hashlib.sha256(
        f"{config_hash}:{watchlist_hash}:{corpus_manifest_hash or ''}".encode("utf-8")
    ).hexdigest()
    return f"{stamp}_{digest[:8]}"


def resolve_git_commit(repo_root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            check=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def _redact_config(config: dict[str, Any]) -> dict[str, Any]:
    def redact(value: Any, key: str | None = None) -> Any:
        if isinstance(value, dict):
            return {k: redact(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(v, key) for v in value]
        if key and any(token in key.lower() for token in ("key", "secret", "token", "password")):
            return "<redacted>"
        return value

    return redact(config)


def _watchlist_hash(watchlist_df: pd.DataFrame) -> str:
    symbols = watchlist_df["symbol"].astype(str).tolist() if not watchlist_df.empty else []
    payload = "\n".join(symbols)
    return hash_text(payload)


def _collect_versions() -> dict[str, str]:
    versions = {"python": sys.version.split()[0]}
    try:
        import importlib.metadata as metadata
    except ImportError:
        import importlib_metadata as metadata  # type: ignore

    for name in ("pandas", "numpy", "requests"):
        try:
            versions[name] = metadata.version(name)
        except metadata.PackageNotFoundError:
            versions[name] = "unknown"
    return versions


@dataclass(frozen=True)
class RunManifest:
    payload: dict[str, Any]

    def to_json(self, *, indent: int | None = None) -> str:
        return json.dumps(self.payload, indent=indent, sort_keys=True)


def build_run_manifest(
    *,
    run_id: str,
    run_start: datetime,
    run_end: datetime,
    config: dict[str, Any],
    config_hash: str,
    watchlist_path: Path | None,
    watchlist_df: pd.DataFrame,
    summary: dict[str, int],
    scored: pd.DataFrame,
    preflight: PreflightReport | None,
    git_commit: str | None,
    corpus_manifest: dict[str, Any] | None = None,
) -> RunManifest:
    watchlist_file_hash = None
    if watchlist_path and watchlist_path.exists():
        watchlist_file_hash = hash_file(watchlist_path)

    watchlist_hash = _watchlist_hash(watchlist_df)
    ohlcv_dir = (
        config.get("data_roots", {}).get("ohlcv_dir")
        or config.get("data", {}).get("paths", {}).get("nasdaq_daily_dir")
    )
    input_files: dict[str, dict[str, str]] = {}
    symbol_ranges: dict[str, dict[str, int | str | None]] = {}
    if preflight:
        for symbol in preflight.symbols:
            if symbol.status != "FOUND" or not symbol.file_path:
                continue
            try:
                path = Path(symbol.file_path)
                input_files[symbol.symbol] = {
                    "path": str(path),
                    "sha256": hash_file(path),
                }
            except OSError:
                continue
            symbol_ranges[symbol.symbol] = {
                "rows": symbol.rows,
                "start_date": symbol.start_date,
                "end_date": symbol.end_date,
            }

    corpus_files = []
    if corpus_manifest:
        for entry in corpus_manifest.get("files", []):
            corpus_files.append(
                {
                    "path": entry.get("path"),
                    "sha256": entry.get("sha256"),
                    "rows": entry.get("rows"),
                    "min_date": entry.get("min_date"),
                    "max_date": entry.get("max_date"),
                }
            )

    eligible_count = int(scored["eligible"].sum()) if "eligible" in scored.columns else 0
    counts = {
        "symbols_requested": int(len(watchlist_df)),
        "symbols_found": int(len(preflight.found_symbols)) if preflight else 0,
        "symbols_processed": int(summary.get("stage3", 0)),
        "symbols_eligible": eligible_count,
    }

    risk_summary = {}
    if "risk_level" in scored.columns and not scored.empty:
        risk_summary = scored["risk_level"].value_counts(dropna=False).to_dict()

    payload = {
        "run_id": run_id,
        "git_commit": git_commit,
        "start_timestamp_utc": run_start.isoformat(),
        "end_timestamp_utc": run_end.isoformat(),
        "config_hash": config_hash,
        "config": _redact_config(config),
        "ohlcv_dir": ohlcv_dir,
        "watchlist_file": str(watchlist_path) if watchlist_path else None,
        "watchlist_file_hash": watchlist_file_hash,
        "watchlist_hash": watchlist_hash,
        "input_files": input_files,
        "ohlcv_symbol_ranges": symbol_ranges,
        "corpus_files": corpus_files,
        "counts": counts,
        "summary": summary,
        "risk_summary": risk_summary,
        "corpus_manifest": corpus_manifest,
        "versions": _collect_versions(),
    }
    return RunManifest(payload=payload)


def run_id_from_inputs(
    *,
    timestamp: datetime,
    config_hash: str,
    watchlist_path: Path | None,
    watchlist_df: pd.DataFrame,
    corpus_manifest_hash: str | None = None,
) -> str:
    if watchlist_path and watchlist_path.exists():
        watchlist_hash = hash_file(watchlist_path)
    else:
        watchlist_hash = _watchlist_hash(watchlist_df)
    return generate_run_id(
        timestamp=timestamp,
        config_hash=config_hash,
        watchlist_hash=watchlist_hash,
        corpus_manifest_hash=corpus_manifest_hash,
    )
