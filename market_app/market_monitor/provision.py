from __future__ import annotations

import json
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from market_monitor.gdelt.doctor import normalize_corpus
from market_monitor.hash_utils import hash_file
from market_monitor.ohlcv_doctor import normalize_directory


@dataclass(frozen=True)
class Inventory:
    created_at_utc: str
    source: str
    destination: str
    files: list[dict]


def _write_inventory(dest_dir: Path, source: str, files: list[Path]) -> Path:
    entries = []
    for path in sorted(files):
        if path.is_dir():
            continue
        entries.append(
            {
                "path": str(path.relative_to(dest_dir)),
                "hash": hash_file(path),
                "bytes": path.stat().st_size,
            }
        )
    payload = Inventory(
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        source=source,
        destination=str(dest_dir),
        files=entries,
    )
    inventory_path = dest_dir / "inventory.json"
    inventory_path.write_text(json.dumps(payload.__dict__, indent=2), encoding="utf-8")
    return inventory_path


def _ensure_dirs(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def init_dirs(root: Path) -> dict:
    root = root.expanduser().resolve()
    ohlcv_raw = root / "ohlcv_raw"
    ohlcv_daily = root / "ohlcv_daily"
    exogenous_daily = root / "exogenous" / "daily_features"
    outputs = root / "outputs"
    _ensure_dirs([ohlcv_raw, ohlcv_daily, exogenous_daily, outputs])
    return {
        "root": str(root),
        "ohlcv_raw": str(ohlcv_raw),
        "ohlcv_daily": str(ohlcv_daily),
        "exogenous_daily": str(exogenous_daily),
        "outputs": str(outputs),
    }


def _extract_or_copy(src: Path, dest: Path) -> list[Path]:
    dest.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        copied: list[Path] = []
        for item in src.rglob("*"):
            if item.is_dir():
                continue
            rel = item.relative_to(src)
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)
            copied.append(target)
        return copied
    if src.suffix.lower() == ".zip":
        with zipfile.ZipFile(src, "r") as archive:
            archive.extractall(dest)
        return [path for path in dest.rglob("*") if path.is_file()]
    raise ValueError(f"Unsupported source: {src}")


def import_ohlcv(
    *,
    src: Path,
    dest: Path,
    normalize: bool,
    date_col: str | None = None,
    delimiter: str | None = None,
) -> dict:
    src = src.expanduser().resolve()
    dest = dest.expanduser().resolve()
    files = _extract_or_copy(src, dest)
    inventory_path = _write_inventory(dest, str(src), files)

    normalized_manifest = None
    if normalize:
        normalized = normalize_directory(
            raw_dir=dest,
            out_dir=dest.parent / "ohlcv_daily",
            date_col=date_col,
            delimiter=delimiter,
            symbol_from_filename=True,
            coerce=True,
            strict=False,
            streaming=True,
            chunk_rows=200_000,
        )
        normalized_manifest = str(normalized["manifest_path"])

    return {
        "inventory_path": str(inventory_path),
        "normalized_manifest": normalized_manifest,
    }


def import_exogenous(
    *,
    src: Path,
    dest: Path,
    normalize: bool,
    normalized_dest: Path | None = None,
    file_glob: str | None = None,
    format_hint: str = "auto",
    write_format: str = "csv",
    date_col: str | None = None,
    allow_annual: bool = False,
) -> dict:
    src = src.expanduser().resolve()
    dest = dest.expanduser().resolve()
    files = _extract_or_copy(src, dest)
    inventory_path = _write_inventory(dest, str(src), files)

    if normalize:
        normalized_dir = (normalized_dest or dest).expanduser().resolve()
        normalize_corpus(
            raw_dir=dest,
            gdelt_dir=normalized_dir,
            file_glob=file_glob,
            format_hint=format_hint,
            write_format=write_format,
            date_col=date_col,
            allow_annual=allow_annual,
        )

    return {
        "inventory_path": str(inventory_path),
    }
