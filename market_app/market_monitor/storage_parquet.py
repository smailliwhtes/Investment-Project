from __future__ import annotations

import csv
import json
import re
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from market_monitor.hash_utils import hash_file


_IGNORED_FILENAMES = {
    "universe.csv",
    "conversion_errors.csv",
    "corpus_transform_params.json",
    "make_corpus_stable.py",
}
_IGNORED_SUFFIXES = {".json", ".log", ".md", ".ps1", ".py", ".yaml", ".yml"}
_IGNORED_DIRS = {"_archive_originals", "__pycache__"}
_MARKET_RAW_BUCKETS = {"New Daily Files", "All Files Together", "d_us_txt"}
_MARKET_DUPLICATE_BUCKETS = {"ohlcv_daily_csv", "_nasdaq_daily_flat_norm"}
_WORKING_STATE_DIR = "_preprocessor_state"
_CHECKPOINT_FILENAME = "conversion_checkpoint.jsonl"
_DAY_COLUMN_CANDIDATES = (
    "day",
    "date",
    "dt",
    "as_of_date",
    "event_date",
    "event_day",
    "timestamp",
    "datetime",
)
_YEAR_COLUMN_CANDIDATES = ("year",) + _DAY_COLUMN_CANDIDATES
_CORPUS_RULES = {
    "GDELT_Data_1.csv": ("corpus_daily_features_raw", "daily_features_raw"),
    "GDELT_Data_1_stable.csv": ("corpus_daily_features_stable", "daily_features_stable"),
    "gdelt_conflict_1_0.csv": ("corpus_annual_conflict", "annual_conflict"),
}


@dataclass(frozen=True)
class StorageAsset:
    root_name: str
    source_path: Path
    relative_path: str
    source_format: str
    dataset_role: str
    status: str
    action: str
    target_path: str | None
    archive_path: str | None
    reason: str
    compression: str | None = None
    partitioning: str | None = None


@dataclass(frozen=True)
class StorageRoots:
    market_root: Path
    corpus_root: Path
    working_root: Path


def audit_parquet_storage(
    *,
    market_root: Path,
    corpus_root: Path,
    working_root: Path,
    out_dir: Path,
) -> dict[str, Any]:
    roots = _resolve_roots(market_root=market_root, corpus_root=corpus_root, working_root=working_root)
    assets = _discover_assets(
        roots=roots,
        out_dir=out_dir,
        archive_root=out_dir / "_archive_preview",
        archive_stamp="preview",
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    inventory = _build_inventory_payload(roots=roots, assets=assets)
    plan = _build_migration_plan(assets)

    _write_json(out_dir / "inventory.json", inventory)
    _write_inventory_csv(out_dir / "inventory.csv", assets)
    _write_json(out_dir / "migration_plan.json", plan)
    (out_dir / "migration_report.md").write_text(
        _render_audit_report(roots=roots, inventory=inventory, plan=plan),
        encoding="utf-8",
        newline="\n",
    )
    return {
        "inventory_path": str(out_dir / "inventory.json"),
        "inventory_csv_path": str(out_dir / "inventory.csv"),
        "migration_plan_path": str(out_dir / "migration_plan.json"),
        "migration_report_path": str(out_dir / "migration_report.md"),
        "inventory": inventory,
        "migration_plan": plan,
    }


def migrate_parquet_storage(
    *,
    market_root: Path,
    corpus_root: Path,
    working_root: Path,
    out_dir: Path,
    archive_root: Path | None,
    apply_changes: bool,
) -> dict[str, Any]:
    roots = _resolve_roots(market_root=market_root, corpus_root=corpus_root, working_root=working_root)
    archive_stamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    assets = _discover_assets(
        roots=roots,
        out_dir=out_dir,
        archive_root=archive_root.expanduser().resolve() if archive_root else None,
        archive_stamp=archive_stamp,
    )
    checkpoint_path = out_dir / _CHECKPOINT_FILENAME
    checkpoint_records = _load_checkpoint_records(checkpoint_path)
    manifest_entries: dict[str, dict[str, Any]] = {}
    rollback_entries: dict[str, dict[str, Any]] = {}
    parity_entries: dict[str, dict[str, Any]] = {}
    for asset_key, record in checkpoint_records.items():
        entry = record.get("entry")
        if isinstance(entry, dict):
            manifest_entries[asset_key] = entry
        rollback = record.get("rollback")
        if isinstance(rollback, dict):
            rollback_entries[asset_key] = rollback
        parity = record.get("parity")
        if isinstance(parity, dict):
            parity_entries[asset_key] = parity

    for asset in assets:
        asset_key = _asset_key(asset.source_path)
        entry = asdict(asset)
        entry["source_path"] = str(asset.source_path)
        if asset.action == "skip":
            entry["status"] = "skipped"
            manifest_entries[asset_key] = entry
            rollback_entries.pop(asset_key, None)
            parity_entries.pop(asset_key, None)
            continue

        if not apply_changes:
            entry["status"] = "planned"
            manifest_entries[asset_key] = entry
            rollback_entries[asset_key] = _build_rollback_entry(asset=asset, applied=False)
            parity_entries.pop(asset_key, None)
            continue

        _require_parquet_support()
        source_df = _read_source_dataframe(asset.source_path)
        target_path = Path(asset.target_path or "")
        archive_path = Path(asset.archive_path or "")
        staged_target_path = _staging_target_path(target_path)
        _cleanup_path(staged_target_path)
        try:
            _write_target_dataframe(asset=asset, dataframe=source_df, target_path=staged_target_path)
            parity = _check_parity(asset=asset, source_df=source_df, target_path=staged_target_path)
            if not parity["parity_passed"]:
                raise RuntimeError(
                    f"Parity check failed for {asset.source_path}: "
                    f"{json.dumps(parity, sort_keys=True)}"
                )
            _replace_target(staged_target_path=staged_target_path, target_path=target_path)
        except Exception:
            _cleanup_path(staged_target_path)
            raise

        archive_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(asset.source_path), str(archive_path))

        entry["status"] = "applied"
        entry["source_hash_sha256"] = hash_file(archive_path)
        entry["target_hash_sha256"] = _hash_target(target_path)
        entry["rows"] = int(len(source_df))
        entry["parity_passed"] = True
        manifest_entries[asset_key] = entry
        rollback_entry = _build_rollback_entry(asset=asset, applied=True)
        rollback_entries[asset_key] = rollback_entry
        parity_entries[asset_key] = parity
        _append_checkpoint_record(
            checkpoint_path,
            {
                "asset_key": asset_key,
                "ts": _utc_now(),
                "entry": entry,
                "rollback": rollback_entry,
                "parity": parity,
            },
        )

    manifest_payload, rollback_payload, parity_payload = _build_migration_payloads(
        manifest_entries=manifest_entries,
        rollback_entries=rollback_entries,
        parity_entries=parity_entries,
        apply_changes=apply_changes,
    )
    report_path = out_dir / "conversion_report.md"
    report_path.write_text(
        _render_conversion_report(manifest=manifest_payload, parity=parity_payload),
        encoding="utf-8",
        newline="\n",
    )
    _write_json(out_dir / "conversion_manifest.json", manifest_payload)
    _write_json(out_dir / "rollback_manifest.json", rollback_payload)
    _write_json(out_dir / "parity_checks.json", parity_payload)
    return {
        "conversion_manifest_path": str(out_dir / "conversion_manifest.json"),
        "conversion_report_path": str(report_path),
        "rollback_manifest_path": str(out_dir / "rollback_manifest.json"),
        "parity_checks_path": str(out_dir / "parity_checks.json"),
        "conversion_checkpoint_path": str(checkpoint_path),
        "apply": bool(apply_changes),
        "summary": {
            "planned_or_applied": sum(1 for asset in assets if asset.action != "skip"),
            "skipped": sum(1 for asset in assets if asset.action == "skip"),
        },
    }


def _resolve_roots(*, market_root: Path, corpus_root: Path, working_root: Path) -> StorageRoots:
    roots = StorageRoots(
        market_root=market_root.expanduser().resolve(),
        corpus_root=corpus_root.expanduser().resolve(),
        working_root=working_root.expanduser().resolve(),
    )
    for path in (roots.market_root, roots.corpus_root, roots.working_root):
        if not path.exists():
            raise FileNotFoundError(f"Missing storage root: {path}")
    return roots


def _discover_assets(
    *,
    roots: StorageRoots,
    out_dir: Path,
    archive_root: Path | None,
    archive_stamp: str,
) -> list[StorageAsset]:
    assets: list[StorageAsset] = []
    assets.extend(
        _classify_root(
            root_name="market_root",
            root_path=roots.market_root,
            out_dir=out_dir,
            archive_root_base=_resolve_archive_base(
                root_path=roots.market_root,
                root_name="market_root",
                archive_root=archive_root,
                archive_stamp=archive_stamp,
            ),
        )
    )
    assets.extend(
        _classify_root(
            root_name="corpus_root",
            root_path=roots.corpus_root,
            out_dir=out_dir,
            archive_root_base=_resolve_archive_base(
                root_path=roots.corpus_root,
                root_name="corpus_root",
                archive_root=archive_root,
                archive_stamp=archive_stamp,
            ),
        )
    )
    assets.extend(
        _classify_root(
            root_name="working_root",
            root_path=roots.working_root,
            out_dir=out_dir,
            archive_root_base=_resolve_archive_base(
                root_path=roots.working_root,
                root_name="working_root",
                archive_root=archive_root,
                archive_stamp=archive_stamp,
            ),
        )
    )
    return sorted(assets, key=lambda item: (item.root_name, item.relative_path))


def _classify_root(
    *,
    root_name: str,
    root_path: Path,
    out_dir: Path,
    archive_root_base: Path,
) -> list[StorageAsset]:
    assets: list[StorageAsset] = []
    for source_path in sorted(path for path in root_path.rglob("*") if path.is_file()):
        relative = source_path.relative_to(root_path)
        if any(part in _IGNORED_DIRS for part in relative.parts):
            continue
        asset = _classify_path(
            root_name=root_name,
            root_path=root_path,
            source_path=source_path,
            relative=relative,
            out_dir=out_dir,
            archive_root=archive_root_base,
        )
        if asset is not None:
            assets.append(asset)
    return assets


def _resolve_archive_base(
    *,
    root_path: Path,
    root_name: str,
    archive_root: Path | None,
    archive_stamp: str,
) -> Path:
    if archive_root is not None:
        return archive_root / root_name
    return root_path / "_archive_originals" / archive_stamp


def _classify_path(
    *,
    root_name: str,
    root_path: Path,
    source_path: Path,
    relative: Path,
    out_dir: Path,
    archive_root: Path,
) -> StorageAsset | None:
    if source_path.name in _IGNORED_FILENAMES or source_path.suffix.lower() in _IGNORED_SUFFIXES:
        return _skip_asset(
            root_name=root_name,
            source_path=source_path,
            relative=relative,
            reason="Control/helper file stays in text format.",
        )

    if root_name == "working_root":
        return _classify_working_path(
            root_path=root_path,
            source_path=source_path,
            relative=relative,
            archive_root=archive_root,
        )
    if root_name == "corpus_root":
        return _classify_corpus_path(
            root_path=root_path,
            source_path=source_path,
            relative=relative,
            archive_root=archive_root,
        )
    if root_name == "market_root":
        return _classify_market_path(
            root_path=root_path,
            source_path=source_path,
            relative=relative,
            out_dir=out_dir,
            archive_root=archive_root,
        )
    return None


def _classify_working_path(
    *,
    root_path: Path,
    source_path: Path,
    relative: Path,
    archive_root: Path,
) -> StorageAsset:
    if relative.parts and relative.parts[0] == _WORKING_STATE_DIR:
        reason = "Preprocessor state/control files remain in text format."
        if source_path.name.startswith("gdelt_") and source_path.suffix.lower() == ".csv":
            reason = "Preprocessor placeholder CSV is not treated as an authoritative dataset."
        return _skip_asset(
            root_name="working_root",
            source_path=source_path,
            relative=relative,
            reason=reason,
        )
    if len(relative.parts) == 1 and source_path.suffix.lower() == ".csv":
        target_path = root_path / f"{source_path.stem}.parquet"
        return StorageAsset(
            root_name="working_root",
            source_path=source_path,
            relative_path=str(relative).replace("\\", "/"),
            source_format="csv",
            dataset_role="canonical_normalized_ohlcv",
            status="planned",
            action="convert",
            target_path=str(target_path),
            archive_path=str((archive_root / relative).resolve()),
            reason="Canonical normalized OHLCV becomes one-file-per-symbol Parquet.",
            compression="zstd",
            partitioning="none",
        )
    return _skip_asset(
        root_name="working_root",
        source_path=source_path,
        relative=relative,
        reason="Unsupported working-root file for Parquet migration.",
    )


def _classify_corpus_path(
    *,
    root_path: Path,
    source_path: Path,
    relative: Path,
    archive_root: Path,
) -> StorageAsset:
    rule = _CORPUS_RULES.get(source_path.name)
    if rule is None:
        return _skip_asset(
            root_name="corpus_root",
            source_path=source_path,
            relative=relative,
            reason="File is not one of the approved corpus conversion inputs.",
        )
    dataset_role, target_dir_name = rule
    partitioning = "year" if dataset_role == "corpus_annual_conflict" else "day"
    return StorageAsset(
        root_name="corpus_root",
        source_path=source_path,
        relative_path=str(relative).replace("\\", "/"),
        source_format=source_path.suffix.lower().lstrip("."),
        dataset_role=dataset_role,
        status="planned",
        action="convert",
        target_path=str((root_path / target_dir_name).resolve()),
        archive_path=str((archive_root / relative).resolve()),
        reason="Approved corpus table becomes partitioned Parquet.",
        compression="zstd",
        partitioning=partitioning,
    )


def _classify_market_path(
    *,
    root_path: Path,
    source_path: Path,
    relative: Path,
    out_dir: Path,
    archive_root: Path,
) -> StorageAsset:
    top_level = relative.parts[0] if relative.parts else ""
    source_format = source_path.suffix.lower().lstrip(".")
    if source_path.suffix.lower() in {".csv", ".txt"} and source_path.stat().st_size == 0:
        return _skip_asset(
            root_name="market_root",
            source_path=source_path,
            relative=relative,
            reason="Empty market file is skipped.",
        )
    if top_level in _MARKET_RAW_BUCKETS and source_path.suffix.lower() in {".csv", ".txt"}:
        symbol = _symbol_from_filename(source_path)
        target_name = f"{_relative_source_slug(relative)}.parquet"
        target_path = root_path / "raw_market_parquet" / f"symbol={symbol}" / target_name
        return StorageAsset(
            root_name="market_root",
            source_path=source_path,
            relative_path=str(relative).replace("\\", "/"),
            source_format=source_format,
            dataset_role="raw_market_source",
            status="planned",
            action="convert",
            target_path=str(target_path),
            archive_path=str((archive_root / relative).resolve()),
            reason="Raw market source is mirrored into canonical Parquet storage without overwriting other feeds.",
            compression="zstd",
            partitioning="none",
        )
    if top_level in _MARKET_DUPLICATE_BUCKETS and source_path.suffix.lower() == ".csv":
        target_path = out_dir / "duplicate_parity" / relative.with_suffix(".parquet")
        return StorageAsset(
            root_name="market_root",
            source_path=source_path,
            relative_path=str(relative).replace("\\", "/"),
            source_format="csv",
            dataset_role="duplicate_normalized_ohlcv",
            status="planned",
            action="convert",
            target_path=str(target_path.resolve()),
            archive_path=str((archive_root / relative).resolve()),
            reason="Duplicate normalized OHLCV is converted only for parity verification before archive.",
            compression="zstd",
            partitioning="none",
        )
    return _skip_asset(
        root_name="market_root",
        source_path=source_path,
        relative=relative,
        reason="File is outside the approved market-source migration buckets.",
    )


def _skip_asset(*, root_name: str, source_path: Path, relative: Path, reason: str) -> StorageAsset:
    return StorageAsset(
        root_name=root_name,
        source_path=source_path,
        relative_path=str(relative).replace("\\", "/"),
        source_format=source_path.suffix.lower().lstrip("."),
        dataset_role="ignored",
        status="ignored",
        action="skip",
        target_path=None,
        archive_path=None,
        reason=reason,
        compression=None,
        partitioning=None,
    )


def _build_inventory_payload(*, roots: StorageRoots, assets: list[StorageAsset]) -> dict[str, Any]:
    by_role: dict[str, int] = {}
    by_action: dict[str, int] = {}
    for asset in assets:
        by_role[asset.dataset_role] = by_role.get(asset.dataset_role, 0) + 1
        by_action[asset.action] = by_action.get(asset.action, 0) + 1

    return {
        "generated_at": _utc_now(),
        "roots": {
            "market_root": str(roots.market_root),
            "corpus_root": str(roots.corpus_root),
            "working_root": str(roots.working_root),
        },
        "summary": {
            "asset_count": len(assets),
            "by_role": dict(sorted(by_role.items())),
            "by_action": dict(sorted(by_action.items())),
        },
        "assets": [_asset_to_json(asset) for asset in assets],
    }


def _build_migration_plan(assets: list[StorageAsset]) -> dict[str, Any]:
    planned = [_asset_to_json(asset) for asset in assets if asset.action != "skip"]
    return {
        "generated_at": _utc_now(),
        "compression": "zstd",
        "planned_count": len(planned),
        "planned_actions": planned,
    }


def _asset_to_json(asset: StorageAsset) -> dict[str, Any]:
    payload = asdict(asset)
    payload["source_path"] = str(asset.source_path)
    return payload


def _write_inventory_csv(path: Path, assets: list[StorageAsset]) -> None:
    rows = [_asset_to_json(asset) for asset in assets]
    pd.DataFrame(rows).to_csv(path, index=False, lineterminator="\n")


def _render_audit_report(
    *,
    roots: StorageRoots,
    inventory: dict[str, Any],
    plan: dict[str, Any],
) -> str:
    summary = inventory["summary"]
    lines = [
        "# Parquet Migration Audit",
        "",
        f"Generated: {inventory['generated_at']}",
        "",
        "## Roots",
        f"- market_root: {roots.market_root}",
        f"- corpus_root: {roots.corpus_root}",
        f"- working_root: {roots.working_root}",
        "",
        "## Summary",
        f"- Assets scanned: {summary['asset_count']}",
        f"- Planned conversions: {plan['planned_count']}",
        f"- Skipped/control files: {summary['by_action'].get('skip', 0)}",
        "",
        "## Planned roles",
    ]
    for role, count in summary["by_role"].items():
        lines.append(f"- {role}: {count}")
    lines.append("")
    return "\n".join(lines)


def _render_conversion_report(*, manifest: dict[str, Any], parity: dict[str, Any]) -> str:
    summary = parity["summary"]
    lines = [
        "# Parquet Migration Report",
        "",
        f"Generated: {manifest['generated_at']}",
        f"- Apply mode: {manifest['apply']}",
        f"- Planned/applied entries: {sum(1 for entry in manifest['entries'] if entry['action'] != 'skip')}",
        f"- Skipped entries: {sum(1 for entry in manifest['entries'] if entry['action'] == 'skip')}",
        "",
        "## Parity",
        f"- Checked: {summary['checked']}",
        f"- Passed: {summary['passed']}",
        f"- Failed: {summary['failed']}",
        "",
    ]
    return "\n".join(lines)


def _build_migration_payloads(
    *,
    manifest_entries: dict[str, dict[str, Any]],
    rollback_entries: dict[str, dict[str, Any]],
    parity_entries: dict[str, dict[str, Any]],
    apply_changes: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    ordered_manifest_entries = [manifest_entries[key] for key in sorted(manifest_entries)]
    ordered_rollback_entries = [rollback_entries[key] for key in sorted(rollback_entries)]
    ordered_parity_entries = [parity_entries[key] for key in sorted(parity_entries)]
    parity_payload = {
        "generated_at": _utc_now(),
        "apply": bool(apply_changes),
        "summary": {
            "checked": len(ordered_parity_entries),
            "passed": sum(1 for entry in ordered_parity_entries if entry["parity_passed"]),
            "failed": sum(1 for entry in ordered_parity_entries if not entry["parity_passed"]),
        },
        "entries": ordered_parity_entries,
    }
    manifest_payload = {
        "generated_at": _utc_now(),
        "apply": bool(apply_changes),
        "compression": "zstd",
        "entries": ordered_manifest_entries,
    }
    rollback_payload = {
        "generated_at": _utc_now(),
        "apply": bool(apply_changes),
        "entries": ordered_rollback_entries,
    }
    return manifest_payload, rollback_payload, parity_payload


def _build_rollback_entry(*, asset: StorageAsset, applied: bool) -> dict[str, Any]:
    return {
        "source_path": str(asset.source_path),
        "target_path": asset.target_path,
        "archive_path": asset.archive_path,
        "applied": bool(applied),
    }


def _require_parquet_support() -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for storage migrate-parquet --apply") from exc


def _read_source_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    last_exception: Exception | None = None
    saw_single_column = False
    for separator in (None, ",", "\t", ";", "|"):
        read_kwargs: dict[str, Any] = {"engine": "python"}
        if separator is None:
            read_kwargs["sep"] = None
        else:
            read_kwargs["sep"] = separator
        try:
            dataframe = pd.read_csv(path, **read_kwargs)
        except (csv.Error, pd.errors.ParserError, UnicodeDecodeError, ValueError) as exc:
            last_exception = exc
            continue
        if dataframe.shape[1] > 1:
            return dataframe
        saw_single_column = True
    if saw_single_column:
        raise RuntimeError(f"Unable to determine a usable delimiter for {path}.")
    raise RuntimeError(f"Unable to read tabular source {path}.") from last_exception


def _load_checkpoint_records(path: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return records
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid checkpoint record at {path}:{line_number}") from exc
        asset_key = str(record.get("asset_key") or "")
        if not asset_key:
            raise RuntimeError(f"Checkpoint record at {path}:{line_number} is missing asset_key.")
        records[asset_key] = record
    return records


def _append_checkpoint_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")


def _write_target_dataframe(*, asset: StorageAsset, dataframe: pd.DataFrame, target_path: Path) -> None:
    normalized = _normalize_dataframe(dataframe.copy(), dataset_role=asset.dataset_role)
    if asset.partitioning == "day":
        day_column = _resolve_partition_column(normalized, desired="day")
        _write_partitioned_parquet_dataset(
            dataframe=normalized,
            target_path=target_path,
            partition_column=day_column,
        )
        return
    if asset.partitioning == "year":
        year_column = _resolve_partition_column(normalized, desired="year")
        _write_partitioned_parquet_dataset(
            dataframe=normalized,
            target_path=target_path,
            partition_column=year_column,
        )
        return

    target_path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_parquet(target_path, index=False, compression="zstd", engine="pyarrow")


def _write_partitioned_parquet_dataset(
    *,
    dataframe: pd.DataFrame,
    target_path: Path,
    partition_column: str,
) -> None:
    target_path.mkdir(parents=True, exist_ok=True)
    for partition_value, partition_frame in dataframe.groupby(partition_column, sort=True, dropna=False):
        if pd.isna(partition_value):
            raise RuntimeError(f"Partition column {partition_column} contains null values.")
        partition_dir = target_path / f"{partition_column}={partition_value}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        partition_frame.drop(columns=[partition_column]).to_parquet(
            partition_dir / "part-00000.parquet",
            index=False,
            compression="zstd",
            engine="pyarrow",
        )


def _staging_target_path(target_path: Path) -> Path:
    suffix = datetime.now(tz=UTC).strftime("%Y%m%d%H%M%S%f")
    return target_path.parent / f".{target_path.name}.staging-{suffix}"


def _replace_target(*, staged_target_path: Path, target_path: Path) -> None:
    _cleanup_path(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(staged_target_path), str(target_path))


def _cleanup_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
        return
    if path.exists():
        path.unlink(missing_ok=True)


def _resolve_partition_column(dataframe: pd.DataFrame, *, desired: str) -> str:
    if desired == "day":
        candidate = _first_present_column(dataframe, _DAY_COLUMN_CANDIDATES)
        if candidate is not None:
            normalized = _normalize_day_values(dataframe[candidate])
            if candidate == "day":
                dataframe["day"] = normalized
            else:
                dataframe["day"] = normalized
            return "day"
        raise RuntimeError("Unable to partition daily corpus data; expected a day/date column.")
    candidate = _first_present_column(dataframe, _YEAR_COLUMN_CANDIDATES)
    if candidate is not None:
        if candidate == "year":
            dataframe["year"] = pd.to_numeric(dataframe["year"], errors="raise").astype(int).astype(str)
        else:
            dataframe["year"] = pd.to_datetime(dataframe[candidate], errors="raise").dt.strftime("%Y")
        return "year"
    raise RuntimeError("Unable to partition annual corpus data; expected a year/date column.")


def _normalize_dataframe(dataframe: pd.DataFrame, *, dataset_role: str) -> pd.DataFrame:
    dataframe.columns = [_normalize_column_name(column) for column in dataframe.columns]
    for column in dataframe.columns:
        if dataframe[column].dtype == object:
            dataframe[column] = dataframe[column].map(
                lambda value: value.strip() if isinstance(value, str) else value
            )
    date_like_column = _first_present_column(dataframe, tuple(column for column in _DAY_COLUMN_CANDIDATES if column != "day"))
    if date_like_column is not None:
        dataframe[date_like_column] = _normalize_day_values(dataframe[date_like_column])
    if "day" in dataframe.columns:
        dataframe["day"] = _normalize_day_values(dataframe["day"])
        if date_like_column is not None and dataframe["day"].equals(dataframe[date_like_column]):
            dataframe = dataframe.drop(columns=["day"])
    if "year" in dataframe.columns and dataframe["year"].dtype == object:
        dataframe["year"] = dataframe["year"].astype(str)
    if dataset_role in {"canonical_normalized_ohlcv", "duplicate_normalized_ohlcv"}:
        required = ["date", "open", "high", "low", "close", "volume"]
        missing = [column for column in required if column not in dataframe.columns]
        if missing:
            raise RuntimeError(
                f"Normalized OHLCV file is missing required columns {missing}."
            )
        dataframe = dataframe[required]
    return dataframe


def _check_parity(*, asset: StorageAsset, source_df: pd.DataFrame, target_path: Path) -> dict[str, Any]:
    source_norm = _normalize_dataframe(source_df.copy(), dataset_role=asset.dataset_role)
    target_norm = _normalize_dataframe(pd.read_parquet(target_path).copy(), dataset_role=asset.dataset_role)
    source_sorted = _sort_dataframe(source_norm)
    target_sorted = _sort_dataframe(target_norm)
    column_set_equal = set(source_sorted.columns) == set(target_sorted.columns)
    if column_set_equal:
        ordered_columns = sorted(source_sorted.columns)
        source_compare = source_sorted.loc[:, ordered_columns]
        target_compare = target_sorted.loc[:, ordered_columns]
    else:
        source_compare = source_sorted
        target_compare = target_sorted
    numeric_ok = _numeric_columns_match(source_compare, target_compare)
    non_numeric_ok = _non_numeric_columns_match(source_compare, target_compare)
    date_min = None
    date_max = None
    if "date" in source_sorted.columns and not source_sorted.empty:
        date_min = str(source_sorted["date"].min())
        date_max = str(source_sorted["date"].max())
    symbol_coverage_equal = True
    if "symbol" in source_sorted.columns:
        symbol_coverage_equal = sorted(source_sorted["symbol"].dropna().astype(str).unique()) == sorted(
            target_sorted["symbol"].dropna().astype(str).unique()
        )
    return {
        "source_path": str(asset.source_path),
        "target_path": str(target_path),
        "row_count_equal": int(len(source_sorted)) == int(len(target_sorted)),
        "min_date_equal": date_min == (str(target_sorted["date"].min()) if "date" in target_sorted.columns and not target_sorted.empty else None),
        "max_date_equal": date_max == (str(target_sorted["date"].max()) if "date" in target_sorted.columns and not target_sorted.empty else None),
        "symbol_coverage_equal": bool(symbol_coverage_equal),
        "column_set_equal": bool(column_set_equal),
        "numeric_equal": bool(numeric_ok),
        "non_numeric_equal": bool(non_numeric_ok),
        "parity_passed": bool(
            int(len(source_sorted)) == int(len(target_sorted))
            and column_set_equal
            and numeric_ok
            and non_numeric_ok
            and symbol_coverage_equal
        ),
    }


def _sort_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.reset_index(drop=True)
    ordered_columns = sorted(dataframe.columns)
    sortable = pd.DataFrame(
        {
            column: dataframe[column].map(lambda value: "" if pd.isna(value) else str(value))
            for column in ordered_columns
        }
    )
    order = sortable.agg("||".join, axis=1).sort_values(kind="mergesort").index
    return dataframe.loc[order].reset_index(drop=True)


def _numeric_columns_match(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    numeric_columns = [
        column
        for column in left.columns
        if pd.api.types.is_numeric_dtype(left[column]) and pd.api.types.is_numeric_dtype(right[column])
    ]
    for column in numeric_columns:
        left_values = pd.to_numeric(left[column], errors="coerce").to_numpy(dtype=float)
        right_values = pd.to_numeric(right[column], errors="coerce").to_numpy(dtype=float)
        if not np.allclose(left_values, right_values, rtol=1e-9, atol=1e-9, equal_nan=True):
            return False
    return True


def _non_numeric_columns_match(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    for column in left.columns:
        if pd.api.types.is_numeric_dtype(left[column]) and pd.api.types.is_numeric_dtype(right[column]):
            continue
        left_values = left[column].map(lambda value: "" if pd.isna(value) else str(value)).tolist()
        right_values = right[column].map(lambda value: "" if pd.isna(value) else str(value)).tolist()
        if left_values != right_values:
            return False
    return True


def _hash_target(target_path: Path) -> str | None:
    if target_path.is_file():
        return hash_file(target_path)
    if target_path.is_dir():
        hashes: list[str] = []
        for file_path in sorted(path for path in target_path.rglob("*.parquet") if path.is_file()):
            hashes.append(hash_file(file_path))
        return "|".join(hashes) if hashes else None
    return None


def _normalize_column_name(value: Any) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z]+", "_", str(value).strip()).strip("_").lower()
    return normalized or "column"


def _first_present_column(dataframe: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in dataframe.columns:
            return candidate
    return None


def _normalize_day_values(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="raise").dt.strftime("%Y-%m-%d")


def _symbol_from_filename(path: Path) -> str:
    symbol = re.sub(r"[^0-9A-Za-z._-]+", "_", path.stem).strip("._-").upper()
    return symbol or "UNKNOWN"


def _relative_source_slug(relative: Path) -> str:
    parts = list(relative.parts)
    if not parts:
        return "source"
    parts[-1] = Path(parts[-1]).stem
    slug = "__".join(_slugify_path_part(part) for part in parts if part)
    return slug or "source"


def _slugify_path_part(value: str) -> str:
    slug = re.sub(r"[^0-9A-Za-z]+", "_", value.strip()).strip("_").lower()
    return slug or "part"


def _asset_key(source_path: Path | str) -> str:
    return str(source_path)


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8", newline="\n")
