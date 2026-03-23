from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd


_DELIMITED_SUFFIXES: tuple[str, ...] = (".csv", ".tsv", ".tab", ".psv", ".txt")
_JSON_SUFFIXES: tuple[str, ...] = (".json", ".jsonl", ".ndjson")
_PARQUET_SUFFIX: str = ".parquet"
_DEFAULT_COMPRESSION = "zstd"
_MANIFEST_FILENAME = "folder_conversion_manifest.json"
_INVENTORY_FILENAME = "folder_conversion_inventory.csv"
_REPORT_FILENAME = "folder_conversion_report.md"


@dataclass(frozen=True)
class FolderConversionEntry:
    source_path: str
    relative_path: str
    target_path: str | None
    source_format: str
    status: str
    reason: str
    rows: int | None
    columns: list[str]


def convert_folder_to_parquet(
    *,
    source_root: Path,
    out_dir: Path | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    _require_parquet_support()

    source_root = source_root.expanduser().resolve()
    if not source_root.exists():
        raise FileNotFoundError(f"Missing source root: {source_root}")
    if not source_root.is_dir():
        raise ValueError(f"Source root must be a directory: {source_root}")

    resolved_out_dir = (
        out_dir.expanduser().resolve()
        if out_dir is not None
        else source_root.parent / f"{source_root.name}_parquet"
    )
    resolved_out_dir = resolved_out_dir.expanduser().resolve()

    if resolved_out_dir == source_root or resolved_out_dir.is_relative_to(source_root):
        raise ValueError(
            "Output directory must be outside the source root to avoid recursive self-conversion."
        )

    source_files = sorted(path for path in source_root.rglob("*") if path.is_file())
    target_map = _plan_target_paths(source_root=source_root, out_dir=resolved_out_dir, files=source_files)
    entries: list[FolderConversionEntry] = []
    strict_failures: list[str] = []
    conversion_errors: list[str] = []

    for source_path in source_files:
        relative = source_path.relative_to(source_root)
        relative_text = relative.as_posix()
        suffix = source_path.suffix.lower()

        if suffix not in _supported_suffixes():
            reason = f"Unsupported file type: {suffix or '<no suffix>'}"
            entries.append(
                FolderConversionEntry(
                    source_path=str(source_path),
                    relative_path=relative_text,
                    target_path=None,
                    source_format=suffix.lstrip("."),
                    status="skipped",
                    reason=reason,
                    rows=None,
                    columns=[],
                )
            )
            if strict:
                strict_failures.append(relative_text)
            continue

        if source_path.stat().st_size == 0:
            reason = "Empty file cannot be materialized as Parquet without schema."
            entries.append(
                FolderConversionEntry(
                    source_path=str(source_path),
                    relative_path=relative_text,
                    target_path=None,
                    source_format=suffix.lstrip("."),
                    status="skipped",
                    reason=reason,
                    rows=None,
                    columns=[],
                )
            )
            if strict:
                strict_failures.append(relative_text)
            continue

        target_path = target_map[source_path]
        try:
            frame = _read_source_dataframe(source_path)
            if len(frame.columns) == 0:
                raise RuntimeError("No tabular columns were discovered in the source file.")

            target_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(
                target_path,
                index=False,
                compression=_DEFAULT_COMPRESSION,
                engine="pyarrow",
            )
            entries.append(
                FolderConversionEntry(
                    source_path=str(source_path),
                    relative_path=relative_text,
                    target_path=str(target_path),
                    source_format=suffix.lstrip("."),
                    status="converted",
                    reason="Converted to Parquet.",
                    rows=int(len(frame)),
                    columns=[str(column) for column in frame.columns],
                )
            )
        except Exception as exc:
            message = f"{relative_text}: {exc}"
            conversion_errors.append(message)
            entries.append(
                FolderConversionEntry(
                    source_path=str(source_path),
                    relative_path=relative_text,
                    target_path=str(target_path),
                    source_format=suffix.lstrip("."),
                    status="error",
                    reason=str(exc),
                    rows=None,
                    columns=[],
                )
            )

    manifest = _build_manifest(
        source_root=source_root,
        out_dir=resolved_out_dir,
        entries=entries,
        strict=strict,
    )
    _write_manifest_files(out_dir=resolved_out_dir, manifest=manifest, entries=entries)

    if conversion_errors:
        raise RuntimeError(
            f"Failed to convert {len(conversion_errors)} file(s). "
            f"See {resolved_out_dir / _MANIFEST_FILENAME} for details."
        )
    if strict_failures:
        raise RuntimeError(
            f"Strict mode blocked conversion because {len(strict_failures)} file(s) were skipped. "
            f"See {resolved_out_dir / _MANIFEST_FILENAME} for details."
        )

    return manifest


def _build_manifest(
    *,
    source_root: Path,
    out_dir: Path,
    entries: list[FolderConversionEntry],
    strict: bool,
) -> dict[str, Any]:
    summary = {
        "scanned": len(entries),
        "converted": sum(1 for entry in entries if entry.status == "converted"),
        "skipped": sum(1 for entry in entries if entry.status == "skipped"),
        "errors": sum(1 for entry in entries if entry.status == "error"),
    }
    return {
        "source_root": str(source_root),
        "out_dir": str(out_dir),
        "strict": bool(strict),
        "compression": _DEFAULT_COMPRESSION,
        "supported_suffixes": sorted(_supported_suffixes()),
        "summary": summary,
        "manifest_path": str(out_dir / _MANIFEST_FILENAME),
        "inventory_csv_path": str(out_dir / _INVENTORY_FILENAME),
        "report_path": str(out_dir / _REPORT_FILENAME),
        "entries": [asdict(entry) for entry in entries],
    }


def _write_manifest_files(
    *,
    out_dir: Path,
    manifest: dict[str, Any],
    entries: list[FolderConversionEntry],
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / _MANIFEST_FILENAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
        newline="\n",
    )
    pd.DataFrame([asdict(entry) for entry in entries]).to_csv(
        out_dir / _INVENTORY_FILENAME,
        index=False,
        lineterminator="\n",
    )
    (out_dir / _REPORT_FILENAME).write_text(
        _render_report(manifest),
        encoding="utf-8",
        newline="\n",
    )


def _render_report(manifest: dict[str, Any]) -> str:
    summary = manifest["summary"]
    lines = [
        "# Folder To Parquet Conversion Report",
        "",
        f"Source root: {manifest['source_root']}",
        f"Output dir: {manifest['out_dir']}",
        f"Strict mode: {manifest['strict']}",
        f"Compression: {manifest['compression']}",
        "",
        "## Summary",
        f"- Files scanned: {summary['scanned']}",
        f"- Files converted: {summary['converted']}",
        f"- Files skipped: {summary['skipped']}",
        f"- Files with errors: {summary['errors']}",
        "",
        "## Supported suffixes",
    ]
    for suffix in manifest["supported_suffixes"]:
        lines.append(f"- {suffix}")
    lines.append("")
    return "\n".join(lines)


def _plan_target_paths(*, source_root: Path, out_dir: Path, files: list[Path]) -> dict[Path, Path]:
    desired: dict[Path, Path] = {}
    reverse: dict[Path, list[Path]] = {}

    for source_path in files:
        suffix = source_path.suffix.lower()
        if suffix not in _supported_suffixes():
            continue
        relative = source_path.relative_to(source_root)
        if suffix == _PARQUET_SUFFIX:
            target_relative = relative
        else:
            target_relative = relative.with_suffix(_PARQUET_SUFFIX)
        target_path = out_dir / target_relative
        desired[source_path] = target_path
        reverse.setdefault(target_path, []).append(source_path)

    resolved: dict[Path, Path] = {}
    for target_path, sources in reverse.items():
        if len(sources) == 1:
            resolved[sources[0]] = target_path
            continue
        preserved_source = next(
            (source_path for source_path in sorted(sources) if source_path.suffix.lower() == _PARQUET_SUFFIX),
            None,
        )
        if preserved_source is not None:
            resolved[preserved_source] = target_path
        for source_path in sorted(sources):
            if source_path == preserved_source:
                continue
            relative = source_path.relative_to(source_root)
            suffix_slug = source_path.suffix.lower().lstrip(".") or "file"
            adjusted_name = f"{source_path.stem}__{suffix_slug}{_PARQUET_SUFFIX}"
            resolved[source_path] = out_dir / relative.with_name(adjusted_name)
    return resolved


def _read_source_dataframe(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == _PARQUET_SUFFIX:
        return pd.read_parquet(path)
    if suffix in _JSON_SUFFIXES:
        return _read_json_dataframe(path)
    if suffix in _DELIMITED_SUFFIXES:
        return _read_delimited_dataframe(path)
    raise RuntimeError(f"Unsupported tabular source: {path}")


def _read_json_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        return pd.read_json(path, lines=True)

    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise RuntimeError("JSON file is empty.")

    payload = json.loads(raw)
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        if payload and all(isinstance(value, list) for value in payload.values()):
            return pd.DataFrame(payload)
        return pd.json_normalize(payload)
    raise RuntimeError("JSON payload is not a record object or record list.")


def _read_delimited_dataframe(path: Path) -> pd.DataFrame:
    explicit_separators = {
        ".csv": [","],
        ".tsv": ["\t"],
        ".tab": ["\t"],
        ".psv": ["|"],
    }.get(path.suffix.lower(), [])

    attempted: list[str] = []
    for separator in explicit_separators + [None, ",", "\t", ";", "|"]:
        if separator in attempted:
            continue
        attempted.append(separator)
        kwargs: dict[str, Any] = {}
        if separator is None:
            kwargs["sep"] = None
            kwargs["engine"] = "python"
        else:
            kwargs["sep"] = separator
        try:
            frame = pd.read_csv(path, **kwargs)
        except (csv.Error, pd.errors.EmptyDataError, pd.errors.ParserError, UnicodeDecodeError, ValueError):
            continue
        if path.suffix.lower() == ".txt" and len(frame.columns) <= 1:
            continue
        return frame

    raise RuntimeError("Unable to read a supported tabular structure from the file.")


def _supported_suffixes() -> set[str]:
    return set(_DELIMITED_SUFFIXES) | set(_JSON_SUFFIXES) | {_PARQUET_SUFFIX}


def _require_parquet_support() -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for folder-to-Parquet conversion.") from exc
