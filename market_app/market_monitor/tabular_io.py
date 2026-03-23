from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

PREFERRED_DATA_SUFFIXES: tuple[str, ...] = (".parquet", ".csv")
READABLE_TABULAR_SUFFIXES: tuple[str, ...] = (".parquet", ".csv", ".txt")


def normalize_symbol_key(value: str) -> str:
    return str(value).replace("-", "").replace(".", "").replace("/", "").strip().upper()


def symbol_variants(symbol: str) -> list[str]:
    base = str(symbol).strip()
    if not base:
        return []
    variants = {
        base,
        base.upper(),
        base.lower(),
        base.replace(".", "-"),
        base.replace("-", "."),
        base.replace("/", "-"),
        base.replace("/", "."),
        base.replace(".", ""),
        base.replace("-", ""),
    }
    return sorted(value for value in variants if value)


def read_tabular(
    path: Path,
    *,
    auto_sep: bool = False,
    usecols: Iterable[str] | None = None,
) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path, columns=list(usecols) if usecols is not None else None)
    if suffix in {".csv", ".txt"}:
        kwargs: dict[str, object] = {}
        if usecols is not None:
            kwargs["usecols"] = list(usecols)
        if auto_sep:
            kwargs["sep"] = None
            kwargs["engine"] = "python"
        return pd.read_csv(path, **kwargs)
    raise ValueError(f"Unsupported tabular file type: {path}")


def write_tabular(
    frame: pd.DataFrame,
    path: Path,
    *,
    parquet_compression: str = "zstd",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        frame.to_parquet(path, index=False, compression=parquet_compression)
        return
    if suffix == ".csv":
        frame.to_csv(path, index=False, lineterminator="\n")
        return
    raise ValueError(f"Unsupported tabular output type: {path}")


def resolve_named_table_path(directory: Path, stems: Iterable[str]) -> Path | None:
    for stem in stems:
        for suffix in PREFERRED_DATA_SUFFIXES:
            candidate = directory / f"{stem}{suffix}"
            if candidate.exists():
                return candidate
    return None


def resolve_partition_part_path(directory: Path, partition_name: str) -> Path | None:
    partition_dir = directory / partition_name
    if not partition_dir.exists() or not partition_dir.is_dir():
        return None
    for suffix in PREFERRED_DATA_SUFFIXES:
        candidate = partition_dir / f"part-00000{suffix}"
        if candidate.exists():
            return candidate
    for suffix in PREFERRED_DATA_SUFFIXES:
        candidates = sorted(path for path in partition_dir.glob(f"*{suffix}") if path.is_file())
        if candidates:
            return candidates[0]
    return None


def resolve_symbol_table_path(
    directory: Path,
    symbol: str,
    *,
    suffixes: tuple[str, ...] = PREFERRED_DATA_SUFFIXES,
) -> Path | None:
    variants = symbol_variants(symbol)
    for variant in variants:
        for suffix in suffixes:
            candidate = directory / f"{variant}{suffix}"
            if candidate.exists():
                return candidate
            partition_candidate = directory / f"symbol={variant}" / f"part-00000{suffix}"
            if partition_candidate.exists():
                return partition_candidate

    target = normalize_symbol_key(symbol)
    best: tuple[int, str, Path] | None = None
    for path in directory.iterdir():
        if path.is_dir() and path.name.startswith("symbol="):
            symbol_name = path.name.split("=", 1)[1]
            part = resolve_partition_part_path(directory, path.name)
            if part is None:
                continue
            suffix_rank = suffixes.index(part.suffix.lower()) if part.suffix.lower() in suffixes else len(suffixes)
            candidate_key = normalize_symbol_key(symbol_name)
            if candidate_key == target:
                rank = (suffix_rank, part.name.lower(), part)
                if best is None or rank < best:
                    best = rank
            continue
        if not path.is_file():
            continue
        if path.suffix.lower() not in suffixes:
            continue
        candidate_key = normalize_symbol_key(path.stem)
        if candidate_key != target:
            continue
        suffix_rank = suffixes.index(path.suffix.lower()) if path.suffix.lower() in suffixes else len(suffixes)
        rank = (suffix_rank, path.name.lower(), path)
        if best is None or rank < best:
            best = rank
    return best[2] if best else None


def list_symbol_table_paths(
    directory: Path,
    *,
    suffixes: tuple[str, ...] = PREFERRED_DATA_SUFFIXES,
    skip_stems: set[str] | None = None,
) -> list[Path]:
    skip = {value.lower() for value in (skip_stems or set())}
    selected: dict[str, tuple[int, str, Path]] = {}

    for path in directory.iterdir():
        if path.is_file():
            suffix = path.suffix.lower()
            if suffix not in suffixes:
                continue
            if path.stem.lower() in skip:
                continue
            key = normalize_symbol_key(path.stem)
            suffix_rank = suffixes.index(suffix)
            rank = (suffix_rank, path.name.lower(), path)
            current = selected.get(key)
            if current is None or rank < current:
                selected[key] = rank
            continue

        if path.is_dir() and path.name.startswith("symbol="):
            symbol_name = path.name.split("=", 1)[1]
            part = resolve_partition_part_path(directory, path.name)
            if part is None:
                continue
            suffix = part.suffix.lower()
            if suffix not in suffixes:
                continue
            key = normalize_symbol_key(symbol_name)
            suffix_rank = suffixes.index(suffix)
            rank = (suffix_rank, part.as_posix().lower(), part)
            current = selected.get(key)
            if current is None or rank < current:
                selected[key] = rank

    return [entry[2] for entry in sorted(selected.values(), key=lambda item: item[:2])]
