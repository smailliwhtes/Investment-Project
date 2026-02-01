from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    market_app_data_root: Path | None
    nasdaq_daily_dir: Path | None
    silver_prices_dir: Path | None


@dataclass(frozen=True)
class CorpusPaths:
    root_dir: Path | None
    gdelt_conflict_dir: Path | None
    gdelt_events_raw_dir: Path | None


def _normalize_path(path_str: str | None, *, root: Path | None, base_dir: Path) -> Path | None:
    if not path_str:
        return None
    expanded = Path(os.path.expandvars(os.path.expanduser(path_str)))
    if expanded.is_absolute():
        return expanded
    if root is not None:
        return (root / expanded).resolve()
    return (base_dir / expanded).resolve()


def resolve_data_paths(config: dict, base_dir: Path) -> DataPaths:
    data_roots = config.get("data_roots", {})
    ohlcv_root = data_roots.get("ohlcv_dir")
    paths_cfg = config.get("data", {}).get("paths", {})
    root_str = paths_cfg.get("market_app_data_root")
    root = _normalize_path(root_str, root=None, base_dir=base_dir) if root_str else None
    if ohlcv_root:
        nasdaq = _normalize_path(ohlcv_root, root=None, base_dir=base_dir)
    else:
        nasdaq = _normalize_path(paths_cfg.get("nasdaq_daily_dir"), root=root, base_dir=base_dir)
    silver_setting = paths_cfg.get("silver_prices_dir") or paths_cfg.get("silver_prices_csv")
    silver = _normalize_path(silver_setting, root=root, base_dir=base_dir)
    return DataPaths(
        market_app_data_root=root,
        nasdaq_daily_dir=nasdaq,
        silver_prices_dir=silver,
    )


def resolve_corpus_paths(config: dict, base_dir: Path) -> CorpusPaths:
    corpus_cfg = config.get("corpus", {})
    data_roots = config.get("data_roots", {})
    root_str = corpus_cfg.get("root_dir")
    root = _normalize_path(root_str, root=None, base_dir=base_dir) if root_str else None
    gdelt_dir = corpus_cfg.get("gdelt_conflict_dir")
    gdelt = _normalize_path(gdelt_dir, root=root, base_dir=base_dir)
    raw_dir = data_roots.get("gdelt_raw_dir") or corpus_cfg.get("gdelt_events_raw_dir")
    gdelt_raw = _normalize_path(raw_dir, root=root, base_dir=base_dir)
    if gdelt is None and root is not None:
        gdelt = root
    if gdelt_raw is None and root is not None:
        gdelt_raw = (root / "gdelt_events_raw").resolve()
    return CorpusPaths(root_dir=root, gdelt_conflict_dir=gdelt, gdelt_events_raw_dir=gdelt_raw)
