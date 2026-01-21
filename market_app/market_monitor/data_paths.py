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


def _normalize_path(path_str: str | None, *, root: Path | None, repo_root: Path) -> Path | None:
    if not path_str:
        return None
    expanded = Path(os.path.expandvars(os.path.expanduser(path_str)))
    if expanded.is_absolute():
        return expanded
    if root is not None:
        return (root / expanded).resolve()
    return (repo_root / expanded).resolve()


def resolve_data_paths(config: dict, repo_root: Path) -> DataPaths:
    paths_cfg = config.get("data", {}).get("paths", {})
    root_str = paths_cfg.get("market_app_data_root")
    root = _normalize_path(root_str, root=None, repo_root=repo_root) if root_str else None
    nasdaq = _normalize_path(paths_cfg.get("nasdaq_daily_dir"), root=root, repo_root=repo_root)
    silver_setting = paths_cfg.get("silver_prices_dir") or paths_cfg.get("silver_prices_csv")
    silver = _normalize_path(silver_setting, root=root, repo_root=repo_root)
    return DataPaths(
        market_app_data_root=root,
        nasdaq_daily_dir=nasdaq,
        silver_prices_dir=silver,
    )


def resolve_corpus_paths(config: dict, repo_root: Path) -> CorpusPaths:
    corpus_cfg = config.get("corpus", {})
    root_str = corpus_cfg.get("root_dir")
    root = _normalize_path(root_str, root=None, repo_root=repo_root) if root_str else None
    gdelt_dir = corpus_cfg.get("gdelt_conflict_dir")
    gdelt = _normalize_path(gdelt_dir, root=root, repo_root=repo_root)
    if gdelt is None and root is not None:
        gdelt = root
    return CorpusPaths(root_dir=root, gdelt_conflict_dir=gdelt)
