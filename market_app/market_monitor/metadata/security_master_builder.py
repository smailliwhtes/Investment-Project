from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from market_monitor.metadata.security_master import (
    SecurityMasterConfig,
    SecurityMasterRecord,
    build_security_master,
)


@dataclass(frozen=True)
class SecurityMasterBuildConfig:
    stooq_root: Path
    output_path: Path
    required_symbols_path: Path | None = None
    filter_required: bool = False
    path_mode: str = "auto"
    repo_root: Path | None = None
    asof_date: date | None = None


def build_security_master_minimal(config: SecurityMasterBuildConfig) -> list[SecurityMasterRecord]:
    bridge = SecurityMasterConfig(
        stooq_root=config.stooq_root,
        output_path=config.output_path,
        required_symbols_path=config.required_symbols_path,
        filter_required=config.filter_required,
        path_mode=config.path_mode,
        repo_root=config.repo_root,
        asof_date=config.asof_date,
    )
    return build_security_master(bridge)
