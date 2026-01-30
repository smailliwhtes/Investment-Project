from __future__ import annotations

import csv
from pathlib import Path

from market_monitor.metadata.security_master import (
    SECURITY_MASTER_COLUMNS,
    SecurityMasterConfig,
    build_security_master,
)


def test_security_master_minimal_schema(tmp_path: Path) -> None:
    stooq_root = Path(__file__).resolve().parent / "fixtures" / "stooq_txt"
    output_path = tmp_path / "out" / "security_master.csv"
    config = SecurityMasterConfig(
        stooq_root=stooq_root,
        output_path=output_path,
        filter_required=False,
        path_mode="relative",
        repo_root=tmp_path,
    )
    build_security_master(config)

    with output_path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = list(reader)

    assert header == SECURITY_MASTER_COLUMNS
    assert rows


def test_security_master_required_filter(tmp_path: Path) -> None:
    stooq_root = Path(__file__).resolve().parent / "fixtures" / "stooq_txt"
    required_path = tmp_path / "required.csv"
    required_path.write_text("symbol\nAAA\n", encoding="utf-8")
    output_path = tmp_path / "out" / "security_master.csv"
    config = SecurityMasterConfig(
        stooq_root=stooq_root,
        output_path=output_path,
        required_symbols_path=required_path,
        filter_required=True,
        path_mode="relative",
        repo_root=tmp_path,
    )
    records = build_security_master(config)
    assert len(records) == 1
    assert records[0].symbol == "AAA"
