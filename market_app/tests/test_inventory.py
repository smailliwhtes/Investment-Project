from __future__ import annotations

import csv
from pathlib import Path

from market_monitor.tools.inventory import InventoryConfig, build_inventory, write_inventory


def test_inventory_writes_outputs(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "config").mkdir()
    (repo_root / "config" / "universe_required.csv").write_text(
        "symbol,notes\nAAA,Test\n", encoding="utf-8"
    )
    stooq_root = repo_root / "stooq"
    (stooq_root / "nasdaq stocks" / "1").mkdir(parents=True)
    (stooq_root / "nasdaq stocks" / "1" / "AAA.us.txt").write_text(
        "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>\n"
        "AAA.US,D,20240103,000000,1,1,1,1,0,0\n",
        encoding="utf-8",
    )

    security_master = repo_root / "out" / "security_master.csv"
    security_master.parent.mkdir(parents=True)
    with security_master.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "symbol",
                "symbol_us",
                "name",
                "exchange",
                "is_etf",
                "cik",
                "sic",
                "sector_bucket",
                "source_bucket",
                "ohlcv_path",
                "asof_date",
            ]
        )

    config = InventoryConfig(
        repo_root=repo_root,
        stooq_root=stooq_root,
        metastock_root=None,
        corpus_root=None,
        output_dir=repo_root / "out",
    )
    payload = write_inventory(config)
    assert payload["stooq_txt"]["exists"] is True
    assert payload["stooq_txt"]["buckets"]["nasdaq stocks/1"] == 1

    assert (repo_root / "out" / "inventory.json").exists()
    assert (repo_root / "out" / "inventory.md").exists()


def test_inventory_schema_validation(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "config").mkdir()
    (repo_root / "config" / "universe_required.csv").write_text(
        "symbol,notes\nAAA,Test\n", encoding="utf-8"
    )
    security_master = repo_root / "out" / "security_master.csv"
    security_master.parent.mkdir(parents=True)
    security_master.write_text("symbol,symbol_us\n", encoding="utf-8")

    config = InventoryConfig(
        repo_root=repo_root,
        stooq_root=None,
        metastock_root=None,
        corpus_root=None,
        output_dir=repo_root / "out",
    )
    payload = build_inventory(config)
    assert payload["security_master"]["exists"] is True
    assert payload["security_master"]["schema_ok"] is False
