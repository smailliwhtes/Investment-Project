from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

import market_monitor.storage_parquet as storage_parquet
from market_monitor.storage_parquet import (
    StorageAsset,
    _append_checkpoint_record,
    _read_source_dataframe,
    _sort_dataframe,
    _symbol_from_filename,
    _write_target_dataframe,
    migrate_parquet_storage,
)


def _write_csv(path: Path, dataframe: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False, lineterminator="\n")


def _build_storage_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    market_root = tmp_path / "Market_Files"
    corpus_root = tmp_path / "NLP Corpus"
    working_root = tmp_path / "Working CSV Files"

    _write_csv(
        market_root / "New Daily Files" / "AAPL.us.txt",
        pd.DataFrame(
            {
                "Date": ["2025-01-02", "2025-01-03"],
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [99.0, 100.0],
                "Close": [101.0, 102.0],
                "Volume": [1_000_000, 1_100_000],
            }
        ),
    )
    _write_csv(
        market_root / "All Files Together" / "AAPL.us.txt",
        pd.DataFrame(
            {
                "Date": ["2025-01-02"],
                "Open": [99.5],
                "High": [101.5],
                "Low": [98.5],
                "Close": [100.5],
                "Volume": [950_000],
            }
        ),
    )
    _write_csv(
        market_root / "ohlcv_daily_csv" / "AAPL.csv",
        pd.DataFrame(
            {
                "date": ["2025-01-02", "2025-01-03"],
                "open": [100.0, 101.0],
                "high": [102.0, 103.0],
                "low": [99.0, 100.0],
                "close": [101.0, 102.0],
                "volume": [1_000_000, 1_100_000],
            }
        ),
    )
    _write_csv(
        market_root / "_nasdaq_daily_flat_norm" / "MSFT.csv",
        pd.DataFrame(
            {
                "date": ["2025-01-02"],
                "open": [200.0],
                "high": [202.0],
                "low": [199.0],
                "close": [201.0],
                "volume": [2_000_000],
            }
        ),
    )
    _write_csv(
        corpus_root / "GDELT_Data_1.csv",
        pd.DataFrame({"dt": ["2025-01-02", "2025-01-03"], "signal": [1.2, 1.4], "symbol": ["AAPL", "MSFT"]}),
    )
    _write_csv(
        corpus_root / "GDELT_Data_1_stable.csv",
        pd.DataFrame({"dt": ["2025-01-02"], "signal": [0.8], "symbol": ["AAPL"]}),
    )
    _write_csv(
        corpus_root / "gdelt_conflict_1_0.csv",
        pd.DataFrame({"year": [2024, 2025], "conflict_score": [5.0, 7.0]}),
    )
    (corpus_root / "corpus_transform_params.json").write_text("{}", encoding="utf-8")
    _write_csv(
        working_root / "AAPL.csv",
        pd.DataFrame(
            {
                "date": ["2025-01-02", "2025-01-03"],
                "open": [100.0, 101.0],
                "high": [102.0, 103.0],
                "low": [99.0, 100.0],
                "close": [101.0, 102.0],
                "volume": [1_000_000, 1_100_000],
            }
        ),
    )
    _write_csv(
        working_root / "MSFT.csv",
        pd.DataFrame(
            {
                "date": ["2025-01-02"],
                "open": [200.0],
                "high": [202.0],
                "low": [199.0],
                "close": [201.0],
                "volume": [2_000_000],
            }
        ),
    )
    _write_csv(working_root / "universe.csv", pd.DataFrame({"symbol": ["AAPL", "MSFT"]}))
    state_dir = working_root / "_preprocessor_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "gdelt_daily_join_ready.csv").write_text("\n", encoding="utf-8")
    (state_dir / "processed_registry.json").write_text("{}", encoding="utf-8")
    return market_root, corpus_root, working_root


def _run_storage_command(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "market_monitor", *args],
        check=False,
        capture_output=True,
        text=True,
    )


def test_storage_audit_parquet_writes_inventory_and_plan(tmp_path: Path) -> None:
    market_root, corpus_root, working_root = _build_storage_fixture(tmp_path)
    out_dir = tmp_path / "audit"
    result = _run_storage_command(
        "storage",
        "audit-parquet",
        "--market-root",
        str(market_root),
        "--corpus-root",
        str(corpus_root),
        "--working-root",
        str(working_root),
        "--out-dir",
        str(out_dir),
    )
    assert result.returncode == 0, result.stderr or result.stdout
    for name in ("inventory.json", "inventory.csv", "migration_plan.json", "migration_report.md"):
        assert (out_dir / name).exists(), name

    payload = json.loads(result.stdout)
    assert payload["inventory"]["summary"]["by_role"]["canonical_normalized_ohlcv"] == 2
    assert payload["inventory"]["summary"]["by_role"]["duplicate_normalized_ohlcv"] == 2
    assert payload["inventory"]["summary"]["by_role"]["raw_market_source"] == 2
    assert payload["migration_plan"]["planned_count"] == 9

    inventory_csv = pd.read_csv(out_dir / "inventory.csv")
    assert {"root_name", "dataset_role", "action", "target_path"}.issubset(inventory_csv.columns)
    assert (inventory_csv["dataset_role"] == "ignored").any()


def test_storage_audit_skips_empty_market_files(tmp_path: Path) -> None:
    market_root, corpus_root, working_root = _build_storage_fixture(tmp_path)
    empty_path = market_root / "All Files Together" / "xfix.us.txt"
    empty_path.parent.mkdir(parents=True, exist_ok=True)
    empty_path.write_text("", encoding="utf-8")
    out_dir = tmp_path / "audit_empty"

    result = _run_storage_command(
        "storage",
        "audit-parquet",
        "--market-root",
        str(market_root),
        "--corpus-root",
        str(corpus_root),
        "--working-root",
        str(working_root),
        "--out-dir",
        str(out_dir),
    )

    assert result.returncode == 0, result.stderr or result.stdout
    inventory_csv = pd.read_csv(out_dir / "inventory.csv")
    row = inventory_csv.loc[inventory_csv["relative_path"] == "All Files Together/xfix.us.txt"].iloc[0]
    assert row["action"] == "skip"
    assert row["reason"] == "Empty market file is skipped."


def test_storage_migrate_parquet_dry_run_preserves_sources(tmp_path: Path) -> None:
    market_root, corpus_root, working_root = _build_storage_fixture(tmp_path)
    out_dir = tmp_path / "migrate"
    archive_root = tmp_path / "archive"
    result = _run_storage_command(
        "storage",
        "migrate-parquet",
        "--market-root",
        str(market_root),
        "--corpus-root",
        str(corpus_root),
        "--working-root",
        str(working_root),
        "--out-dir",
        str(out_dir),
        "--archive-root",
        str(archive_root),
        "--dry-run",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    for name in ("conversion_manifest.json", "conversion_report.md", "rollback_manifest.json", "parity_checks.json"):
        assert (out_dir / name).exists(), name

    payload = json.loads(result.stdout)
    assert payload["apply"] is False
    assert (working_root / "AAPL.csv").exists()
    assert not (working_root / "AAPL.parquet").exists()
    assert not archive_root.exists()

    manifest = json.loads((out_dir / "conversion_manifest.json").read_text(encoding="utf-8"))
    statuses = {entry["status"] for entry in manifest["entries"]}
    assert statuses >= {"planned", "skipped"}
    parity = json.loads((out_dir / "parity_checks.json").read_text(encoding="utf-8"))
    assert parity["summary"]["checked"] == 0


def test_storage_migrate_parquet_apply_converts_and_archives(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    market_root, corpus_root, working_root = _build_storage_fixture(tmp_path)
    out_dir = tmp_path / "migrate_apply"
    archive_root = tmp_path / "archive_apply"
    result = _run_storage_command(
        "storage",
        "migrate-parquet",
        "--market-root",
        str(market_root),
        "--corpus-root",
        str(corpus_root),
        "--working-root",
        str(working_root),
        "--out-dir",
        str(out_dir),
        "--archive-root",
        str(archive_root),
        "--apply",
    )
    assert result.returncode == 0, result.stderr or result.stdout

    assert (working_root / "AAPL.parquet").exists()
    assert not (working_root / "AAPL.csv").exists()
    assert (archive_root / "working_root" / "AAPL.csv").exists()
    assert (market_root / "raw_market_parquet" / "symbol=AAPL.US" / "new_daily_files__aapl_us.parquet").exists()
    assert (market_root / "raw_market_parquet" / "symbol=AAPL.US" / "all_files_together__aapl_us.parquet").exists()
    assert (archive_root / "market_root" / "New Daily Files" / "AAPL.us.txt").exists()
    assert any((corpus_root / "daily_features_raw").rglob("*.parquet"))
    assert any((corpus_root / "daily_features_stable").rglob("*.parquet"))
    assert any((corpus_root / "annual_conflict").rglob("*.parquet"))
    assert (out_dir / "duplicate_parity" / "ohlcv_daily_csv" / "AAPL.parquet").exists()
    assert (working_root / "universe.csv").exists()
    assert (out_dir / "conversion_checkpoint.jsonl").exists()

    parity = json.loads((out_dir / "parity_checks.json").read_text(encoding="utf-8"))
    assert parity["summary"]["checked"] == 9
    assert parity["summary"]["failed"] == 0

    rollback = json.loads((out_dir / "rollback_manifest.json").read_text(encoding="utf-8"))
    assert any(entry["applied"] for entry in rollback["entries"])

    aapl = pd.read_parquet(working_root / "AAPL.parquet")
    assert list(aapl.columns) == ["date", "open", "high", "low", "close", "volume"]


def test_symbol_from_filename_preserves_dot_segments() -> None:
    assert _symbol_from_filename(Path("BRK.B.us.txt")) == "BRK.B.US"


def test_storage_migrate_parquet_reuses_checkpointed_applied_entries(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    market_root, corpus_root, working_root = _build_storage_fixture(tmp_path)
    out_dir = tmp_path / "resume"
    archive_root = tmp_path / "archive_resume"

    source_path = working_root / "AAPL.csv"
    archive_path = archive_root / "working_root" / "AAPL.csv"
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe = pd.read_csv(source_path)
    target_path = working_root / "AAPL.parquet"
    dataframe.to_parquet(target_path, index=False, compression="zstd")
    source_path.replace(archive_path)

    _append_checkpoint_record(
        out_dir / "conversion_checkpoint.jsonl",
        {
            "asset_key": str(source_path),
            "ts": "2026-03-22T00:00:00+00:00",
            "entry": {
                "root_name": "working_root",
                "source_path": str(source_path),
                "relative_path": "AAPL.csv",
                "source_format": "csv",
                "dataset_role": "canonical_normalized_ohlcv",
                "status": "applied",
                "action": "convert",
                "target_path": str(target_path),
                "archive_path": str(archive_path),
                "reason": "checkpoint",
                "compression": "zstd",
                "partitioning": "none",
                "source_hash_sha256": "source",
                "target_hash_sha256": "target",
                "rows": 2,
                "parity_passed": True,
            },
            "rollback": {
                "source_path": str(source_path),
                "target_path": str(target_path),
                "archive_path": str(archive_path),
                "applied": True,
            },
            "parity": {
                "source_path": str(source_path),
                "target_path": str(target_path),
                "row_count_equal": True,
                "min_date_equal": True,
                "max_date_equal": True,
                "symbol_coverage_equal": True,
                "column_set_equal": True,
                "numeric_equal": True,
                "non_numeric_equal": True,
                "parity_passed": True,
            },
        },
    )

    payload = migrate_parquet_storage(
        market_root=market_root,
        corpus_root=corpus_root,
        working_root=working_root,
        out_dir=out_dir,
        archive_root=archive_root,
        apply_changes=False,
    )

    assert payload["apply"] is False
    manifest = json.loads((out_dir / "conversion_manifest.json").read_text(encoding="utf-8"))
    assert any(entry["source_path"] == str(source_path) and entry["status"] == "applied" for entry in manifest["entries"])
    parity = json.loads((out_dir / "parity_checks.json").read_text(encoding="utf-8"))
    assert any(entry["source_path"] == str(source_path) for entry in parity["entries"])


def test_partitioned_writer_handles_many_daily_partitions(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    asset = StorageAsset(
        root_name="corpus_root",
        source_path=tmp_path / "GDELT_Data_1.csv",
        relative_path="GDELT_Data_1.csv",
        source_format="csv",
        dataset_role="corpus_daily_features_raw",
        status="planned",
        action="convert",
        target_path=str(tmp_path / "daily_features_raw"),
        archive_path=None,
        reason="test",
        compression="zstd",
        partitioning="day",
    )
    dataframe = pd.DataFrame(
        {
            "dt": pd.date_range("2020-01-01", periods=1025, freq="D").strftime("%Y-%m-%d"),
            "bucket": ["macro"] * 1025,
            "signal": [1.0] * 1025,
        }
    )

    target_path = tmp_path / "daily_features_raw"
    _write_target_dataframe(asset=asset, dataframe=dataframe, target_path=target_path)

    assert len(list(target_path.glob("day=*/part-00000.parquet"))) == 1025


def test_sort_dataframe_is_stable_across_column_order() -> None:
    left = pd.DataFrame(
        {
            "year": [2025, 2024],
            "countrycode": ["US", "CA"],
            "value": [2.0, 1.0],
        }
    )
    right = left[["countrycode", "value", "year"]]

    left_sorted = _sort_dataframe(left).loc[:, ["year", "countrycode", "value"]]
    right_sorted = _sort_dataframe(right).loc[:, ["year", "countrycode", "value"]]

    pd.testing.assert_frame_equal(left_sorted, right_sorted)


def test_read_source_dataframe_falls_back_when_sniffer_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_path = tmp_path / "AAPL.us.txt"
    source_path.write_text(
        "Date,Open,High,Low,Close,Volume\n2025-01-02,100,101,99,100.5,1000000\n",
        encoding="utf-8",
        newline="\n",
    )
    real_read_csv = pd.read_csv
    call_separators: list[str | None] = []

    def fake_read_csv(path: Path, *args: object, **kwargs: object) -> pd.DataFrame:
        separator = kwargs.get("sep")
        call_separators.append(separator if isinstance(separator, str) or separator is None else str(separator))
        if separator is None:
            raise csv.Error("Could not determine delimiter")
        return real_read_csv(path, *args, **kwargs)

    monkeypatch.setattr(storage_parquet.pd, "read_csv", fake_read_csv)

    dataframe = _read_source_dataframe(source_path)

    assert list(dataframe.columns) == ["Date", "Open", "High", "Low", "Close", "Volume"]
    assert call_separators[:2] == [None, ","]
