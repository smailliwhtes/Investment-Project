from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from market_monitor.folder_to_parquet import convert_folder_to_parquet


def _run_command(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "market_monitor", *args],
        check=False,
        capture_output=True,
        text=True,
    )


def test_convert_folder_to_parquet_recurses_and_preserves_structure(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    source_root = tmp_path / "source"
    nested = source_root / "nested"
    nested.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"symbol": ["AAPL", "MSFT"], "score": [1.2, 1.4]}).to_csv(
        source_root / "scores.csv",
        index=False,
        lineterminator="\n",
    )
    pd.DataFrame({"symbol": ["AAPL"], "signal": [3]}).to_csv(
        nested / "signals.tsv",
        index=False,
        sep="\t",
        lineterminator="\n",
    )
    (nested / "events.jsonl").write_text(
        '{"symbol":"AAPL","day":"2025-01-31"}\n{"symbol":"MSFT","day":"2025-02-01"}\n',
        encoding="utf-8",
        newline="\n",
    )
    pd.DataFrame({"symbol": ["QQQ"], "weight": [0.5]}).to_parquet(
        nested / "weights.parquet",
        index=False,
        compression="zstd",
    )
    (nested / "notes.md").write_text("# ignore", encoding="utf-8", newline="\n")

    out_dir = tmp_path / "converted"
    summary = convert_folder_to_parquet(source_root=source_root, out_dir=out_dir)

    assert summary["summary"] == {
        "scanned": 5,
        "converted": 4,
        "skipped": 1,
        "errors": 0,
    }
    assert (out_dir / "scores.parquet").exists()
    assert (out_dir / "nested" / "signals.parquet").exists()
    assert (out_dir / "nested" / "events.parquet").exists()
    assert (out_dir / "nested" / "weights.parquet").exists()

    manifest = json.loads((out_dir / "folder_conversion_manifest.json").read_text(encoding="utf-8"))
    assert manifest["summary"]["converted"] == 4
    skipped = [entry for entry in manifest["entries"] if entry["status"] == "skipped"]
    assert len(skipped) == 1
    assert skipped[0]["relative_path"] == "nested/notes.md"


def test_convert_folder_to_parquet_disambiguates_name_collisions(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"value": [1, 2]}).to_csv(
        source_root / "same.csv",
        index=False,
        lineterminator="\n",
    )
    pd.DataFrame({"value": [3, 4]}).to_parquet(
        source_root / "same.parquet",
        index=False,
        compression="zstd",
    )

    convert_folder_to_parquet(source_root=source_root, out_dir=tmp_path / "out")

    assert (tmp_path / "out" / "same.parquet").exists()
    assert (tmp_path / "out" / "same__csv.parquet").exists()


def test_storage_convert_folder_cli_strict_fails_when_file_is_skipped(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)

    pd.DataFrame({"value": [1]}).to_csv(
        source_root / "ok.csv",
        index=False,
        lineterminator="\n",
    )
    (source_root / "notes.md").write_text("ignore", encoding="utf-8", newline="\n")

    result = _run_command(
        "storage",
        "convert-folder-parquet",
        "--source-root",
        str(source_root),
        "--strict",
    )

    assert result.returncode == 4
    assert "Strict mode blocked conversion" in result.stdout
    manifest = json.loads(
        (tmp_path / "source_parquet" / "folder_conversion_manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["summary"]["converted"] == 1
    assert manifest["summary"]["skipped"] == 1
