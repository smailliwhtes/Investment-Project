from __future__ import annotations

import csv
import json
from pathlib import Path

from market_monitor.metadata.security_master import (
    SECURITY_MASTER_COLUMNS,
    SecurityMasterConfig,
    build_security_master,
    parse_stooq_file,
)


def _fixture_root() -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "stooq_txt"


def test_parse_stooq_file() -> None:
    path = _fixture_root() / "nasdaq stocks" / "1" / "AAA.us.txt"
    symbol_us, symbol, asof = parse_stooq_file(path)
    assert symbol_us == "AAA.US"
    assert symbol == "AAA"
    assert asof == "2024-01-03"


def test_build_security_master_with_metadata(tmp_path: Path) -> None:
    stooq_root = _fixture_root()
    metadata_root = tmp_path / "incoming_metadata"
    nasdaq_dir = metadata_root / "nasdaq_trader"
    sec_dir = metadata_root / "sec"
    submissions_dir = sec_dir / "submissions"
    nasdaq_dir.mkdir(parents=True)
    submissions_dir.mkdir(parents=True)

    nasdaq_dir.joinpath("nasdaqlisted.txt").write_text(
        "\n".join(
            [
                "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
                "AAA|Alpha Labs|Q|N|N|100|N|N",
                "File Creation Time|20240104|",
            ]
        ),
        encoding="utf-8",
    )
    nasdaq_dir.joinpath("otherlisted.txt").write_text(
        "\n".join(
            [
                "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Test Issue|Round Lot Size|NASDAQ Symbol",
                "BBB|Beta Fund|N|BBB|Y|N|100|BBB",
                "File Creation Time|20240104|",
            ]
        ),
        encoding="utf-8",
    )

    sec_dir.joinpath("company_tickers.json").write_text(
        json.dumps(
            {
                "0": {
                    "cik_str": 123456,
                    "ticker": "AAA",
                    "title": "Alpha Labs",
                }
            }
        ),
        encoding="utf-8",
    )
    submissions_dir.joinpath("CIK0000123456.json").write_text(
        json.dumps({"sic": "3571", "sicDescription": "Electronic Computers"}),
        encoding="utf-8",
    )

    sic_codes = tmp_path / "out" / "sic_codes.csv"
    sic_codes.parent.mkdir(parents=True, exist_ok=True)
    with sic_codes.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["sic", "office", "industry_title"])
        writer.writerow(["3571", "Office of Tech", "Electronic Computers"])

    output_path = tmp_path / "out" / "security_master.csv"
    config = SecurityMasterConfig(
        stooq_root=stooq_root,
        output_path=output_path,
        metadata_root=metadata_root,
        filter_required=False,
        path_mode="relative",
        repo_root=tmp_path,
        sic_codes_path=sic_codes,
    )
    records = build_security_master(config)

    assert output_path.exists()
    with output_path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        rows = list(reader)

    assert header == SECURITY_MASTER_COLUMNS
    assert len(records) == 2
    record_by_symbol = {row[0]: row for row in rows}
    aaa = record_by_symbol["AAA"]
    bbb = record_by_symbol["BBB"]
    assert aaa[2] == "Alpha Labs"
    assert aaa[4] == "false"
    assert aaa[5] == "0000123456"
    assert aaa[6] == "3571"
    assert aaa[7] == "Electronic Computers"
    assert bbb[2] == "Beta Fund"
    assert bbb[4] == "true"


def test_build_security_master_required_filter(tmp_path: Path) -> None:
    stooq_root = _fixture_root()
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
