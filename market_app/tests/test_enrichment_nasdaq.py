from __future__ import annotations

import csv
from pathlib import Path

from tools.enrich_security_master import enrich_security_master


def _write_security_master(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
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
        writer.writerow(["AAA", "AAA.US", "", "", "", "", "", "", "stooq_txt", "AAA.us.txt", "2025-01-01"])
        writer.writerow(["BBB", "BBB.US", "", "", "", "", "", "", "stooq_txt", "BBB.us.txt", "2025-01-01"])


def test_enrich_nasdaq_symboldir(tmp_path: Path) -> None:
    metadata_cache = tmp_path / "out" / "metadata_cache"
    nasdaq_dir = metadata_cache / "nasdaq_trader"
    nasdaq_dir.mkdir(parents=True, exist_ok=True)
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

    input_path = tmp_path / "out" / "security_master.csv"
    _write_security_master(input_path)
    output_path = tmp_path / "out" / "security_master_enriched.csv"
    sic_codes = tmp_path / "sic_codes.csv"
    sic_codes.write_text("sic,office,industry_title\n", encoding="utf-8")
    overrides = tmp_path / "sector_overrides.csv"
    overrides.write_text("symbol,sector_bucket\n", encoding="utf-8")

    enrich_security_master(
        input_path=input_path,
        output_path=output_path,
        metadata_cache=metadata_cache,
        sic_codes_path=sic_codes,
        sector_overrides_path=overrides,
    )

    with output_path.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["AAA"]["name"] == "Alpha Labs"
    assert by_symbol["AAA"]["is_etf"] == "false"
    assert by_symbol["BBB"]["name"] == "Beta Fund"
    assert by_symbol["BBB"]["is_etf"] == "true"
