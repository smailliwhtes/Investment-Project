from __future__ import annotations

import csv
import json
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


def test_enrich_sec_cik_and_sic(tmp_path: Path) -> None:
    metadata_cache = tmp_path / "out" / "metadata_cache"
    sec_dir = metadata_cache / "sec"
    sec_dir.mkdir(parents=True, exist_ok=True)
    sec_dir.joinpath("company_tickers.json").write_text(
        json.dumps(
            {"0": {"cik_str": 123456, "ticker": "AAA", "title": "Alpha Labs", "exchange": "NASDAQ"}}
        ),
        encoding="utf-8",
    )

    sic_by_cik = sec_dir / "sic_by_cik.csv"
    with sic_by_cik.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["cik", "sic", "sic_desc", "asof_date", "source_bucket"])
        writer.writerow(["0000123456", "3674", "Semiconductors", "2025-01-01", "sec_submissions_zip"])

    input_path = tmp_path / "out" / "security_master.csv"
    _write_security_master(input_path)
    output_path = tmp_path / "out" / "security_master_enriched.csv"
    sic_codes = tmp_path / "sic_codes.csv"
    sic_codes.write_text("sic,office,industry_title\n3674,Office,Semiconductors\n", encoding="utf-8")
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

    row = rows[0]
    assert row["cik"] == "0000123456"
    assert row["sic"] == "3674"
    assert row["sector_bucket"] == "semis"
