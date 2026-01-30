from __future__ import annotations

import csv
import json
from pathlib import Path

from market_monitor.metadata.security_master import SecurityMasterConfig, build_security_master
from tools.enrich_security_master import enrich_security_master


def test_offline_smoke_pipeline(tmp_path: Path) -> None:
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

    metadata_cache = tmp_path / "out" / "metadata_cache"
    nasdaq_dir = metadata_cache / "nasdaq_trader"
    sec_dir = metadata_cache / "sec"
    nasdaq_dir.mkdir(parents=True, exist_ok=True)
    sec_dir.mkdir(parents=True, exist_ok=True)
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

    sic_codes = tmp_path / "sic_codes.csv"
    sic_codes.write_text("sic,office,industry_title\n3674,Office,Semiconductors\n", encoding="utf-8")
    overrides = tmp_path / "sector_overrides.csv"
    overrides.write_text("symbol,sector_bucket\n", encoding="utf-8")

    enrich_security_master(
        input_path=output_path,
        output_path=output_path,
        metadata_cache=metadata_cache,
        sic_codes_path=sic_codes,
        sector_overrides_path=overrides,
    )

    with output_path.open("r", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert rows
    assert any(row["symbol"] == "AAA" for row in rows)
