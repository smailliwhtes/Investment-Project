from __future__ import annotations

import argparse
import csv
from pathlib import Path

from market_monitor.metadata.enrich_nasdaq_symboldir import load_nasdaq_symboldir
from market_monitor.metadata.enrich_sec_sic import (
    derive_sector_bucket,
    load_sector_overrides,
    load_sic_by_cik,
    load_sic_codes,
    load_submissions_sic,
)
from market_monitor.metadata.enrich_sec_tickers import load_sec_ticker_map
from market_monitor.metadata.security_master import SECURITY_MASTER_COLUMNS


def enrich_security_master(
    input_path: Path,
    output_path: Path,
    metadata_cache: Path,
    sic_codes_path: Path,
    sector_overrides_path: Path,
) -> None:
    records = _read_security_master(input_path)
    nasdaq = load_nasdaq_symboldir(metadata_cache / "nasdaq_trader")
    sec = load_sec_ticker_map(metadata_cache / "sec")
    sic_codes = load_sic_codes(sic_codes_path) if sic_codes_path.exists() else {}
    overrides = load_sector_overrides(sector_overrides_path)

    submissions_dir = metadata_cache / "sec" / "submissions"
    submissions_sic = load_submissions_sic(submissions_dir)
    sic_by_cik = load_sic_by_cik(metadata_cache / "sec" / "sic_by_cik.csv")
    if sic_by_cik:
        submissions_sic = sic_by_cik

    for record in records:
        symbol = record["symbol"].upper()
        source_bucket = record.get("source_bucket", "")

        nasdaq_row = nasdaq.get(symbol)
        if nasdaq_row:
            record["name"] = record["name"] or nasdaq_row.get("name") or ""
            record["exchange"] = record["exchange"] or nasdaq_row.get("exchange") or ""
            if record["is_etf"] == "":
                is_etf = nasdaq_row.get("is_etf")
                record["is_etf"] = "" if is_etf is None else str(is_etf).lower()
            source_bucket = _add_source_bucket(source_bucket, "nasdaq_symboldir")

        sec_row = sec.get(symbol)
        if sec_row:
            if not record["cik"]:
                record["cik"] = sec_row.get("cik") or ""
            if not record["name"]:
                record["name"] = sec_row.get("name") or ""
            if not record["exchange"]:
                record["exchange"] = sec_row.get("exchange") or ""
            source_bucket = _add_source_bucket(source_bucket, "sec_company_tickers")

        cik = record.get("cik") or ""
        sic_info = submissions_sic.get(cik)
        if sic_info:
            record["sic"] = record["sic"] or sic_info.get("sic") or ""
            source_bucket = _add_source_bucket(source_bucket, "sec_submissions_zip")

        if not record["sector_bucket"]:
            sector_bucket = derive_sector_bucket(
                symbol=symbol,
                name=record.get("name") or "",
                sic=record.get("sic") or None,
                overrides=overrides,
                fallback_bucket=_fallback_bucket(record.get("source_bucket") or ""),
            )
            record["sector_bucket"] = sector_bucket or ""

        record["source_bucket"] = source_bucket

        if record["sic"] and sic_codes and not record["sector_bucket"]:
            record["sector_bucket"] = sic_codes.get(record["sic"], "")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SECURITY_MASTER_COLUMNS)
        writer.writeheader()
        writer.writerows(records)


def _read_security_master(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        records = []
        for row in reader:
            record = {key: (row.get(key) or "").strip() for key in SECURITY_MASTER_COLUMNS}
            records.append(record)
        return records


def _add_source_bucket(existing: str, entry: str) -> str:
    if not existing:
        return entry
    parts = [part.strip() for part in existing.split("+") if part.strip()]
    if entry not in parts:
        parts.append(entry)
    return "+".join(parts)


def _fallback_bucket(source_bucket: str) -> str | None:
    lowered = source_bucket.lower()
    if "nasdaq_etfs" in lowered or "nyse_etfs" in lowered:
        return "broad_market"
    return None


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enrich security_master.csv from offline snapshots.")
    ap.add_argument("--input", default="out/security_master.csv", help="Input security master CSV.")
    ap.add_argument(
        "--output",
        default="out/security_master.csv",
        help="Output CSV path (default overwrites input).",
    )
    ap.add_argument(
        "--metadata-cache",
        default="out/metadata_cache",
        help="Metadata cache root (default: out/metadata_cache).",
    )
    ap.add_argument(
        "--sic-codes",
        default="data/sic_codes.csv",
        help="SIC codes CSV path (default: data/sic_codes.csv).",
    )
    ap.add_argument(
        "--sector-overrides",
        default="config/sector_overrides.csv",
        help="Sector override CSV path (default: config/sector_overrides.csv).",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    enrich_security_master(
        input_path=Path(args.input),
        output_path=Path(args.output),
        metadata_cache=Path(args.metadata_cache),
        sic_codes_path=Path(args.sic_codes),
        sector_overrides_path=Path(args.sector_overrides),
    )


if __name__ == "__main__":
    main()
