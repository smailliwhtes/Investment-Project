from __future__ import annotations

import argparse
import csv
import json
from datetime import date
from pathlib import Path
from zipfile import ZipFile


def build_sic_cache(submissions_zip: Path, output_path: Path) -> None:
    records = []
    with ZipFile(submissions_zip) as archive:
        for name in archive.namelist():
            if not name.startswith("submissions/CIK") or not name.endswith(".json"):
                continue
            with archive.open(name) as handle:
                payload = json.load(handle)
            sic = str(payload.get("sic") or "").strip()
            if not sic:
                continue
            cik = Path(name).stem.replace("CIK", "")
            sic_desc = payload.get("sicDescription") or ""
            records.append(
                {
                    "cik": cik,
                    "sic": sic,
                    "sic_desc": sic_desc,
                    "asof_date": date.today().isoformat(),
                    "source_bucket": "sec_submissions_zip",
                }
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["cik", "sic", "sic_desc", "asof_date", "source_bucket"]
        )
        writer.writeheader()
        writer.writerows(records)


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build SIC cache from submissions.zip.")
    ap.add_argument("--submissions-zip", required=True, help="Path to SEC submissions.zip.")
    ap.add_argument(
        "--output",
        default="out/sec/sic_by_cik.csv",
        help="Output CSV path (default: out/sec/sic_by_cik.csv).",
    )
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    build_sic_cache(Path(args.submissions_zip), Path(args.output))


if __name__ == "__main__":
    main()
