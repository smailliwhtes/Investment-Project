from __future__ import annotations

import argparse
import logging
from pathlib import Path

from market_monitor.metadata.security_master import SecurityMasterConfig, build_security_master


def _find_repo_root(start: Path) -> Path | None:
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build offline security_master.csv from Stooq files.")
    ap.add_argument("--stooq-root", required=True, help="Root directory containing .us.txt files.")
    ap.add_argument(
        "--output",
        default="out/security_master.csv",
        help="Path for output CSV (default: out/security_master.csv).",
    )
    ap.add_argument(
        "--metadata-root",
        default="incoming_metadata",
        help="Root directory containing nasdaq_trader/ and sec/ snapshots.",
    )
    ap.add_argument(
        "--required-symbols",
        default="config/universe_required.csv",
        help="Optional CSV/TXT listing required symbols (default: config/universe_required.csv).",
    )
    ap.add_argument(
        "--filter-required",
        action="store_true",
        help="Filter output to required symbols only.",
    )
    ap.add_argument(
        "--path-mode",
        choices=["auto", "relative", "absolute"],
        default="auto",
        help="How to store ohlcv_path entries (default: auto).",
    )
    ap.add_argument(
        "--metastock-root",
        help="Optional MetaStock pack root for XMASTER parsing.",
    )
    ap.add_argument(
        "--sic-codes",
        help="Optional sic_codes.csv path; defaults to out/sic_codes.csv.",
    )
    return ap.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = _parse_args()
    repo_root = _find_repo_root(Path.cwd())
    metadata_root = Path(args.metadata_root).expanduser()
    required_path = Path(args.required_symbols).expanduser() if args.required_symbols else None
    metastock_root = Path(args.metastock_root).expanduser() if args.metastock_root else None
    sic_codes = Path(args.sic_codes).expanduser() if args.sic_codes else None

    config = SecurityMasterConfig(
        stooq_root=Path(args.stooq_root).expanduser(),
        output_path=Path(args.output).expanduser(),
        metadata_root=metadata_root if metadata_root.exists() else None,
        required_symbols_path=required_path if required_path and required_path.exists() else None,
        filter_required=args.filter_required,
        path_mode=args.path_mode,
        repo_root=repo_root,
        metastock_root=metastock_root if metastock_root and metastock_root.exists() else None,
        sic_codes_path=sic_codes if sic_codes and sic_codes.exists() else None,
    )
    records = build_security_master(config)
    logging.info("Wrote %s records to %s", len(records), config.output_path)


if __name__ == "__main__":
    main()
