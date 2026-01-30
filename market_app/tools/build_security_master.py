from __future__ import annotations

import argparse
import logging
from pathlib import Path

from market_monitor.config.discovery import ensure_required_symbols_file, find_repo_root
from market_monitor.metadata.security_master import SecurityMasterConfig, build_security_master


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Build offline security_master.csv from Stooq files.")
    ap.add_argument("--stooq-root", required=True, help="Root directory containing .us.txt files.")
    ap.add_argument(
        "--output",
        default="out/security_master.csv",
        help="Path for output CSV (default: out/security_master.csv).",
    )
    ap.add_argument(
        "--required-symbols",
        default=None,
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
    return ap.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    args = _parse_args()
    repo_root = find_repo_root(Path.cwd())
    required_path = Path(args.required_symbols).expanduser() if args.required_symbols else None
    if required_path is None and repo_root:
        required_path = ensure_required_symbols_file(repo_root)

    config = SecurityMasterConfig(
        stooq_root=Path(args.stooq_root).expanduser(),
        output_path=Path(args.output).expanduser(),
        required_symbols_path=required_path if required_path and required_path.exists() else None,
        filter_required=args.filter_required,
        path_mode=args.path_mode,
        repo_root=repo_root,
    )
    records = build_security_master(config)
    logging.info("Wrote %s records to %s", len(records), config.output_path)


if __name__ == "__main__":
    main()
