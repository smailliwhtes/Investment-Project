from __future__ import annotations

import argparse
import sys
from pathlib import Path

from market_monitor.config_schema import ConfigError, load_config
from market_monitor.data_paths import resolve_data_paths
from market_monitor.paths import resolve_path
from market_monitor.providers.nasdaq_daily import NasdaqDailyProvider, NasdaqDailySource
from market_monitor.universe import read_watchlist


def _print_ohlcv_remediation() -> None:
    print("[remediation] Configure the offline OHLCV data directory:")
    print(
        "  1) Set data_roots.ohlcv_dir in config.yaml "
        "(example: data_roots: {ohlcv_dir: ./data/ohlcv})"
    )
    print(
        '  2) Or set MARKET_APP_OHLCV_DIR '
        '(example: export MARKET_APP_OHLCV_DIR="/path/to/ohlcv")'
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Assert every watchlist symbol has a NASDAQ daily OHLCV CSV."
    )
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--watchlist", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    config_path = Path(args.config)
    try:
        config_result = load_config(config_path)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    config = config_result.config
    base_dir = config_path.parent
    data_paths = resolve_data_paths(config, base_dir)
    if not data_paths.nasdaq_daily_dir:
        print("[error] OHLCV data directory is not configured.")
        _print_ohlcv_remediation()
        return 2
    if not data_paths.nasdaq_daily_dir.exists():
        print(f"[error] OHLCV data directory does not exist: {data_paths.nasdaq_daily_dir}")
        _print_ohlcv_remediation()
        return 2

    watchlist_path = resolve_path(
        base_dir, args.watchlist or config["paths"]["watchlist_file"]
    )
    if not watchlist_path.exists():
        print(f"[error] Watchlist file not found: {watchlist_path}")
        return 2

    watchlist_df = read_watchlist(watchlist_path)
    if watchlist_df.empty:
        print(f"[error] Watchlist is empty: {watchlist_path}")
        return 1

    provider = NasdaqDailyProvider(
        NasdaqDailySource(directory=data_paths.nasdaq_daily_dir, cache_dir=Path("."))
    )

    missing = [
        symbol
        for symbol in watchlist_df["symbol"].astype(str).tolist()
        if not provider.resolve_symbol_file(symbol)
    ]

    if missing:
        print("[error] Missing OHLCV CSV files for watchlist symbols:")
        for symbol in missing:
            print(f"  - {symbol}")
        return 1

    print(
        f"[ok] Found OHLCV CSV files for {len(watchlist_df)} watchlist symbols in {data_paths.nasdaq_daily_dir}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
