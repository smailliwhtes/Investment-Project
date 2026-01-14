import os
import sys
from pathlib import Path

import requests

from market_monitor.config_schema import ConfigError, load_config
from market_monitor.paths import find_repo_root, resolve_path


def run_doctor(config_path: Path) -> int:
    print("[doctor] Market Monitor diagnostics")
    root = find_repo_root()
    if root != Path.cwd():
        print(f"[fix] Run from repo root: {root}")

    if sys.version_info < (3, 10):
        print("[error] Python 3.10+ required. Install Python 3.10 or 3.11.")
        return 2

    try:
        result = load_config(config_path)
    except ConfigError as exc:
        print(f"[error] {exc}")
        return 2

    config = result.config
    watchlist_path = resolve_path(root, config["paths"]["watchlist_file"])
    if not watchlist_path.exists():
        print(f"[warn] Watchlist missing at {watchlist_path}. Create it or pass --watchlist.")

    outputs_dir = resolve_path(root, config["paths"]["outputs_dir"])
    outputs_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = resolve_path(root, config["paths"]["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)

    try:
        response = requests.get("https://stooq.pl/q/d/l/?s=aapl.us&i=d", timeout=10)
        if response.status_code != 200 or "Date,Open,High,Low,Close,Volume" not in response.text:
            print("[warn] Stooq did not return CSV. Check connectivity or provider block.")
    except requests.RequestException as exc:
        print(f"[warn] Stooq connectivity check failed: {exc}")

    if config["data"]["provider"] == "twelvedata" and not os.getenv("TWELVEDATA_API_KEY"):
        print("[error] TWELVEDATA_API_KEY is missing. Set it in your environment.")

    if config["data"]["provider"] == "alphavantage" and not os.getenv("ALPHAVANTAGE_API_KEY"):
        print("[error] ALPHAVANTAGE_API_KEY is missing. Set it in your environment.")

    if config["data"]["provider"] == "finnhub" and not os.getenv("FINNHUB_API_KEY"):
        print("[error] FINNHUB_API_KEY is missing. Set it in your environment.")

    print("[doctor] Done. If errors are listed above, address them before running.")
    return 0
