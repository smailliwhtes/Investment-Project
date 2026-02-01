#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

CONFIG="${REPO_ROOT}/config.example.yaml"
WATCHLIST=""
DATA_DIR="${REPO_ROOT}/outputs/ohlcv_smoke"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --watchlist)
      WATCHLIST="$2"
      shift 2
      ;;
    --data-dir)
      DATA_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

CONFIG=$(python -c "from pathlib import Path; import sys; print(Path(sys.argv[1]).expanduser().resolve())" "$CONFIG")
if [[ -z "$WATCHLIST" ]]; then
  WATCHLIST=$(python - <<'PY' "$CONFIG"
import sys
from pathlib import Path
import yaml

config_path = Path(sys.argv[1])
config = yaml.safe_load(config_path.read_text()) or {}
watchlist = config.get("watchlist_path") or config.get("paths", {}).get("watchlist_file")
if not watchlist:
    watchlist = "watchlists/watchlist_core.csv"
watchlist_path = (config_path.parent / watchlist).resolve()
print(str(watchlist_path))
PY
  )
fi

WATCHLIST=$(python -c "from pathlib import Path; import sys; print(Path(sys.argv[1]).expanduser().resolve())" "$WATCHLIST")

export MARKET_APP_OHLCV_DIR="$DATA_DIR"
export NASDAQ_DAILY_DIR="$DATA_DIR"

python -m market_monitor.tools.check_watchlist_ohlcv --config "$CONFIG" --watchlist "$WATCHLIST"
