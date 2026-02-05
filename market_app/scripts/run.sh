#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

CONFIG="${REPO_ROOT}/config.example.yaml"
WATCHLIST=""
OUTDIR="${REPO_ROOT}/outputs"
OHLCV_RAW_DIR=""
OHLCV_DAILY_DIR=""
EXOGENOUS_DAILY_DIR=""
RUN_ID_OVERRIDE=""
ASOF_DATE=""

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
    --outdir)
      OUTDIR="$2"
      shift 2
      ;;
    --ohlcv-raw-dir)
      OHLCV_RAW_DIR="$2"
      shift 2
      ;;
    --ohlcv-daily-dir)
      OHLCV_DAILY_DIR="$2"
      shift 2
      ;;
    --exogenous-daily-dir)
      EXOGENOUS_DAILY_DIR="$2"
      shift 2
      ;;
    --asof)
      ASOF_DATE="$2"
      shift 2
      ;;
    --run-id)
      RUN_ID_OVERRIDE="$2"
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

if [[ ! -f "$WATCHLIST" ]]; then
  echo "Watchlist file not found: $WATCHLIST" >&2
  exit 2
fi

RUN_ID="${RUN_ID_OVERRIDE:-run_$(date +%Y%m%d_%H%M%S)}"

RUN_ARGS=("run" "--config" "$CONFIG" "--watchlist" "$WATCHLIST" "--run-id" "$RUN_ID" "--outputs-dir" "$OUTDIR")
if [[ -n "$ASOF_DATE" ]]; then
  RUN_ARGS+=("--asof" "$ASOF_DATE")
fi
if [[ -n "$OHLCV_RAW_DIR" ]]; then
  RUN_ARGS+=("--ohlcv-raw-dir" "$OHLCV_RAW_DIR")
fi
if [[ -n "$OHLCV_DAILY_DIR" ]]; then
  RUN_ARGS+=("--ohlcv-daily-dir" "$OHLCV_DAILY_DIR")
fi
if [[ -n "$EXOGENOUS_DAILY_DIR" ]]; then
  RUN_ARGS+=("--exogenous-daily-dir" "$EXOGENOUS_DAILY_DIR")
fi

MARKET_MONITOR_BIN="${REPO_ROOT}/.venv/bin/market-monitor"
if [[ -x "$MARKET_MONITOR_BIN" ]]; then
  "$MARKET_MONITOR_BIN" "${RUN_ARGS[@]}"
else
  market-monitor "${RUN_ARGS[@]}"
fi

FINAL_OUTDIR="${OUTDIR%/}/${RUN_ID}"
echo "[done] Outputs written to $FINAL_OUTDIR"
