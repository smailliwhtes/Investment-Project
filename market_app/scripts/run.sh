#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

CONFIG="${REPO_ROOT}/config.example.yaml"
WATCHLIST=""
OUTDIR="${REPO_ROOT}/outputs"
DATA_DIR="${REPO_ROOT}/outputs/ohlcv_smoke"
CACHE_DIR="${REPO_ROOT}/outputs/cache"
LOG_LEVEL="INFO"
RUN_ID_OVERRIDE=""

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
    --data-dir)
      DATA_DIR="$2"
      shift 2
      ;;
    --cache-dir)
      CACHE_DIR="$2"
      shift 2
      ;;
    --log-level)
      LOG_LEVEL="$2"
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

python - <<'PY' "$WATCHLIST" "$DATA_DIR"
from __future__ import annotations

import csv
import sys
from pathlib import Path

import numpy as np
import pandas as pd

watchlist_path = Path(sys.argv[1])
data_dir = Path(sys.argv[2])
data_dir.mkdir(parents=True, exist_ok=True)

symbols = []
with watchlist_path.open("r", encoding="utf-8") as handle:
    reader = csv.DictReader(handle)
    if "symbol" not in reader.fieldnames:
        raise SystemExit("Watchlist CSV missing 'symbol' column.")
    for row in reader:
        symbol = (row.get("symbol") or "").strip()
        if symbol:
            symbols.append(symbol)

rows = 300

def build_ohlcv(symbol: str) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=rows, freq="B")
    base = 100 + (sum(ord(letter) for letter in symbol) % 25)
    trend = np.linspace(0, 12, rows)
    close = base + trend
    open_ = close * 0.995
    high = close * 1.01
    low = close * 0.99
    volume = 1_000_000 + np.arange(rows) * 10
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        }
    )

for symbol in symbols:
    df = build_ohlcv(symbol)
    df.to_csv(data_dir / f"{symbol}.csv", index=False)

print(f"[ok] Wrote synthetic OHLCV for {len(symbols)} symbols to {data_dir}")
PY

export MARKET_APP_OHLCV_DIR="$DATA_DIR"
export NASDAQ_DAILY_DIR="$DATA_DIR"

TEMP_OUTDIR="${OUTDIR%/}/tmp_run"
rm -rf "$TEMP_OUTDIR"
mkdir -p "$TEMP_OUTDIR"

RUN_ARGS=("-m" "market_monitor" "run" "--config" "$CONFIG" "--watchlist" "$WATCHLIST" "--outdir" "$TEMP_OUTDIR" "--cache-dir" "$CACHE_DIR" "--offline" "--mode" "watchlist" "--log-level" "$LOG_LEVEL")
if [[ -n "$RUN_ID_OVERRIDE" ]]; then
  RUN_ARGS+=("--run-id" "$RUN_ID_OVERRIDE")
fi

python "${RUN_ARGS[@]}"

RUN_ID=$(python - <<'PY' "$TEMP_OUTDIR"
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1]) / "run_manifest.json"
if not manifest_path.exists():
    raise SystemExit("run_manifest.json not found in output directory")
manifest = json.loads(manifest_path.read_text())
run_id = manifest.get("run_id")
if not run_id:
    raise SystemExit("run_id missing from run_manifest.json")
print(run_id)
PY
)

FINAL_OUTDIR="${OUTDIR%/}/${RUN_ID}"
mkdir -p "$FINAL_OUTDIR"

shopt -s dotglob
mv "$TEMP_OUTDIR"/* "$FINAL_OUTDIR"/
shopt -u dotglob

if [[ -f "$FINAL_OUTDIR/eligible_${RUN_ID}.csv" ]]; then
  mv "$FINAL_OUTDIR/eligible_${RUN_ID}.csv" "$FINAL_OUTDIR/eligible.csv"
fi
if [[ -f "$FINAL_OUTDIR/scored_${RUN_ID}.csv" ]]; then
  mv "$FINAL_OUTDIR/scored_${RUN_ID}.csv" "$FINAL_OUTDIR/scored.csv"
fi
if [[ -f "$FINAL_OUTDIR/run_report.md" ]]; then
  mv "$FINAL_OUTDIR/run_report.md" "$FINAL_OUTDIR/report.md"
fi

rmdir "$TEMP_OUTDIR" 2>/dev/null || true

echo "[done] Outputs written to $FINAL_OUTDIR"
