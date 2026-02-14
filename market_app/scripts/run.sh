#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
cd "$REPO_ROOT"

CONFIG="${REPO_ROOT}/config/config.yaml"
RUN_ID=""
TOP_N="200"
AS_OF=""
SYMBOLS_DIR=""
OHLCV_DIR=""
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --top-n) TOP_N="$2"; shift 2 ;;
    --as-of) AS_OF="$2"; shift 2 ;;
    --symbols-dir) SYMBOLS_DIR="$2"; shift 2 ;;
    --ohlcv-dir) OHLCV_DIR="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

CMD=(python -m market_app.cli --config "$CONFIG" --offline --top-n "$TOP_N")
[[ -n "$RUN_ID" ]] && CMD+=(--run-id "$RUN_ID")
[[ -n "$AS_OF" ]] && CMD+=(--as-of "$AS_OF")
[[ -n "$SYMBOLS_DIR" ]] && CMD+=(--symbols-dir "$SYMBOLS_DIR")
[[ -n "$OHLCV_DIR" ]] && CMD+=(--ohlcv-dir "$OHLCV_DIR")
[[ -n "$OUTPUT_DIR" ]] && CMD+=(--output-dir "$OUTPUT_DIR")
"${CMD[@]}"
