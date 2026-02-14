#!/usr/bin/env bash
set -euo pipefail

if ! command -v python >/dev/null 2>&1; then
  echo "python was not found in PATH. Install Python and retry." >&2
  exit 1
fi

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "WARNING: No active virtual environment detected (VIRTUAL_ENV is empty)." >&2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_ROOT="$REPO_ROOT/market_app"

if [[ ! -d "$APP_ROOT" ]]; then
  echo "market_app directory not found at expected path: $APP_ROOT" >&2
  exit 1
fi

cd "$APP_ROOT"
python -m pytest -q tests/test_offline_e2e_market_and_corpus.py -k offline_e2e --maxfail=1
