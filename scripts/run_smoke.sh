#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
APP_ROOT="${REPO_ROOT}/market_app"
cd "${APP_ROOT}"

python -m market_app.cli run --config tests/data/mini_dataset/config.yaml --offline --as-of-date 2025-01-31 --run-id smoke
