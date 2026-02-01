#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
cd "$REPO_ROOT"

if command -v python3.13 >/dev/null 2>&1; then
  PYTHON_BIN="python3.13"
elif command -v python3.12 >/dev/null 2>&1; then
  PYTHON_BIN="python3.12"
else
  echo "Python 3.13/3.12 not found. Install a supported version and retry." >&2
  exit 1
fi

${PYTHON_BIN} -m venv .venv
VENV_PY="${REPO_ROOT}/.venv/bin/python"
if [[ ! -x "$VENV_PY" ]]; then
  echo "Virtual environment python not found at $VENV_PY" >&2
  exit 1
fi

"$VENV_PY" -m pip install --upgrade pip
"$VENV_PY" -m pip install --only-binary=:all: -e ".[dev]"

"$VENV_PY" -m market_monitor.env_doctor --self-test
"$VENV_PY" -m pytest -q

echo "[done] bootstrap completed successfully."
