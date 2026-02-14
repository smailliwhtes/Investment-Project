#!/usr/bin/env bash
set -euo pipefail

warn() { echo "[warn] $*" 1>&2; }
info() { echo "[info] $*" 1>&2; }
err()  { echo "[error] $*" 1>&2; }

# Resolve repo root from this script location
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

PYTHON=""

if [[ -x "${REPO_ROOT}/.venv/bin/python" ]]; then
  PYTHON="${REPO_ROOT}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON="$(command -v python)"
fi

if [[ -z "${PYTHON}" ]]; then
  err "Python not found. Install Python 3.x and/or create .venv."
  exit 127
fi

if [[ -z "${VIRTUAL_ENV:-}" && -z "${CONDA_PREFIX:-}" ]]; then
  if [[ -d "${REPO_ROOT}/.venv" ]]; then
    warn "No active venv detected (VIRTUAL_ENV/CONDA_PREFIX empty). A repo .venv exists; consider activating it."
  else
    warn "No active venv detected (VIRTUAL_ENV/CONDA_PREFIX empty)."
  fi
fi

info "Repo root: ${REPO_ROOT}"
info "Using Python: ${PYTHON}"

TEST_PATH="tests/test_offline_e2e_market_and_corpus.py"
if [[ ! -f "${TEST_PATH}" ]]; then
  err "E2E test not found at expected path: ${TEST_PATH} (repo root tests/)."
  exit 2
fi

"${PYTHON}" -m pytest -q "${TEST_PATH}" -k offline_e2e --maxfail=1
info "E2E test passed."
