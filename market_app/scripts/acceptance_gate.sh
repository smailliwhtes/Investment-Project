#!/usr/bin/env bash
set -euo pipefail

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if ! command -v git >/dev/null 2>&1; then
  echo "git is required for acceptance_gate.sh" >&2
  exit 1
fi

CLONE_ROOT="${CLONE_ROOT:-}"
if [[ -z "$CLONE_ROOT" ]]; then
  CLONE_ROOT="$(mktemp -d -t market_app_acceptance_XXXXXX)"
fi
CLONE_PATH="$CLONE_ROOT/repo"

echo "[stage] cloning repo to $CLONE_PATH"
git clone "$SOURCE_ROOT" "$CLONE_PATH" >/dev/null
cd "$CLONE_PATH"

PYTHON_BIN="python"
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

if [[ ! -f ".venv/bin/python" ]]; then
  echo "[stage] creating venv"
  "$PYTHON_BIN" -m venv .venv
fi

VENV_PY=".venv/bin/python"

echo "[stage] python version"
"$VENV_PY" --version

echo "[stage] installing dependencies"
if [[ -f "requirements.txt" ]]; then
  echo "[info] using requirements.txt"
  "$VENV_PY" -m pip install --upgrade pip >/dev/null
  "$VENV_PY" -m pip install -r requirements.txt >/dev/null
elif [[ -f "pyproject.toml" ]]; then
  echo "[info] using pyproject.toml"
  "$VENV_PY" -m pip install --upgrade pip >/dev/null
  "$VENV_PY" -m pip install . >/dev/null
else
  echo "No requirements.txt or pyproject.toml found." >&2
  exit 1
fi

echo "[stage] running pytest"
"$VENV_PY" -m pytest -q

echo "[stage] running doctor"
"$VENV_PY" -m market_app.cli doctor --config configs/acceptance.yaml

RUNS_DIR="runs_acceptance"
RUN1="acceptance_run_1"
RUN2="acceptance_run_2"

echo "[stage] running pipeline (run 1)"
"$VENV_PY" -m market_app.cli run --config configs/acceptance.yaml --offline --runs-dir "$RUNS_DIR" --run-id "$RUN1"

echo "[stage] running pipeline (run 2)"
"$VENV_PY" -m market_app.cli run --config configs/acceptance.yaml --offline --runs-dir "$RUNS_DIR" --run-id "$RUN2"

EXPECTED_CORE=(eligible.csv scored.csv features.csv classified.csv universe.csv)
EXPECTED_META=(manifest.json digest.json run.log)

for run_id in "$RUN1" "$RUN2"; do
  run_dir="$CLONE_PATH/$RUNS_DIR/$run_id"
  if [[ ! -d "$run_dir" ]]; then
    echo "Missing run dir: $run_dir" >&2
    exit 1
  fi
  for name in "${EXPECTED_CORE[@]}"; do
    if [[ ! -f "$run_dir/$name" ]]; then
      echo "Missing output $name in $run_dir" >&2
      exit 1
    fi
  done
  for name in "${EXPECTED_META[@]}"; do
    if [[ ! -f "$run_dir/$name" ]]; then
      echo "Missing output $name in $run_dir" >&2
      exit 1
    fi
  done
  if [[ ! -f "$run_dir/report.md" && ! -f "$run_dir/report.html" ]]; then
    echo "Missing report.md or report.html in $run_dir" >&2
    exit 1
  fi
done

DIGEST1="$CLONE_PATH/$RUNS_DIR/$RUN1/digest.json"
DIGEST2="$CLONE_PATH/$RUNS_DIR/$RUN2/digest.json"

HASH1=$("$VENV_PY" -c "import hashlib, pathlib; print(hashlib.sha256(pathlib.Path('$DIGEST1').read_bytes()).hexdigest())")
HASH2=$("$VENV_PY" -c "import hashlib, pathlib; print(hashlib.sha256(pathlib.Path('$DIGEST2').read_bytes()).hexdigest())")

if [[ "$HASH1" != "$HASH2" ]]; then
  echo "[error] Determinism check failed." >&2
  echo "  run1 digest: $DIGEST1" >&2
  echo "  run2 digest: $DIGEST2" >&2
  echo "  hash1: $HASH1" >&2
  echo "  hash2: $HASH2" >&2
  exit 1
fi

echo "[done] acceptance gate PASS"
