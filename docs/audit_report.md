# Repository Readiness Report (cross-referenced against guide markdown)

Date: 2026-02-16  
Repo: `/home/runner/work/Investment-Project/Investment-Project`

## AGENTS instruction check
- Loaded `/home/runner/work/Investment-Project/Investment-Project/AGENTS.md` before review.
- Nested `AGENTS.md` overrides: none found.

## Review scope and method
- Inventory baseline: `git ls-files` reports 1060 tracked files (`.csv` 589, `.py` 210, `.md` 71, `.ps1` 36, `.sh` 8).
- Reference guides reviewed for expected behavior:
  - `docs/codex/00_context.md`
  - `docs/codex/10_data_contracts.md`
  - `docs/codex/20_output_contract.md`
  - `docs/codex/40_commands.md`
  - `market_app/docs/application_overview.md`
  - `market_app/docs/architecture.md`
- Implementation cross-check anchors:
  - `market_app/src/market_app/cli.py`
  - `market_app/src/market_app/offline_pipeline.py`
  - `market_app/scripts/run.ps1`
  - `market_app/scripts/run.sh`

## Execution evidence (current state)
1. Bootstrap + tests run from `market_app/` completed with failures:
   - `python -m pytest -q` -> `2 failed, 144 passed`
   - Failing tests:
     - `tests/test_no_network.py::test_network_guard_blocks_socket` (DNS resolution error instead of expected blocked runtime error)
     - `tests/test_ui_import.py::test_ui_import` (`ModuleNotFoundError: tkinter`)
2. Offline smoke execution:
   - `bash scripts/run.sh --run-id readiness_smoke` succeeded.
   - Artifacts written to `market_app/outputs/runs/readiness_smoke`.

## Contract alignment status

### 1) Offline-first and deterministic execution (`00_context.md`, `AGENTS.md`)
**Status: PARTIAL**
- ✅ Offline smoke run succeeds without network and falls back to bundled sample data when local inputs are missing.
- ✅ Deterministic sorting and required-output assertions are enforced in `offline_pipeline.py`.
- ⚠️ Baseline test suite has two failures, so repository is not test-clean.

### 2) Required run outputs (`20_output_contract.md`)
**Status: PARTIAL**
- ✅ `eligible.csv`, `scored.csv`, `report.md` are produced on smoke run.
- ⚠️ `eligible.csv` schema differs from contract:
  - Observed columns include `last_date`, `lag_days`, `lag_bin`, `stale`, `dq_flags`.
  - Expected contract columns include `theme_bucket`, `asset_type` and standardized gate reason codes.
- ⚠️ `scored.csv` schema differs from contract:
  - Observed score field is `monitor_score` (not `score_1to10`), and contract ML fields (`ml_signal`, `ml_model_id`, `ml_featureset_id`) are absent.

### 3) Data contracts (`10_data_contracts.md`)
**Status: PARTIAL**
- ✅ Per-symbol OHLCV ingestion exists and pipeline fails fast when required columns are missing.
- ✅ Manifest artifact is emitted (`manifest.json`) with run metadata.
- ⚠️ Current run output does not fully match codified manifest/content-hash structure described for data domains in `10_data_contracts.md`.

### 4) Commands and entrypoints (`40_commands.md`)
**Status: PARTIAL**
- ✅ Primary wrappers exist and work:
  - `market_app/scripts/run.ps1`
  - `market_app/scripts/run.sh`
- ⚠️ Documented Linux smoke command uses `--watchlist`, but `run.sh` does not accept that argument (returns `Unknown argument: --watchlist`).

### 5) Product/docs intent alignment (`application_overview.md`, `architecture.md`)
**Status: MOSTLY ALIGNED**
- ✅ Monitoring-only language and offline pipeline architecture are consistent with implementation.
- ✅ Pipeline emits broader artifacts (`universe.csv`, `classified.csv`, `features.csv`, `data_quality.csv`) in addition to required outputs.
- ⚠️ Several docs describe workflows and modes that are not fully reflected in the currently passing test/runtime surface.

## Readiness summary
- **Runtime readiness (offline smoke):** **Good**
- **Contract readiness (codex docs):** **Partial**
- **Test readiness (full suite):** **Not ready** (2 failing tests)
- **Documentation-command consistency:** **Needs cleanup**

## Recommended next actions (priority order)
1. Fix or quarantine the two baseline failing tests so the repo is test-clean.
2. Align `eligible.csv`/`scored.csv` schemas and gate reason naming with `docs/codex/20_output_contract.md` (or update the contract docs to match intended behavior).
3. Reconcile `docs/codex/40_commands.md` with actual `run.sh` CLI flags.
4. Confirm whether current manifest structure or `10_data_contracts.md` should be treated as source-of-truth, then align one to the other.
