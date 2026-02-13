# market_app audit report (offline + deterministic)

## AGENTS instruction check
- Loaded `/workspace/Investment-Project/AGENTS.md` before coding.
- No nested `AGENTS.md` overrides were found under the repository tree.

## A1) Entry points and one-command run
- Primary local wrapper entrypoint is `market_app/src/market_app/cli.py` with local-offline execution delegated to `run_offline_pipeline`.  
- Windows one-command wrapper remains `market_app/scripts/run.ps1` and Linux/macOS `market_app/scripts/run.sh`.
- Added `--as-of` alias for `--as-of-date` in CLI parsing to align with requested invocation style.

## A2) Pipeline stage map and artifacts
Observed stage sequence in `offline_pipeline.py`:
1. Load/filter symbol universe
2. Load OHLCV per symbol
3. Compute per-symbol features
4. Compute data quality/staleness
5. Apply gates
6. Score and rank
7. Write report + manifest

Artifacts now emitted by the local offline orchestrator:
- `universe.csv`
- `classified.csv`
- `features.csv`
- `data_quality.csv`
- `eligible.csv`
- `ineligible.csv`
- `scored.csv`
- `report.md`
- `manifest.json`

## A3) Staleness/data quality gap root cause and fix
- Root cause: data quality was previously minimal and `scored.csv` only carried raw `lag_days`/`last_date` from feature frame without explicit staleness bins or richer diagnostics.
- Fixes:
  - expanded data quality schema with `n_rows`, `missing_days`, `zero_volume_fraction`, `bad_ohlc_count`, and `lag_bin`.
  - merged these fields into features and included `lag_bin` in scoring output.
  - ensured `scored.csv` contains `last_date` and `lag_days` merged from the same quality path.

## A4) Symbol filtering (units/warrants/rights/prefs)
- Filtering lives in `market_app/src/market_app/symbols_local.py` via config-driven include flags.
- Pattern matching is based on symbol metadata (`name`, `asset_type`) and remains deterministic.
- Filter behavior verified by existing local symbol tests and full pipeline smoke tests.

## A5) OHLCV schema assumptions
- Canonical columns enforced at module boundaries:
  - required: `date, open, high, low, close`
  - optional: `volume, adj_close`
- Added explicit schema validator module for boundary checks.

## A6) Online-call audit and offline guard
- Offline block mechanism is in `market_app/src/market_app/offline_guard.py` and wraps local run execution in CLI.
- Core local monitor run path is network-independent and fixture/file-system based.
- Existing offline guard tests remain in place; run suite includes no-network checks.

## PDF reference-spec handling
- Attempted to parse the requested PDF specs from repo root.
- Environment lacks PDF extraction tooling (`pdftotext` unavailable) and package install is blocked by network proxy.
- Work was still aligned against existing codified contracts/docs and implemented in deterministic/offline-first style.
