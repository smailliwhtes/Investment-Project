# Repo Audit — Offline Monitor Upgrade

## Inventory
- **Repo root:** `/workspace/Investment-Project`
- **App root:** `market_app/`
- **Python package (src layout):** `market_app/src/market_app/`
- **Legacy pipeline package:** `market_app/market_monitor/`
- **Primary CLI:** `market_app/src/market_app/cli.py`
- **Scripts:** `market_app/scripts/` (includes `run.ps1`, `acceptance.ps1`, `provision_data.ps1`)
- **Configs/templates:** `market_app/config/` (`config.yaml`, `watchlists.yaml`, `logging.yaml`, `sources.yaml`)
- **Docs/contracts:** `docs/codex/10_data_contracts.md`, `docs/codex/20_output_contract.md`, `docs/codex/40_commands.md`
- **Nested AGENTS.md overrides:** none found under the repo tree.

## What is working today
- The legacy CLI (`market_app/src/market_app/cli.py`) runs a blueprint pipeline and writes outputs in
  `outputs/runs/<run_id>/` with `eligible.csv`, `scored.csv`, and `report.md`.
- The repo already contains extensive tests and fixtures under `market_app/tests/`.

## Gaps vs requirements (broken or missing)
- **Offline-first + config schema mismatch:** The local config template
  `market_app/config/config.yaml` previously used a legacy schema (watchlist, provider, and macro settings)
  and did not provide `paths.symbols_dir`, `paths.ohlcv_dir`, or new gating/scoring fields. This blocks the
  required `python -m market_app.cli --config config/config.yaml --offline --top_n 50` UX.
- **Symbols ingestion:** `market_app/market_monitor/universe.py` uses live Nasdaq endpoints and `requests`,
  which violates offline-first requirements when used without explicit `--online`.
- **Sample data fallback:** There was no tiny bundled dataset for missing external data paths; the system
  could crash when `MARKET_APP_*` directories were not configured.
- **Acceptance gate:** `market_app/scripts/acceptance.ps1` relied on legacy env vars
  (`MARKET_APP_NASDAQ_DAILY_DIR`) and did not run the offline pipeline required by the new config.
- **Schema contracts for new outputs:** No explicit schema list was enforced for the new output set
  (`universe.csv`, `classified.csv`, `features.csv`, etc.).
- **Tests for new requirements:** There were no unit/integration tests for the new offline symbol/ohlcv
  loaders, gates, and CLI smoke run.

## Missing artifacts
- **Tiny sample data:** required under `tests/data` (symbols + ohlcv) and used for offline demo mode.
- **Config templates:** a config with new `paths` and `gates` defaults aligned to the required CLI behavior.
- **Manifest strategy:** explicit input hashing policy for OHLCV sampling.
