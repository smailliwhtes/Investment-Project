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

## AGENTS.md operational check (2026-02-16)

### Scope checked
- `AGENTS.md` contract at repo root.
- No nested `AGENTS.md` overrides were found.
- Compared AGENTS contract paths/commands with the current repository layout and CLI surfaces.

### Verification commands run
- `cd market_app && python -m pytest -q`  
  Result: failed in this environment (`No module named pytest`).
- `dotnet restore src/gui/MarketApp.Gui.sln`  
  Result: failed (`src/gui/MarketApp.Gui.sln` does not exist in this repo layout).
- `cd market_app && python -m market_monitor.cli run --config config/config.yaml --out-dir outputs/runs/_smoke --offline --progress-jsonl`  
  Result: failed in this environment (`No module named pandas`), and contract flags differ from current CLI.

### AGENTS contract vs repository reality
1. **GUI layout mismatch (P0 for AGENTS contract conformance):**
   - AGENTS requires `src/gui/MarketApp.Gui*` projects and `src/gui/MarketApp.Gui.sln`.
   - Current repo does not have `/src/gui`; it has a root `MarketApp.Gui.csproj` and Python/Tkinter UI flow under `market_app`.
2. **GUI workflow mismatch:**
   - `.github/workflows/gui-windows-build.yml` triggers/builds `src/gui/**` and references `scripts/build_gui.ps1`.
   - Those paths are absent in the current repository.
3. **Engine CLI contract mismatch:**
   - AGENTS requires `market_monitor.cli run --config --out-dir --offline --progress-jsonl`.
   - `market_monitor/cli.py` `run` currently requires `--watchlist` and `--run-id`; no `--out-dir` or `--progress-jsonl` flag is defined there.
   - AGENTS requires `validate-config --format json`; current `market_monitor/cli.py` exposes `validate`, not `validate-config`.
4. **Artifact naming mismatch:**
   - AGENTS requires `run_manifest.json` + `config_snapshot.yaml` for successful runs.
   - `market_app/src/market_app/cli.py` writes `manifest.json`; naming contract diverges depending on entrypoint.
5. **What appears aligned:**
   - Python CI runs on `ubuntu-latest` and `windows-latest` in `.github/workflows/ci.yml`.
   - `last_date` and `lag_days` are present in the offline pipeline code paths (`market_app/src/market_app/offline_pipeline.py` and related schemas).

### Operational verdict
- **Application status: Partially operational.**
- The Python engine appears actively maintained and test-backed, but the repository does **not** currently conform to the AGENTS.md MAUI/CLI path and command contracts as written.
- To call the app “fully operational” against AGENTS.md, the project needs a contract reconciliation:
  - either implement the AGENTS-specified `src/gui` + CLI/output contracts,
  - or update AGENTS.md to match the current, actual repository architecture and entrypoints.
- If you want, next I can do a follow-up pass to tighten parity even further (e.g., richer progress-jsonl stage granularity and stricter manifest field population) while keeping backward compatibility intact.
