# GUI/UI Readiness Audit (Repository Snapshot)

Date: 2026-02-25  
Scope: Validate whether the current repository contains the assets and contracts required to run the MAUI GUI/UI against the Python engine.

## What is present and ready

- GUI solution and expected project layout are present:
  - `src/gui/MarketApp.Gui.sln`
  - `src/gui/MarketApp.Gui/`
  - `src/gui/MarketApp.Gui.Core/`
  - `src/gui/MarketApp.Gui.Tests/`
- MAUI app project is configured as Windows-first and executable:
  - `TargetFramework=net8.0-windows10.0.19041.0`
  - `OutputType=Exe`
  - `UseMaui=true`
- Test project includes the expected xUnit stack and global usings (`Xunit`, `System.IO`).
- Root helper scripts for GUI build/run are present:
  - `scripts/build_gui.ps1`
  - `scripts/run_gui.ps1`
- Windows CI workflow exists for GUI build/test (`.github/workflows/gui-windows-build.yml`) and includes MAUI workload install + build + test steps.
- Engine CLI contract surface needed by GUI is implemented in code:
  - `run --config ... --out-dir ... --offline --progress-jsonl`
  - `validate-config --config ... --format json`
  - Progress JSONL emission helper
  - Required artifacts enforcement (`scored.csv`, `eligible.csv`, `report.md`, `run_manifest.json`, `config_snapshot.yaml`)
  - `scored.csv` freshness hardening (`last_date`, `lag_days`) with deterministic merge/fallback.

## Gaps that block full "GUI drives engine" readiness

1. **GUI is still demo/simulated rather than engine-bridged**
   - DI registers `SampleDataService` and `SimulatedRunOrchestrator`.
   - No `EngineBridgeService`, `RunDiscoveryService`, or `CsvLoader` implementation/registration found.
   - Result: GUI currently runs sample data and simulated progress instead of launching Python engine and parsing live JSONL.

2. **Python process discovery/spawn contract not implemented in GUI code**
   - No evidence of logic for interpreter discovery order:
     1) user setting
     2) repo-local `.venv\Scripts\python.exe`
     3) `python` on PATH
   - No evidence of required process environment settings:
     - `PYTHONUTF8=1`
     - `PYTHONIOENCODING=utf-8`

3. **Contract-focused GUI tests are incomplete**
   - Existing tests validate contract record shapes and sample-data behavior.
   - Missing explicit tests for:
     - progress JSONL parser resilience,
     - run discovery via `run_manifest.json`,
     - deterministic scored/data_quality merge failure semantics.

4. **Environment-dependent build check could not be completed in this Linux container**
   - `dotnet` is not installed in this environment, so GUI restore/build/test commands could not be executed here.

## Contract and docs drift to reconcile

- Root AGENTS map references `scripts/run.ps1` and `scripts/provision_data.ps1` as primary engine entrypoints, but those files exist under `market_app/scripts/` in this repository snapshot.
- Root README engine examples mix `market_monitor.cli` and `market_app.cli` command families; this should be normalized to the intended public contract.

## Practical conclusion

- **Repository has enough structure to build and run the MAUI shell on a properly provisioned Windows machine** (SDK + MAUI workload), but **does not yet fully satisfy the "GUI orchestrates real engine runs" contract**.
- To be fully ready for production-style GUI orchestration, next implementation step should add concrete engine bridge/discovery/csv/run-history services and corresponding contract tests.
