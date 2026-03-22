# GUI/UI Readiness Audit (Repository Snapshot)

Date: 2026-03-21  
Scope: Validate whether the current repository contains the assets and contracts required to run the MAUI GUI/UI against the Python engine.

## Update

- `EngineBridgeService`, run discovery, and contract-focused GUI tests are now present.
- The GUI now includes a thin `Policy Simulator` screen that shells out to `python -m market_monitor.cli policy simulate ... --offline`.
- Policy simulation remains additive to the core engine contracts; the GUI is still orchestrating the Python engine rather than duplicating logic.

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
  - `policy simulate --config ... --scenario ... --outdir ... --offline`
  - Progress JSONL emission helper
  - Required artifacts enforcement (`scored.csv`, `eligible.csv`, `report.md`, `run_manifest.json`, `config_snapshot.yaml`)
  - `scored.csv` freshness hardening (`last_date`, `lag_days`) with deterministic merge/fallback.
- GUI orchestration services are present in the current codebase:
  - `EngineBridgeService`
  - `RunDiscoveryService`
  - `RunCompareService`
  - `QualityMetricsService`
- Contract-oriented GUI tests exist for:
  - progress JSONL parsing
  - engine path resolution and process environment
  - run discovery / dashboard artifact loading
  - policy simulator request wiring

## Remaining gaps

1. **Environment-dependent build check could not be completed in this Linux container**
   - `dotnet` is not installed in this environment, so GUI restore/build/test commands could not be executed here.

2. **Policy simulator is intentionally thin**
   - The current GUI page shells out to the engine and surfaces the generated summary/report payloads.
   - It does not yet stream live policy-stage progress or render policy charts/tables beyond the textual summary.

3. **Deep-learning work remains a design seam, not a shipped backend**
   - The repo now documents a clean deep-learning integration path, but the production backend is still the classical offline ML lane.

## Contract and docs drift to reconcile

- Root AGENTS map has been updated for the watchlist runner path and now documents the additive `policy simulate` CLI.
- Root README examples now use the public `market_monitor.cli` command family consistently.

## Practical conclusion

- **Repository now satisfies the core "GUI orchestrates real engine runs" contract for the implemented offline engine lanes**, including contract runs, run discovery, comparison, and the new policy simulator.
- The next practical step is not architecture rescue; it is refinement: richer policy visualization, optional deep-learning backends behind the existing engine contracts, and Windows-side build verification in a provisioned MAUI environment.
