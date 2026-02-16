# MAUI GUI Dev Guide

## Structure
- `src/gui/MarketApp.Gui`: MAUI UI shell/pages and platform wiring.
- `src/gui/MarketApp.Gui.Core`: MVVM models/services/abstractions.
- `src/gui/MarketApp.Gui.Tests`: core unit tests (Windows CI target).

## Build
```powershell
./scripts/build_gui.ps1
```

## Run
```powershell
./scripts/run_gui.ps1
```

## Engine bridge behavior
- Launches Python using configured path / `.venv` / PATH fallback.
- Sets `PYTHONUTF8=1` and `PYTHONIOENCODING=utf-8`.
- Calls:
  - `market-monitor run --config ... --out-dir ... --progress-jsonl`
  - `market-monitor validate-config --config ... --format json`
- Writes UI-side log to `<run_dir>/ui_engine.log`.

## Chart provider switch
`ChartProvider` setting supports:
- `LiveCharts2` (default)
- `Syncfusion` (optional)
- `Telerik` (optional)

## Staleness indicators
Always surface `last_date` and `lag_days` in symbol tables and run status summaries.
