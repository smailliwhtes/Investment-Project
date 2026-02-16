# Repo Audit — GUI launch readiness (2026-02-16)

**Reference materials consulted:** `AGENTS.md`, `README.md`, `docs/codex/20_output_contract.md`, `GUI design guidelines.pdf`, `Market monitor blueprint.pdf`, `Market screening plan.pdf`, `Research AI blueprint.pdf`, `FIX_PLAN.md`, and existing test projects.

## Repository inventory (high-signal map)
- **Root coordination docs:** `AGENTS.md` (contract), `FIX_PLAN.md`, `AUDIT.md` (this file), blueprint PDFs.
- **Python engine (source of truth):** `market_app/`
  - `src/market_app/` main offline-first pipeline and CLI (`cli.py`, `offline_pipeline.py`, `outputs.py`).
  - `market_monitor/` legacy helpers still referenced (determinism, hashing, universe).
  - `config/` templates (`config.yaml`, `watchlists.yaml`, `logging.yaml`, `sources.yaml`).
  - `scripts/` PowerShell helpers (`run.ps1`, `provision_data.ps1`, `acceptance.ps1`, smoke/e2e runners).
  - `tests/` offline fixtures and unit tests; `pytest.ini` for defaults.
- **GUI stack (MAUI skeleton, Windows-first):** `src/gui/`
  - `MarketApp.Gui/` MAUI shell (App.xaml, MainPage.xaml, MauiProgram.cs).
  - `MarketApp.Gui.Core/` contracts (chart + secrets abstractions).
  - `MarketApp.Gui.Tests/` xUnit tests (contracts only) and `GlobalUsings.cs`.
  - Solution: `src/gui/MarketApp.Gui.sln`; helper build scripts live under `scripts/` in repo root.
- **Auxiliary artifacts:** `tests` (repo-root offline e2e runner), `docs/codex/*` for data/output/command contracts, PDF reference guides in root.
- **No nested AGENTS.md overrides** were found.

## Verification runs on this host (Linux CI runner)
- `cd market_app && python -m pytest -q` → **fails**: `No module named pytest` (dependency not preinstalled).
- `dotnet restore src/gui/MarketApp.Gui.sln` → **ok**.
- `dotnet build src/gui/MarketApp.Gui.sln -c Release` → **fails**: MAUI base types (`Application`, `ContentPage`, `MauiApp`) missing because MAUI workload is not supported on this platform (`dotnet workload install maui` is unsupported here). Expect success on Windows with MAUI workload installed.

## Readiness for GUI/UI launch
- **Strengths / assets**
  - MAUI solution exists with Windows target (`net8.0-windows10.0.19041.0`) and contract abstractions (`IChartProvider`, `ISecretsStore`, forecast/series models) aligned with `AGENTS.md` §10–11.
  - Output/data contracts are documented (`docs/codex/20_output_contract.md`) and the engine writes required columns including `last_date` / `lag_days`.
  - PowerShell and bash runners exist for offline runs; determinism utilities are present.
- **Gaps preventing a user-friendly GUI launch**
  - GUI is only a placeholder page; no run orchestration, progress parsing, run discovery, or CSV/manifest loaders are wired up (requirements in `AGENTS.md` §§4, 6, 9 are unmet).
  - No DI or services registered in `MauiProgram` (Engine bridge, Run discovery, CSV loader, Secrets store, Chart provider are absent).
  - Secrets storage and chart provider abstractions have no concrete implementations.
  - Progress JSONL throttling/parsing, cancellation wiring, and log tailing are not implemented.
  - Build/test on non-Windows hosts currently fails due to unsupported MAUI workload; CI must target `windows-latest` as documented.
  - Python test runner missing dependency on this host (`pytest` not installed), so engine test status is unknown without installing dev requirements.

## Recommendations (minimal next steps to be launch-ready)
1. Implement GUI services + DI registrations per `AGENTS.md` (engine process bridge honoring `--offline --progress-jsonl`, run discovery/manifest parsing, CSV loaders with `last_date/lag_days` enforcement, secrets store using SecureStorage, chart provider behind `IChartProvider`).
2. Expand GUI UI to required screens (dashboard, run orchestration, runs history/diff, scored table with virtualization, settings/logs) guided by `GUI design guidelines.pdf`.
3. Add GUI contract tests (progress parsing, run discovery, CSV merge) and platform build to `windows-latest` CI; keep Linux build optional or gated.
4. Ensure Python dev dependencies (`pytest`, pandas, numpy, etc.) are installed when running engine tests; optionally add a helper to bootstrap a local venv.

**Verdict:** The repository contains the core engine and a MAUI skeleton, but the GUI is not yet feature-complete nor wired to the engine. Additional GUI services, integration, and Windows-hosted builds/tests are required before a user-friendly interface can be shipped.
