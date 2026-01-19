# Repo Audit Update (Monitoring-Only)

> Scope: `market_app` repository. This update validates monitoring-only guardrails, bulk downloader behavior, data locations, and reproducible Windows PowerShell runners.

## Executive Summary

- ✅ The system remains **monitoring-only** (eligibility, risk flags, priority scoring, scenario sensitivity, reports). No buy/sell guidance is introduced.
- ✅ Bulk downloader is functional via CLI (`bulk-plan`, `bulk-download`, `bulk-standardize`) and now has a connectivity check in `doctor`.
- ✅ One-command runner and fresh-clone acceptance test are now explicit PowerShell scripts.
- ⚠️ Known gaps: bulk sources depend on upstream availability; optional API keys are still required when providers are configured as primary.

## Coverage Checklist (A)

1) **Executive purpose & guardrails** ✅
   - README and report outputs emphasize monitoring-only (no trading advice).

2) **Configuration & environment expectations** ✅
   - `config.json` defines providers, bulk paths, cache, and outputs.
   - `.env` is gitignored; `.env.example` remains safe to share.

3) **Tooling & quality gates** ✅
   - `pyproject.toml`, `mypy.ini`, and existing lint/test guidance are intact.

4) **Core pipeline modules** ✅
   - Staged pipeline (stage 1/2/3), gates, features, scoring, risk, scenarios, reporting.

5) **CLI orchestration + logging** ✅
   - CLI entrypoints, JSONL run logs, deterministic outputs in `outputs/`.

6) **Universe ingestion + watchlist** ✅
   - Watchlist and Nasdaq universe ingestion are intact.

7) **Providers + HTTP retry logic** ✅
   - Retry/backoff in `providers/http.py`; budgets and fallback chain.

8) **Bulk downloader subsystem** ✅
   - Planner, manifest, registry, downloader, standardize are functional.

9) **Utilities/scripts/legacy runners** ✅
   - Existing `run.ps1`, `run_universe.ps1`, `doctor.ps1` remain.
   - New `run_all.ps1` and `acceptance_test.ps1` are canonical.

10) **Tests + fixtures** ✅
    - Offline-friendly tests, bulk manifest tests intact.

11) **Data assets + watchlists + state** ✅
    - `data/`, `inputs/`, and `state` paths are documented.

12) **Audit artifacts + file manifest + command log** ✅
    - `docs/audit/file_manifest.json` updated with new files.

## Directory Summary (High-Level)

- `market_monitor/`: core pipeline (staging, scoring, risk, reporting, providers, bulk).
- `docs/`: audit + bulk lifecycle + roadmap documentation.
- `scripts/`, `tools/`, `py/`: helper scripts and wrappers.
- `data/`, `inputs/`, `outputs/`: storage for universe, watchlists, cache, and run outputs.

## One-Command Runner (B)

New canonical PowerShell runner:

```powershell
.\run_all.ps1
```

- Creates `.venv` if missing
- Installs requirements (+ dev requirements if present)
- Runs `python -m pytest -q`
- Runs the pipeline (watchlist by default)
- Writes outputs to `outputs\runs\run_<UTC_TIMESTAMP>`

## Bulk Download Verification + Storage Clarity (C)

- **Entry points:** `python -m market_monitor bulk-plan`, `bulk-download`, `bulk-standardize`
- **Storage details:** documented in `docs/bulk/WHERE_DATA_LIVES.md`
- **Connectivity checks:** `python -m market_monitor doctor --config config.json`
  - Optional offline skip via `MM_OFFLINE=1`
  - Strict mode (`--strict`) treats connectivity warnings as errors

## Acceptance Test (D)

Fresh-clone PowerShell verification:

```powershell
.\acceptance_test.ps1
```

This script installs dependencies, runs tests, performs connectivity checks, runs a watchlist pipeline, and validates output artifacts.

## Gaps / Risks

- Provider connectivity is dependent on external services; strict mode is recommended in CI but optional locally.
- Bulk downloads overwrite files unless archived externally; manifests provide reproducibility but not auto-versioning.

## What Changed in This Update

- Added new PowerShell runners (`run_all.ps1`, `acceptance_test.ps1`).
- Added bulk storage/location doc (`docs/bulk/WHERE_DATA_LIVES.md`).
- Updated doctor diagnostics to show data directories, cache hit rate (when available), and bulk reachability.
- Added strict mode for connectivity checks.
- Updated README and audit artifacts.
