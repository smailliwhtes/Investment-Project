# AGENTS.md — market_app / market_monitor (Engine + MAUI GUI)

This file is the authoritative “how to work in this repo” contract for coding agents (Codex) and reviewers. Treat it as binding. If you find a conflict with existing docs, call it out and propose a small PR to reconcile.

## 0) Prime directive
This is an OFFLINE-FIRST market monitoring application. The Python engine is the source of truth. The .NET MAUI GUI is a thin orchestration + visualization layer over the engine. Do not duplicate scoring/eligibility logic in the UI.

## 1) Repository expectations (non-negotiable)
1) Offline-first: no mandatory runtime internet calls. Tests must be hermetic and runnable offline.
2) Determinism: stable output naming, stable schemas, seeded randomness, content hashing for artifacts.
3) Platform:
   - Python engine must run on Windows and on Linux/macOS CI runners.
   - MAUI GUI is Windows-first; CI must build/test it on `windows-latest`.
4) Change scope: one task = one PR. Keep changes small and testable.
5) Contracts are sacred:
   - CLI signatures, progress JSONL schema, and output artifacts are treated as public APIs.
   - Breaking changes require explicit migration notes + tests + doc updates.

## 2) Repo map (where things live)
- Repo root (Windows dev): `C:\Users\micha\OneDrive\Desktop\Investment-Project`
- Python app root: `market_app/`
- Engine/CLI module namespace: `market_monitor` (and/or `market_app`, depending on existing structure)
- Primary PowerShell entrypoints:
  - Engine run: `scripts/run.ps1`
  - Provisioning: `scripts/provision_data.ps1` (optional)
- Watchlist runner: `tools/run_watchlist.py`
- GUI solution root: `src/gui/`
  - `src/gui/MarketApp.Gui/` (MAUI app)
  - `src/gui/MarketApp.Gui.Core/` (Core MVVM/services/contracts)
  - `src/gui/MarketApp.Gui.Tests/` (unit/contract tests)
- GUI helper scripts:
  - `scripts/build_gui.ps1`
  - `scripts/run_gui.ps1`
- GUI spec document (must read for UI work):
  - `GUI design guidelines.pdf` (repo root copy; also uploaded to the project chat)

## 3) Authoritative “after changes” commands
### 3.1 Python engine validation
From repo root (PowerShell or bash):
- `cd market_app`
- `python -m pytest -q`

Optional but recommended:
- `python -m compileall -q .`

### 3.2 Offline smoke run (engine)
A run must be possible without internet given local OHLCV data:
- `python -m market_monitor.cli run --config config/config.yaml --out-dir outputs/runs/_smoke --offline --progress-jsonl`

Expected:
- Exit code 0
- Artifacts created (see Section 6)
- `scored.csv` includes `last_date` and `lag_days` (see Section 6.4)

### 3.3 MAUI GUI build/test (Windows)
From repo root:
- `dotnet restore src/gui/MarketApp.Gui.sln`
- `dotnet build src/gui/MarketApp.Gui.sln -c Release`
- `dotnet test  src/gui/MarketApp.Gui.Tests/MarketApp.Gui.Tests.csproj -c Release`

If CI needs it:
- `dotnet workload restore src/gui/MarketApp.Gui.sln`
(or install workload explicitly in workflow: `dotnet workload install maui --ignore-failed-sources`)

## 4) Engine ⇄ GUI integration contract (must remain stable)
The MAUI GUI is a client of the engine via CLI + files. The engine must support these commands and outputs.

### 4.1 Required CLI commands (engine)
1) Run pipeline:
- `python -m market_monitor.cli run --config <path> --out-dir <dir> --offline --progress-jsonl`

Required flags:
- `--config <path>`: config YAML path
- `--out-dir <dir>`: output directory to create/write artifacts
- `--offline`: never attempt network calls (engine must not require internet)
- `--progress-jsonl`: emit JSONL progress events to stdout (and optionally to a file)

2) Validate config:
- `python -m market_monitor.cli validate-config --config <path> --format json`

Required:
- `--format json` returns machine-readable errors.

### 4.2 Exit codes (engine)
- `0`: success (all required artifacts produced)
- `2`: invalid config / schema validation failure
- `3`: missing required input data (e.g., OHLCV dir missing)
- `4`: runtime failure during pipeline stages
- `130`: interrupted/canceled (SIGINT-like semantics)
Never return 0 on partial failure.

### 4.3 Progress JSONL schema (engine stdout)
When `--progress-jsonl` is enabled, engine emits one JSON object per line (UTF-8). The GUI parses these.

Minimal event schema:
- `ts` (string, ISO-8601 UTC preferred)
- `type` (string enum): `stage_start | stage_progress | stage_end | artifact_emitted | warning | error`
- `stage` (string): canonical stage id
- `pct` (number 0..1, optional for non-progress events)
- `message` (string, human readable)
- `counters` (object, optional): `{"done": int, "total": int, "units": "tickers|rows|files|..."}`

Artifact event (type = artifact_emitted) adds:
- `artifact` (object):
  - `name` (string): e.g., `scored.csv`
  - `path` (string): relative to out-dir
  - `rows` (int, optional)
  - `hash` (string, optional, sha256)

Error event adds:
- `error` (object):
  - `code` (string): stable error code
  - `detail` (string)
  - `traceback` (string, optional; keep short; full in logs)

Throttling requirement:
- Engine OR GUI must throttle updates (e.g., max 10 events/sec) to prevent UI freezes.

### 4.4 Cancellation semantics (GUI → engine)
The GUI cancels runs by terminating the process. Engine should:
- stop promptly,
- flush logs,
- exit with 130.

### 4.5 Logs
- Engine writes its log to `<out_dir>/logs/engine.log` (preferred) OR `<out_dir>/engine.log`.
- GUI captures stdout/stderr to `<out_dir>/ui_engine.log`.
Never print secrets to logs.

## 5) Run folder structure and manifest (reproducibility)
A run is a reproducible unit identified by `run_id`. The GUI browses and compares runs.

### 5.1 Canonical run directory convention
Preferred:
- `outputs/runs/<run_id>/`

However, any `--out-dir` is acceptable as long as it contains the required artifacts.

### 5.2 Required manifest
`<out_dir>/run_manifest.json` must exist for every successful run.

Manifest schema (minimum):
- `run_id` (string)
- `started_at` (string ISO-8601)
- `finished_at` (string ISO-8601)
- `duration_s` (number)
- `app` (object):
  - `name` (string) = `market_monitor`
  - `version` (string, git sha preferred)
- `environment` (object):
  - `python_version` (string)
  - `platform` (string)
  - `timezone` (string)
- `config` (object):
  - `path` (string, relative path if possible)
  - `hash_sha256` (string)
- `counts` (object):
  - `universe_count` (int)
  - `eligible_count` (int)
- `artifacts` (array of objects):
  - `name` (string)
  - `path` (string, relative to out_dir)
  - `rows` (int, optional)
  - `hash_sha256` (string, optional)
- `data_freshness` (object) (required when available):
  - `worst_lag_days` (int)
  - `median_lag_days` (number)
  - `stale_count_over_threshold` (int)
  - `last_date_max` (string YYYY-MM-DD)

### 5.3 Config snapshot
`<out_dir>/config_snapshot.yaml` must exist and be exactly what the run used. Hash in manifest must match.

## 6) Output contracts (CSV + artifacts)
These are the minimum required outputs for every run (unless explicitly stated otherwise). Fail fast if missing.

### 6.1 Required artifacts for every run
- `<out_dir>/scored.csv`
- `<out_dir>/eligible.csv` (if eligibility is separate; otherwise document which file contains eligibility)
- `<out_dir>/report.md` (or `report.txt` if markdown not used)
- `<out_dir>/run_manifest.json`
- `<out_dir>/config_snapshot.yaml`
- `<out_dir>/ui_engine.log` (GUI capture) and `<out_dir>/logs/engine.log` (engine log)

### 6.2 Optional-but-supported artifacts
- `<out_dir>/data_quality.csv`
- `<out_dir>/flags.csv`
- `<out_dir>/explain/` (per-symbol explainability packs)
- `<out_dir>/progress.jsonl` (if engine also writes progress file)

### 6.3 `eligible.csv` minimal columns
At minimum:
- `symbol` (string)
- `eligible` (bool or 0/1)
- `reasons` (string; semicolon-separated; optional but recommended)

### 6.4 `scored.csv` minimal columns (contract)
At minimum:
- `symbol` (string)
- `score` (number)
- `rank` (int) OR deterministic sort rule documented if rank not persisted
- `flags_count` (int, optional but recommended)
- `theme_labels` (string, optional; delimiter documented)
- `gates_passed` (bool/0-1 or string)
- **`last_date` (YYYY-MM-DD)**  ← REQUIRED
- **`lag_days` (int)**          ← REQUIRED

“Killer requirement”:
- `last_date` and `lag_days` must be present in `scored.csv` for debugging staleness/gaps.
- If engine computes these in `data_quality.csv`, engine must merge them into `scored.csv` (preferred).
- If GUI merges, it must be deterministic, tested, and documented; missingness must be a hard error.

### 6.5 `data_quality.csv` recommended columns
- `symbol`
- `last_date`
- `lag_days`
- `row_count`
- `has_volume` (bool)
- `missing_days` (int)
- `quality_flags` (string)

### 6.6 Explainability output (`explain/`)
Explain packs are used by the GUI “Explain” tab and run compare “why changed” summaries.

Preferred:
- `<out_dir>/explain/<symbol>.json` (one per symbol)

Minimal per-symbol explain schema:
- `symbol` (string)
- `run_id` (string)
- `as_of` (string YYYY-MM-DD)
- `score` (number)
- `contributions` (array):
  - `feature` (string)
  - `value` (number)
  - `normalized` (number)
  - `weight` (number)
  - `contribution` (number)
  - `direction` (`pos|neg`)
- `gates` (array):
  - `name` (string)
  - `passed` (bool)
  - `detail` (string)
- `flags` (array):
  - `name` (string)
  - `severity` (`info|warn|risk`)
  - `detail` (string)
- `theme_evidence` (array, optional):
  - `theme` (string)
  - `evidence` (string)
  - `ts` (string, ISO-8601 optional)

If true SHAP is not available:
- Provide a deterministic “contribution proxy” (e.g., weight * normalized_feature) and label it clearly in metadata:
  - `contribution_method`: `proxy_weighted_normalized`

## 7) Run comparison (diff) contract
The GUI compares two runs.

Engine should provide either:
A) A CLI helper:
- `python -m market_monitor.cli diff-runs --run-a <dirA> --run-b <dirB> --format json --out <path>`
or
B) A deterministic file-based diff produced by GUI using scored.csv + explain packs.

Diff JSON schema (minimum):
- `run_a` (run_id)
- `run_b` (run_id)
- `summary`:
  - `n_symbols` (int)
  - `n_new` (int)
  - `n_removed` (int)
  - `n_rank_changed` (int)
- `rows` (array):
  - `symbol`
  - `rank_a` (int or null)
  - `rank_b` (int or null)
  - `score_a` (number or null)
  - `score_b` (number or null)
  - `delta_score` (number or null)
  - `delta_rank` (int or null)
  - `flags_a` (int or null)
  - `flags_b` (int or null)
  - `drivers` (array of strings, optional; derived from explain packs)

## 8) Config contract and validation
- Config is YAML (authoritative), validated by engine.
- Engine validation must return structured JSON errors.

`validate-config --format json` response schema:
- `valid` (bool)
- `errors` (array):
  - `path` (string): dotted path, e.g., `data.ohlcv_dir`
  - `message` (string)
  - `severity` (`error|warning`)

GUI requirement:
- GUI must block “Run” if `valid == false`.
- GUI must present errors with field context.

## 9) MAUI GUI architecture and constraints
### 9.1 Solution structure (must remain)
Under `src/gui/`:
- `MarketApp.Gui`:
  - App shell, pages, DI, platform-specific (Windows-only allowed if isolated)
- `MarketApp.Gui.Core`:
  - Models, services, ViewModels, abstractions/interfaces
- `MarketApp.Gui.Tests`:
  - Unit tests + contract tests (progress parsing, run discovery, CSV merge)

### 9.2 MAUI build rules (avoid CI failures)
- Only `MarketApp.Gui` sets `<OutputType>Exe</OutputType>`.
- `MarketApp.Gui` must include:
  - `<UseMaui>true</UseMaui>`
  - `<TargetFramework>net8.0-windows10.0.19041.0</TargetFramework>` (Windows-first)
  - `<SingleProject>true</SingleProject>`
  - `<EnableDefaultMauiItems>true</EnableDefaultMauiItems>`
- Test project must include:
  - `Microsoft.NET.Test.Sdk`
  - `xunit`
  - `xunit.runner.visualstudio`
- Prefer `src/gui/MarketApp.Gui.Tests/GlobalUsings.cs` with:
  - `global using Xunit;`
  - `global using System.IO;`

### 9.3 MVVM/DI conventions
- Use MVVM: ViewModels hold state/commands; Views are XAML shells.
- Use DI in `MauiProgram.CreateMauiApp()`:
  - Register EngineBridgeService, RunDiscoveryService, CsvLoader, SecretsStore, ChartProvider.
- Do not block UI thread:
  - All engine runs are `Task`-based.
  - Progress updates are throttled (e.g., coalesce to 10 updates/sec).

### 9.4 Engine discovery (GUI → python)
GUI must locate python interpreter in this order:
1) User-configured python path in GUI settings
2) Repo-local `.venv\Scripts\python.exe` (if present)
3) `python` on PATH

Set environment when spawning process:
- `PYTHONUTF8=1`
- `PYTHONIOENCODING=utf-8`

Capture stdout/stderr and parse JSONL progress lines.

### 9.5 UI performance requirements
- Symbol table must handle 5k–100k rows without hitching:
  - Use virtualization (DataGrid/CollectionView incremental load).
  - Never bind massive observable collections without paging/virtualization.
- Parsing CSV must be streaming/efficient; avoid loading entire files repeatedly.

### 9.6 Screens (minimum viable set)
1) Dashboard:
   - Run summary, data freshness (`last_date/lag_days`), cache health
   - Launch “New Run”
2) Run Orchestration:
   - Select config, select out-dir/run-id, Start/Cancel
   - Live progress + log tail
3) Runs History:
   - List runs by manifest; open; compare two runs
4) Universe/Scored view:
   - Virtualized table; filters; last_date/lag_days visible
   - Symbol detail pane with Explain tab
5) Settings:
   - Config path/profile management; validate-config integration
   - Secrets management
6) Logs:
   - Engine log + UI log viewer with copy diagnostics (redact secrets)

## 10) Charting requirements (MAUI)
Charting must be behind an abstraction to allow library swaps.

### 10.1 Abstraction
`IChartProvider` must exist in `MarketApp.Gui.Core` and be consumed by views:
- `View CreatePriceChart(PriceSeriesModel model, ForecastOverlayModel? forecast)`
- `View CreateIndicatorChart(IndicatorSeriesModel model)`

### 10.2 Default provider (MVP)
- Default: LiveCharts2 provider (if included).
- Optional: Syncfusion or Telerik providers only if packages/licenses present.
- Must support:
  - Candlestick (OHLC)
  - Overlay: actual close, forecast, prediction interval band
  - Tooltip/readout, zoom/pan
  - Decimation/downsampling for long series (target <= 2k–5k points plotted)

### 10.3 Forecast overlay model (contract)
- `trained_until` (DateTime)
- `horizon_points` (int)
- `yhat[]`, `lo[]`, `hi[]` arrays aligned to forecast timestamps
- Forecast display toggles: show/hide line, show/hide band

## 11) Secrets (no plaintext)
Secrets must be stored via OS-backed secure storage.

### 11.1 Abstraction
`ISecretsStore`:
- `Task SetAsync(string key, string value)`
- `Task<string?> GetAsync(string key)`
- `Task RemoveAsync(string key)`

### 11.2 Default implementation
- Use MAUI SecureStorage.
- Never write secrets to config files, run folders, or logs.
- Diagnostics copy must redact values.

Keys expected:
- `FINNHUB_API_KEY`
- `TWELVEDATA_API_KEY`
- `ALPHAVANTAGE_API_KEY`

## 12) CI/workflows
### 12.1 Python CI (cross-platform)
- Must remain green on ubuntu-latest and windows-latest.
- No network dependencies in tests.

### 12.2 GUI CI (Windows)
Workflow: `.github/workflows/gui-windows-build.yml` must:
- Setup .NET 8
- Restore workloads if needed:
  - `dotnet workload restore src/gui/MarketApp.Gui.sln` (preferred)
  - or `dotnet workload install maui --ignore-failed-sources`
- Build + test:
  - `dotnet build ... -c Release`
  - `dotnet test  ... -c Release`

## 13) Testing expectations (high signal)
### 13.1 Python tests
- Any CLI changes require tests.
- Add/maintain tests for:
  - validate-config JSON format
  - progress JSONL emission
  - required artifacts existence
  - scored.csv includes last_date/lag_days

### 13.2 GUI tests (must exist)
At minimum in `MarketApp.Gui.Tests`:
- Progress JSONL parsing tests:
  - valid lines parse
  - bad lines handled (warning + continue, unless fatal)
- Run discovery tests:
  - finds runs by `run_manifest.json`
  - handles missing/invalid manifests with clear errors
- CSV load/merge tests:
  - scored.csv requires last_date/lag_days OR deterministic merge from data_quality.csv
  - missingness is a hard failure

## 14) Documentation updates required when touching contracts
If you change any of:
- CLI flags / signatures
- Progress JSONL schema
- Required artifacts or columns
You must update:
- this `AGENTS.md`
- `docs/codex/20_output_contract.md`
- any relevant `docs/gui_*` files
- tests that enforce the contract

## 15) Review guidelines (what is P0 vs P1)
P0 (must fix before merge):
- Wrong exit codes / silent success
- Non-deterministic outputs without justification
- Breaking CLI/artifact/progress contracts
- Introducing mandatory network dependencies
- Missing tests for touched modules
- Missing `last_date/lag_days` visibility in scored flow

P1 (should fix):
- Weak error messages (missing file/row context)
- Missing docs updates for new flags/files
- UI regressions (unthrottled progress spam, non-virtualized table)
- Logging secrets or leaking sensitive info

## 16) Pre-coding checklist (agent must do)
Before making changes, explicitly confirm:
1) You read this AGENTS.md.
2) You searched for nested AGENTS.md in subdirectories and noted overrides (if any).
3) You identified which contract(s) will be touched (CLI/progress/artifacts/GUI).
4) You listed the exact commands you will run to verify success (Python + GUI).

If anything here conflicts with the repo’s current reality, do NOT guess silently—call it out and propose the minimal reconciliation PR.
