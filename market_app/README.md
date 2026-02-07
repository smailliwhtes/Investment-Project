# Market Monitor (Monitoring-Only)

Market Monitor is a **monitoring-only** system for U.S.-listed stocks and ETFs. It **does not** place orders or provide investment recommendations. Outputs are focused on:

- Eligibility status (with reason codes)
- Monitor priority score (1–10) with component breakdown
- Risk flags (RED/AMBER with reason codes)
- Scenario sensitivity (defense/tech/metals)
- Offline corpus context + historical analog summaries

## Run the monitor (canonical command)

From the repo root:

**Windows (PowerShell)**
```powershell
python -m market_app.cli run --config .\config.yaml --offline
```

**macOS/Linux (bash)**
```bash
python -m market_app.cli run --config ./config.yaml --offline
```

This runs the canonical wrapper (`python -m market_app.cli run`) and writes
output artifacts to `outputs/runs/<run_id>/` (or `--runs-dir` if provided).

## Doctor checks

Validate environment + config (no run performed):

**Windows (PowerShell)**
```powershell
python -m market_app.cli doctor --config .\config.yaml
```

**macOS/Linux (bash)**
```bash
python -m market_app.cli doctor --config ./config.yaml
```

The doctor command verifies required paths (config, watchlist, data dirs) and
warns about optional assets like macro series files or missing theme watchlists.

### Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Doctor fails with missing NASDAQ daily data | Offline mode without local OHLCV fixtures | Point `paths.nasdaq_daily_dir` at `tests/fixtures/ohlcv` or your own dataset. |
| Doctor fails with missing watchlist | `paths.watchlist_file` not found | Update the config path or create the watchlist file. |
| Warnings about macro series | Optional macro CSVs not present | Add the CSVs under `paths.data_dir` or remove the entries from config. |

## Acceptance gate (fresh-clone proof)

These scripts clone the repo to a temp directory, create a venv, install deps,
run tests, run doctor, run the pipeline twice, and check determinism via
`digest.json`.

**Windows (PowerShell)**
```powershell
.\scripts\acceptance_gate.ps1
```

**macOS/Linux (bash)**
```bash
bash scripts/acceptance_gate.sh
```

## Verify ready for GUI

Runs the canonical CLI in offline mode twice and confirms deterministic `digest.json` output.
From the repo root:

```powershell
.\market_app\scripts\verify_ready_for_gui.ps1
```

The verifier checks:
- acceptance config exists and CLI is reachable
- fixtures look repo-local (warns on absolute paths)
- doctor + two runs complete with required artifacts
- `digest.json` matches across runs

## Run minimal GUI

Launch the offline-first Tkinter wrapper:

```powershell
python -m market_app.ui
```

Buttons and expected artifacts:
- **Validate (doctor)**: runs `python -m market_app.cli doctor` and streams output.
- **Run pipeline**: runs `python -m market_app.cli run` and streams output into the log view.
- **Open latest report**: opens `report.html`, `report.md`, or `report.pdf` from the newest run folder.
- **Open latest log**: opens `run.log` (or `logs/run.log`) from the newest run folder.
- **Open runs folder**: opens the configured runs directory in your file explorer.

## Day-to-day wrapper (Windows)

```powershell
.\scripts\run_monitor.ps1 -Config .\config.yaml -Offline
```

## Windows setup (LF fixtures + bootstrap)

This repo expects **LF line endings** for fixtures to keep golden manifests stable across
Windows and Linux. `.gitattributes` already enforces LF under `tests/fixtures/**`, but you
should also configure your local git settings to avoid CRLF drift:

```powershell
git config core.autocrlf false
git config core.eol lf
```

Bootstrap the Windows dev/test environment from the repo root:

```powershell
.\scripts\bootstrap.ps1
```

## Makefile-Equivalent Commands (PowerShell)

```powershell
# install dependencies
python -m pip install -r .\requirements.txt

# run tests
python -m pytest -q

# run the blueprint wrapper (offline)
python -m market_app.cli run --config .\config.yaml --offline --top-n 15 --conservative

# run acceptance (fresh clone flow)
.\scripts\acceptance_gate.ps1
```

## Legacy One-Command Run (PowerShell)

From the repo root (offline by default):

```powershell
.\scripts\acceptance.ps1
```

This will:
1) Create a Python 3.11 venv
2) Install dependencies (and optional dev deps)
3) Run tests (`python -m pytest -q`)
4) Run doctor + preflight diagnostics
5) Run the offline watchlist pipeline into a timestamped outputs folder
6) Verify run outputs and manifest are present

Optional flags (examples):

```powershell
.\scripts\acceptance.ps1 -Config .\config.yaml
```

## CLI Usage

```powershell
# Blueprint wrapper (preferred)
python -m market_app.cli run --config config.yaml --offline --run-id demo_run --top-n 15 --conservative

# Legacy Market Monitor CLI
# Validate config
python -m market_monitor validate --config config.yaml

# Create default config
python -m market_monitor init-config --out config.yaml

# Run watchlist mode (default)
python -m market_monitor run --config config.yaml --mode watchlist

# Run preflight only
python -m market_monitor preflight --config config.yaml
```

## Staged Ingestion (Required)

Market Monitor uses staged ingestion to reduce API load:

- **Stage 0**: Universe filtering (no OHLCV)
- **Stage 1**: Micro-history (N1=7) to validate data presence
- **Stage 2**: Short history (N2=60) for data status and risk flags
- **Stage 3**: Deep history (N3=600) for full features, risk flags, and scoring

## Providers (Offline-Only)

This release supports **offline-only** watchlist runs using:

- **NASDAQ daily (offline)** via per-ticker CSVs stored locally.

Online providers remain in the codebase for future milestones, but offline mode is the only supported
mode here. Any network access attempts are hard-failed during offline runs and tests.

## Offline Security Master + Metadata Pack

Build a canonical offline `security_master.csv` from the Stooq TXT pack and local metadata
snapshots. This is the **metadata provisioning** layer that stays offline during normal runs.

- Runbook: `docs/offline_metadata_pack.md`
- MetaStock notes: `docs/stooq_metastock_notes.md`

Quick start (fixtures):

```powershell
python tools/build_security_master.py --stooq-root tests/fixtures/stooq_txt --output out/security_master.csv --filter-required
```

## One Command (Offline Fixtures + Security Master)

```powershell
.\scripts\offline_one_command.ps1
```

This creates a venv, installs dependencies, builds `out/security_master.csv` from fixtures, and
runs one offline monitor pass.

## Offline Corpus: GDELT Conflict Events (Local CSV)

The corpus pipeline ingests locally stored GDELT conflict event CSVs and produces daily context
features, analogs, and event-impact summaries. The pipeline is entirely offline and will skip
corpus enrichment if the corpus folder is empty or missing.

- Drop the Kaggle GDELT conflict CSV into your corpus folder (this is the scalable historical base).
- Optional post-2021 extension: place manually downloaded GDELT Events ZIPs under
  `corpus/gdelt_events_raw/` (offline only).
- Daily features are written to `outputs/corpus/daily_features.csv` and
  `outputs/corpus/daily_features.parquet`.
- Analogs report: `outputs/corpus/analogs_report.md`
- Event impact library: `outputs/corpus/event_impact_library.csv` (when baseline/watchlist data exist)

The GDELT web Event Record Exporter is capped to 20,000 results per query and is not used for
full-history builds. Prefer the Kaggle CSV and raw Events ZIPs for offline scale.

### Corpus CLI

```powershell
python -m market_monitor corpus validate --config config.yaml
python -m market_monitor corpus build --config config.yaml
```

### Corpus Folder Layout (Offline)

```
<corpus_root>/
  gdelt_conflict_1971_2021.csv
  gdelt_events_raw/
    20220101.export.CSV.zip
    20220102.export.CSV.zip
```

### Offline Evaluation

```powershell
python -m market_monitor evaluate --config config.yaml
```

Outputs:

- `outputs/eval/eval_metrics.csv`
- `outputs/eval/eval_report.md`

## Bulk CSV Downloader (Design + Stubs)

A bulk historical CSV downloader design (including module stubs and a novice-first roadmap) lives in:

- `docs/bulk_downloader.md`
- `docs/product_roadmap.md`

Actual bulk storage locations + lifecycle are documented here:

- `docs/bulk/WHERE_DATA_LIVES.md`

## Outputs

Each run writes:

- `outputs/features_<run_id>.csv`
- `outputs/scored_<run_id>.csv`
- `outputs/eligible_<run_id>.csv`
- `outputs/preflight_report.csv` + `outputs/preflight_report.md`
- `outputs/run_manifest.json`
- `outputs/predictions_<run_id>.csv` (if prediction enabled)
- `outputs/run_report.md` + `outputs/run_report_<run_id>.md`
- `outputs/corpus/daily_features.csv` (if corpus configured)
- `outputs/corpus/daily_features.parquet` (if corpus configured)
- `outputs/corpus/analogs_report.md` (if corpus configured)
- `outputs/corpus/event_impact_library.csv` (if corpus configured)
- `outputs/corpus/corpus_manifest.json` (if corpus configured)
- `outputs/corpus/corpus_index.json` (if corpus configured)
- `outputs/corpus/corpus_validate.json` (if corpus configured)
- `outputs/eval/eval_metrics.csv` (if evaluation run)
- `outputs/eval/eval_report.md` (if evaluation run)
- `outputs/model_card.md` (if prediction enabled)
- `outputs/calibration_plot.png` (if prediction enabled)

## External Data Paths (Offline)

`config.yaml` is the canonical configuration. `config.json` is deprecated for this offline-only
milestone and will be ignored in favor of `config.yaml` defaults.

Set either `config.yaml` (prefer the `data_roots` section) or environment variables:

- `MARKET_APP_DATA_ROOT`
- `MARKET_APP_OHLCV_DIR` (canonical OHLCV root)
- `MARKET_APP_NASDAQ_DAILY_DIR` (legacy, still supported)
- `MARKET_APP_SILVER_PRICES_DIR`
- `MARKET_APP_CORPUS_ROOT`
- `MARKET_APP_GDELT_DIR` (optional, canonical)
- `MARKET_APP_GDELT_CONFLICT_DIR` (optional override, legacy)
- `MARKET_APP_GDELT_EVENTS_RAW_DIR` (optional override)
- `MARKET_APP_OUTPUTS_DIR` (optional override)
- `OFFLINE_MODE` (true/false, defaults to true)

See `.env.example` and `config.example.yaml` for templates. The Kaggle folders should be placed
locally on your machine (for example, under `C:\Users\<YOU>\OneDrive\Desktop\Kaggle_datasets\`)
and referenced via the env vars or config file.

## Offline Tests

```powershell
pytest
```

Tests are offline-friendly and run against fixtures in `tests/fixtures`.

## Optional GPU Acceleration (Prediction Training)

Prediction training defaults to CPU-only scikit-learn models. To opt into XGBoost (CPU by
default, GPU when available), set:

- `MARKET_APP_PREDICTION_BACKEND=xgboost`
- `MARKET_APP_ENABLE_GPU=1` (optional; uses `gpu_hist` when supported)

## Fixtures (Deterministic Offline Data)

Fixture inputs live under `tests/fixtures` so the offline pipeline is repeatable without network
access or API keys:

- `tests/fixtures/watchlist.txt` (AAA/BBB/SPY)
- `tests/fixtures/data/universe_universe.csv`
- `tests/fixtures/data/state/batch_state.json`
- `tests/fixtures/ohlcv/{AAA,BBB,SPY}.csv` (deterministic 300-row OHLCV)

To regenerate the OHLCV fixtures deterministically:

```powershell
python -m market_monitor.fixtures.ohlcv_generator --outdir tests/fixtures/ohlcv
```

Canonical offline fixtures one-liner:

```powershell
python -m market_monitor doctor --config tests/fixtures/minimal_config.yaml --offline
python -m market_monitor run --config tests/fixtures/minimal_config.yaml --mode watchlist --outdir $env:TEMP\market_audit --offline
python -m market_monitor evaluate --config tests/fixtures/minimal_config.yaml --outdir $env:TEMP\market_audit --offline
```

## Acceptance Script (Offline)

```powershell
.\scripts\acceptance.ps1
```

Note: Do not commit outputs/data/venv artifacts; acceptance runs a git hygiene guard. If you hit a
Codex diff size limit, run `.\scripts\git_hygiene_check.ps1` and ensure forbidden paths are untracked.

The acceptance script creates a venv, installs requirements quietly, runs pytest, runs doctor + preflight,
executes an offline watchlist pipeline, and verifies outputs + manifest.

## Setup (Venv + Dependencies)

```powershell
.\setup.ps1
```

Or manually:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r .\requirements.txt
```

## Diagnostics

```powershell
python -m market_monitor doctor --config config.yaml
```

The doctor command explains failures in plain English and points to the logs directory.

To skip connectivity checks (offline mode):

```powershell
$env:MM_OFFLINE = "1"
python -m market_monitor doctor --config config.yaml
```

## Example Runs

Small watchlist scan:

```powershell
python -m market_monitor run --config config.yaml --mode watchlist
```

Full staged scan:

```powershell
python -m market_monitor run --config config.yaml --mode universe
```

## Tooling

Lint, format, type-check, security, and tests:

```powershell
ruff check .
black .
mypy market_monitor
bandit -r market_monitor
pytest
```

Install pre-commit hooks:

```powershell
pre-commit install
```

## Environment Variables

Copy `.env.example` to `.env` and set paths as needed:

- `MARKET_APP_NASDAQ_DAILY_DIR`
- `MARKET_APP_SILVER_PRICES_DIR` (optional)
- `MARKET_APP_CORPUS_ROOT` (optional)
- `MARKET_APP_GDELT_CONFLICT_DIR` (optional)
- `OFFLINE_MODE` (true/false)

API keys are ignored in offline mode and are retained only for future milestones.

## Data Sources (Offline Only)

Data must be downloaded manually and stored locally. Reference materials:

- GDELT 2.1 Event Database Codebook (fields + schema): https://www.gdeltproject.org/data.html
- GDELT 2.1 Event data (conflict event extracts): https://www.gdeltproject.org/data.html

## Acceptance Test (Fresh Clone)

Run the fresh-clone verification (creates venv, installs deps, runs tests, runs doctor + preflight, runs a watchlist pipeline):

```powershell
.\scripts\acceptance.ps1
```

## Adding a Provider

Implement `HistoryProvider` in `market_monitor/providers/` with explicit capability flags:
- `supports_history`, `supports_quote`, `supports_adjusted`, `rate_limit_model`

Then register it in `market_monitor/cli.py` and add any required env vars to the doctor checks.
