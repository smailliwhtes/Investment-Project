# Market Monitor (Monitoring-Only)

Market Monitor is a **monitoring-only** system for U.S.-listed stocks and ETFs. It **does not** place orders or provide investment recommendations. Outputs are focused on:

- Eligibility gates (with reason codes)
- Monitor priority score (1–10) with component breakdown
- Risk flags (RED/AMBER with reason codes)
- Scenario sensitivity (defense/tech/metals)
- Optional historical analog distributions (future extension)

## One-Command Run (PowerShell)

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
- **Stage 1**: Micro-history (N1=7) to apply `price_max` gate and proxy liquidity
- **Stage 2**: Short history (N2=60) for true gates (ADV20$, zero volume, history sufficiency)
- **Stage 3**: Deep history (N3=600) for full features, risk flags, and scoring

## Providers (Offline-Only)

This release supports **offline-only** watchlist runs using:

- **NASDAQ daily (offline)** via per-ticker CSVs stored locally.

Online providers remain in the codebase for future milestones, but offline mode is the only supported
mode here. Any network access attempts are hard-failed during offline runs and tests.

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
- `outputs/model_card.md` (if prediction enabled)
- `outputs/calibration_plot.png` (if prediction enabled)

## External Data Paths (Offline)

Set either `config.yaml` or environment variables:

- `MARKET_APP_DATA_ROOT`
- `MARKET_APP_NASDAQ_DAILY_DIR`
- `MARKET_APP_SILVER_PRICES_DIR`
- `OFFLINE_MODE` (true/false, defaults to true)

See `.env.example` and `config.example.yaml` for templates. The Kaggle folders should be placed
locally on your machine (for example, under `C:\Users\<YOU>\OneDrive\Desktop\Kaggle_datasets\`)
and referenced via the env vars or config file.

## Offline Tests

```powershell
pytest
```

Tests are offline-friendly and run against fixtures in `tests/fixtures`.

## Acceptance Script (Offline)

```powershell
.\scripts\acceptance.ps1
```

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
- `OFFLINE_MODE` (true/false)

API keys are ignored in offline mode and are retained only for future milestones.

## Acceptance Test (Fresh Clone)

Run the fresh-clone verification (creates venv, installs deps, runs tests, runs doctor + preflight, runs a watchlist pipeline):

```powershell
.\scripts\acceptance.ps1
```

## Adding a Provider

Implement `HistoryProvider` in `market_monitor/providers/` with explicit capability flags:
- `supports_history`, `supports_quote`, `supports_adjusted`, `rate_limit_model`

Then register it in `market_monitor/cli.py` and add any required env vars to the doctor checks.
