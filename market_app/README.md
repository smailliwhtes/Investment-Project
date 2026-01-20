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
.\run_all.ps1
```

This will:
1) Create a Python 3.11 venv
2) Install dependencies (and optional dev deps)
3) Run tests (`python -m pytest -q`)
4) Run the pipeline (watchlist by default) into a timestamped outputs folder

Optional flags (examples):

```powershell
.\run_all.ps1 -Mode universe
.\run_all.ps1 -Mode themed -Themes defense,tech
.\run_all.ps1 -Mode batch -BatchSize 200 -BatchCursorFile .\data\state\batch_state.json
```

## CLI Usage

```powershell
# Validate config
python -m market_monitor validate --config config.yaml

# Create default config
python -m market_monitor init-config --out config.yaml

# Run watchlist mode (default)
python -m market_monitor run --config config.yaml --mode watchlist

# Run full universe
python -m market_monitor run --config config.yaml --mode universe

# Run themed mode
python -m market_monitor run --config config.yaml --mode themed --themes defense,tech
```

## Staged Ingestion (Required)

Market Monitor uses staged ingestion to reduce API load:

- **Stage 0**: Universe filtering (no OHLCV)
- **Stage 1**: Micro-history (N1=7) to apply `price_max` gate and proxy liquidity
- **Stage 2**: Short history (N2=60) for true gates (ADV20$, zero volume, history sufficiency)
- **Stage 3**: Deep history (N3=600) for full features, risk flags, and scoring

## Providers

Supported providers are:
- **NASDAQ daily (offline)** via external per-ticker CSVs (`NASDAQ_DAILY_DIR`)
- **Stooq** (online, history-only, no API key)
- **Twelve Data** (history-only, credit-limited, uses `TWELVEDATA_API_KEY`)
- **Alpha Vantage** (history-only, credit-limited, uses `ALPHAVANTAGE_API_KEY`)
- **Finnhub** (quote + optional history; uses `FINNHUB_API_KEY`)

If a selected provider is missing credentials or blocked, the system falls back to the configured chain and logs a plain-language explanation.

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
- `outputs/predictions_<run_id>.csv` (if prediction enabled)
- `outputs/run_report.md` + `outputs/run_report_<run_id>.md`
- `outputs/model_card.md` (if prediction enabled)
- `outputs/calibration_plot.png` (if prediction enabled)

## External Data Paths (Offline)

Set either `config.yaml` or environment variables:

- `MARKET_APP_DATA_ROOT`
- `NASDAQ_DAILY_DIR`
- `SILVER_PRICES_CSV`
- `OFFLINE_MODE` (true/false, defaults to true)

See `.env.example` and `config.example.yaml` for templates.

## Offline Tests

```powershell
pytest
```

Tests are offline-friendly and run against fixtures in `tests/fixtures`.

## Acceptance Script (Offline)

```powershell
.\scripts\acceptance.ps1
```

The acceptance script creates a venv, installs requirements quietly, runs pytest, and (if
`NASDAQ_DAILY_DIR` or `MARKET_APP_DATA_ROOT` is set) runs a watchlist pipeline and verifies outputs.

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

Copy `.env.example` to `.env` and set keys as needed:

- `FINNHUB_API_KEY`
- `FINNHUB_WEBHOOK_SECRET`
- `TWELVEDATA_API_KEY`
- `ALPHAVANTAGE_API_KEY`

## Acceptance Test (Fresh Clone)

Run the fresh-clone verification (creates venv, installs deps, runs tests, runs a watchlist pipeline, and checks connectivity):

```powershell
.\acceptance_test.ps1
```

## Adding a Provider

Implement `HistoryProvider` in `market_monitor/providers/` with explicit capability flags:
- `supports_history`, `supports_quote`, `supports_adjusted`, `rate_limit_model`

Then register it in `market_monitor/cli.py` and add any required env vars to the doctor checks.
