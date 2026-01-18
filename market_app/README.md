# Market Monitor (Monitoring-Only)

Market Monitor is a **monitoring-only** system for U.S.-listed stocks and ETFs. It **does not** place orders or provide investment recommendations. Outputs are focused on:

- Eligibility gates (with reason codes)
- Monitor priority score (1–10) with component breakdown
- Risk flags (RED/AMBER with reason codes)
- Scenario sensitivity (defense/tech/metals)
- Optional historical analog distributions (future extension)

## One-Command Run (PowerShell)

From the repo root:

```powershell
.\doctor.ps1
```

This will:
1) Create a Python 3.11 venv
2) Install dependencies
3) Run diagnostics
4) Run the pipeline

## CLI Usage

```powershell
# Validate config
python -m market_monitor validate --config config.json

# Create default config
python -m market_monitor init-config --out config.json

# Run watchlist mode (default)
python -m market_monitor run --config config.json --mode watchlist

# Run full universe
python -m market_monitor run --config config.json --mode universe

# Run themed mode
python -m market_monitor run --config config.json --mode themed --themes defense,tech
```

## Staged Ingestion (Required)

Market Monitor uses staged ingestion to reduce API load:

- **Stage 0**: Universe filtering (no OHLCV)
- **Stage 1**: Micro-history (N1=7) to apply `price_max` gate and proxy liquidity
- **Stage 2**: Short history (N2=60) for true gates (ADV20$, zero volume, history sufficiency)
- **Stage 3**: Deep history (N3=600) for full features, risk flags, and scoring

## Providers

Supported providers are:
- **Stooq** (default, history-only, no API key)
- **Twelve Data** (history-only, credit-limited, uses `TWELVEDATA_API_KEY`)
- **Alpha Vantage** (history-only, credit-limited, uses `ALPHAVANTAGE_API_KEY`)
- **Finnhub** (quote + optional history; uses `FINNHUB_API_KEY`)

If a selected provider is missing credentials or blocked, the system falls back to the configured chain and logs a plain-language explanation.

## Bulk CSV Downloader (Design + Stubs)

A bulk historical CSV downloader design (including module stubs and a novice-first roadmap) lives in:

- `docs/bulk_downloader.md`
- `docs/product_roadmap.md`

## Outputs

Each run writes:

- `outputs/features_<run_id>.csv`
- `outputs/scored_<run_id>.csv`
- `outputs/eligible_<run_id>.csv`
- `outputs/report_<run_id>.md`

## Offline Tests

```powershell
pytest
```

Tests are offline-friendly and run against fixtures in `tests/fixtures`.

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
python -m market_monitor doctor --config config.json
```

The doctor command explains failures in plain English and points to the logs directory.

## Example Runs

Small watchlist scan:

```powershell
python -m market_monitor run --config config.json --mode watchlist
```

Full staged scan:

```powershell
python -m market_monitor run --config config.json --mode universe
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

## Adding a Provider

Implement `HistoryProvider` in `market_monitor/providers/` with explicit capability flags:
- `supports_history`, `supports_quote`, `supports_adjusted`, `rate_limit_model`

Then register it in `market_monitor/cli.py` and add any required env vars to the doctor checks.
