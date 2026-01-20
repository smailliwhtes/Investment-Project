# Application Overview: Market Monitor

Market Monitor is a **monitoring-only** pipeline for U.S.-listed stocks and ETFs. It is designed to
filter a universe or watchlist, score eligible symbols, and emit structured CSV outputs and reports.
It does **not** place orders or provide investment recommendations.

## What It Does (Capabilities)

- **Scans watchlists or the full universe** using staged ingestion (micro/short/deep history) to
  reduce data load while progressively applying gates.
- **Applies eligibility gates** (price caps, liquidity, zero-volume constraints, and history
  sufficiency) and records reason codes.
- **Scores symbols** with weighted components (trend, momentum, liquidity, quality, volatility and
  drawdown penalties, attention, theme bonus, etc.).
- **Flags risk** (RED/AMBER) and provides scenario sensitivity (defense/tech/metals).
- **Emits artifacts** including scored/eligible/features CSVs plus a markdown run report; optional
  model artifacts are available if prediction is enabled.
- **Runs preflight checks** before feature computation, emitting coverage and data-quality summaries.

## Operational Modes

For this milestone, **offline watchlist mode is the only supported mode**. Universe, themed, and
batch modes remain in the codebase but are disabled in offline runs to prevent any network access.

## Readiness Checklist (Quick Start)

1. **Python 3.11** installed (PowerShell scripts target 3.11).
2. **Config created** (`config.yaml`) from `config.example.yaml` and tuned for your data paths.
3. **Data provider ready**:
   - Offline: set `MARKET_APP_NASDAQ_DAILY_DIR` (or the config path) to point to per-ticker CSVs.
4. **Run tests** (`pytest`) to validate the offline fixtures.
5. **Execute the pipeline** (e.g., `run_all.ps1` or `python -m market_monitor run`).

## What It Does *Not* Do

- Execute trades or place orders.
- Provide investment advice or recommendations.

## Artful Note

Think of Market Monitor as a **quiet lighthouse**: it doesn’t steer the ship, but it does cast a
clear beam over the market’s surface so you can choose your own course.
