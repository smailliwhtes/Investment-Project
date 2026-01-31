# 10_data_contracts — Data formats, schemas, partitioning, manifests

## A) OHLCV per-symbol CSV (required)
Location: configured directory (e.g., data/ohlcv/) containing one CSV per symbol: <SYMBOL>.csv

Required columns (case-insensitive):
- Date
- Open
- High
- Low
- Close

Preferred:
- Volume (may be missing; must not fail pipeline)
Optional:
- Adj Close / AdjClose (use if present, else Close)

Rules:
- Parse Date as YYYY-MM-DD preferred, accept common formats.
- Sort ascending by Date.
- Deduplicate same-day rows deterministically.
- Fail fast if core OHLC missing.

## B) Watchlist CSV (required)
Location: watchlists/watchlist_core.csv (default), watchlists/watchlist_smoke.csv (for tests)

Required headers:
- symbol, theme_bucket, asset_type

Normalization:
- symbol = uppercase + trimmed
Allowed asset_type:
- ETF, equity, trust, ETN

## C) Local manifests (required)
Each cache directory must have a manifest JSON:
- path: data/<domain>/manifest.json
- includes: schema_version, created_utc, source, coverage (min_date/max_date), row_counts, checksum/schema_hash

## D) GDELT offline datasets (planned; exported via BigQuery or ingest)
We will treat GDELT as offline tables stored locally (prefer Parquet) partitioned by day:
- data/gdelt/events/day=YYYY-MM-DD/part-*.parquet
- data/gdelt/gkg/day=YYYY-MM-DD/part-*.parquet

Minimal columns to retain (events):
- day/date, event identifiers, actor fields, event_code, goldstein_scale, geos, mentions count (as available)

Minimal columns to retain (gkg):
- day/date, document identifiers, themes, entities/organizations/persons (as available), tone-like fields

Important:
- Strict no-leakage joining: when creating features for date D, only use GDELT rows with timestamps <= D.

## E) BigQuery export notes (for provisioning)
- Public datasets are accessed under bigquery-public-data, and dataset location constraints apply (commonly US multi-region).
- Use SQL exports filtered by date range and only required columns to control cost.

GDELT’s events/mentions/GKG tables are available in Google BigQuery, and BigQuery public datasets have defined dataset locations/constraints (e.g., US multi-region).
