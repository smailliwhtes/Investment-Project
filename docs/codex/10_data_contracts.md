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

## D) GDELT offline datasets (local ingest)
Local GDELT data is ingested offline from user-provided CSVs. The ingest step supports headered CSVs and raw
GDELT events files that are tab-delimited even if the extension is `.csv`. Outputs are partitioned by day:
- data/gdelt/events/day=YYYY-MM-DD/part-00000.parquet (or `.csv`)
- data/gdelt/gkg/day=YYYY-MM-DD/part-00000.parquet (or `.csv`)

Events canonical columns (snake_case):
- day, event_id, event_code, event_base_code, event_root_code, quad_class, goldstein_scale, avg_tone,
  num_mentions, num_sources, num_articles, actor1_country_code, actor2_country_code,
  actiongeo_country_code, source_url

GKG canonical columns (if present):
- datetime, document_identifier, themes, persons, organizations, locations, tone

Manifests:
- data/gdelt/events/manifest.json (schema_version, created_utc, raw_dir, file_glob, coverage, row_counts,
  columns, content_hash)
- data/gdelt/gkg/manifest.json (same structure as events)

Config/env:
- data_roots.gdelt_raw_dir (raw CSV input directory)
- data_roots.gdelt_dir (normalized cache output directory, default: data/gdelt)
- MARKET_APP_GDELT_RAW_DIR, MARKET_APP_GDELT_DIR override the paths above

Note: OneDrive/Dropbox sync can lock files. Ingestion opens files read-only and will fail with a message that
includes the locked path and a remediation step.

Important:
- Strict no-leakage joining: when creating features for date D, only use GDELT rows with day == D.

## E) BigQuery export notes (for provisioning)
- Public datasets are accessed under bigquery-public-data, and dataset location constraints apply (commonly US multi-region).
- Use SQL exports filtered by date range and only required columns to control cost.

GDELT’s events/mentions/GKG tables are available in Google BigQuery, and BigQuery public datasets have defined dataset locations/constraints (e.g., US multi-region).

## F) Joined market + GDELT daily features (PR5)
Join output location:
- data/features/joined/day=YYYY-MM-DD/part-00000.parquet
- data/features/joined/manifest.json

Schema (long-form):
- day (YYYY-MM-DD, join key)
- symbol (uppercase ticker)
- market_features... (all per-symbol OHLCV-derived daily features)
- gdelt_features... (daily GDELT features, plus lagged/rolling variants)

Lag/rolling rules:
- GDELT lags: per feature, t-1/t-3/t-7 (configurable)
- Optional rolling mean/sum for count-like features (default window 7)
- **No leakage**: only GDELT rows with day <= D may be used to build features for day D

Manifest fields:
- schema_version, created_utc
- coverage (min_day, max_day, n_days)
- row_counts (total_rows, rows_per_day)
- columns
- inputs (market_path, gdelt_path, file fingerprints)
- config (lags, rolling settings)
