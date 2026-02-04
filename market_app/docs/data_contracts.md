# Data Contracts

## OHLCV Canonical Schema

Normalized OHLCV daily files must use these columns:

Required:
- `date` (YYYY-MM-DD)
- `open`, `high`, `low`, `close` (float)

Optional:
- `volume` (int preferred; float allowed)
- `adj_close` (float)

### Duplicate-day rules
When multiple rows share the same date, aggregation is deterministic:

- `open`: first non-null
- `high`: max
- `low`: min
- `close`: last non-null
- `volume`: sum (null treated as 0)
- `adj_close`: last non-null

Missing required prices after aggregation will drop the row with a warning unless `--strict` is used.

## Exogenous Daily Schema

Exogenous daily features are expected to come from the GDELT doctor cache (`data/exogenous/daily_features/`).
See `market_monitor/gdelt/doctor.py` and the generated `features_manifest.json` for the authoritative schema.

## Results Schema

`results.csv` columns (stable ordering):

- `symbol`, `asof_date`, `theme_bucket`, `asset_type`
- `gates_passed` (bool)
- `failed_gates` (pipe-delimited)
- `priority_score` (int 1–10)
- `risk_flags` (pipe-delimited)
- `returns_20d`, `vol_20d`, `trend_50`, `rsi_14`, `avg_dollar_vol`, `regime_label`
- `explanation_1` ... `explanation_5`

`results.jsonl` mirrors the same data but with structured `metrics` and `explanations` arrays.

## Run Manifest Schema

`run_manifest.json` includes:

- `run_id`, `asof_date`, `started_utc`
- `config_hash`, `watchlist_content_hash`
- `ohlcv_manifest_hash`, `exogenous_manifest_hash`
- `code_version`, `determinism_fingerprint`
- resolved path map and warnings
