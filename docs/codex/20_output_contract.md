# 20_output_contract — Required outputs per run

All runs write to: outputs/<run_id>/ where run_id is a timestamp or unique id.

## 1) eligible.csv (always produced)
Required columns:
- symbol (string)
- eligible (bool)
- gate_fail_reasons (string; pipe-delimited; empty if eligible)
- theme_bucket (string)
- asset_type (string)

Gate reason codes (minimum set):
- MISSING_OHLC
- HISTORY_LT_MIN
- PRICE_LT_FLOOR
- LIQUIDITY_LT_FLOOR (only if Volume exists)
- WATCHLIST_INVALID_ROW

## 2) scored.csv (always produced; for eligible and/or all, but must be documented)
Required columns:
- symbol (string)
- score_1to10 (int in [1..10])
- risk_flags (string; pipe-delimited; empty allowed)
- explanation (string; one-line, short)
- theme_bucket (string)
- asset_type (string)
- last_date (YYYY-MM-DD)
- lag_days (int; calendar-day lag vs run as-of frontier / last_date_max)
- staleness_days_at_run (int; calendar-day lag vs run timestamp anchor: finished_at preferred, else started_at)
- ml_signal (float; nullable; model output only when available)
- ml_model_id (string; nullable; training identifier)
- ml_featureset_id (string; nullable; dataset schema/config hash)

Risk flags (minimum set):
- volume_missing
- high_volatility
- deep_drawdown

## 3) report.md (always produced)
Must include:
- Run metadata (run_id, config used)
- Counts: total watchlist, eligible, ineligible
- Top N by score
- Compact per-symbol lines: symbol | score | flags | one-line explanation

## 4) run_manifest.json (required)
Required for successful runs.

Must include `data_freshness` object:
- `last_date_max` (YYYY-MM-DD)
- `worst_lag_days` (int)
- `median_lag_days` (number)
- `staleness_days_at_run` (int, computed at run time from run timestamp anchor; do not compute a read-time `..._now` field)

## 5) config_snapshot.yaml (required)
Exact run config snapshot used for the run.

## 6) logs
- logs/engine.log (required)
- ui_engine.log (required; may be empty for direct CLI runs)

## Optional artifacts (allowed)
- features_*.csv / intermediate debug files
Optional artifacts must NOT replace the required outputs.
