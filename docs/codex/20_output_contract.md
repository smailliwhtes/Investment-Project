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

## Optional artifacts (allowed)
- run_manifest.json (metadata)
- features_*.csv / intermediate debug files
Optional artifacts must NOT replace the three required outputs.
