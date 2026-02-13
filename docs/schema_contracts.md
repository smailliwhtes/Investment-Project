# Local schema contracts (market_app)

This document defines the local offline pipeline boundary schemas enforced in `market_app/src/market_app/schemas_local.py`.

## Universe
Required columns:
- `symbol`
- `name`
- `exchange`
- `asset_type`
- `is_etf`

## OHLCV
Required columns:
- `date`
- `open`
- `high`
- `low`
- `close`
- `volume`

## DataQuality
Required columns:
- `symbol`
- `last_date`
- `as_of_date`
- `lag_days`
- `lag_bin`
- `n_rows`
- `missing_days`
- `zero_volume_fraction`
- `bad_ohlc_count`
- `stale_data`

`lag_days` uses **calendar-day lag**:
`lag_days = (as_of_date - last_date).days`

## Features
Required columns:
- `symbol`
- `return_1m`, `return_3m`, `return_6m`, `return_12m`
- `close_to_sma20`, `close_to_sma50`, `close_to_sma200`
- `volatility_20d`, `volatility_60d`
- `max_drawdown_6m`
- `adv20_usd`

## Score
Required columns:
- `symbol`
- `monitor_score`
- `total_score`
- `risk_flags`
- `last_date`
- `lag_days`
- `lag_bin`

`scored.csv` must carry `last_date` + `lag_days` so stale symbols can be sorted/debugged directly.
