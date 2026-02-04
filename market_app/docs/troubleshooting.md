# Troubleshooting

## Missing symbols in OHLCV

**Symptoms:** `MISSING_OHLC` gate failures or warnings in the run manifest.

**Fix:** Ensure normalized OHLCV files exist in `data/ohlcv_daily/` (or set `paths.ohlcv_daily_dir`).
If you only have raw data, run the OHLCV doctor:

```bash
python -m market_monitor.ohlcv_doctor normalize --raw-dir data/ohlcv_raw --out-dir data/ohlcv_daily
```

## Date parsing failures

**Symptoms:** Normalization drops many rows or reports missing required prices.

**Fix:** Check the input date column name or pass `--date-col` to the doctor. Confirm dates are parseable and deterministic.

## Empty exogenous coverage

**Symptoms:** `exogenous_coverage` is 0 in `run_manifest.json`.

**Fix:** Verify `data/exogenous/daily_features/day=<YYYY-MM-DD>/part-00000.csv` exists for the as-of date, or point `--exogenous-daily-dir` to your cache.

## Missing volume

**Symptoms:** `volume_missing` risk flag, `avg_dollar_vol` is null, or liquidity gates are skipped.

**Fix:** Provide volume in the OHLCV input, or lower/disable `scoring.average_dollar_volume_floor` in `config.yaml`.
