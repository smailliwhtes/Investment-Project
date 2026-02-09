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

## Persistent environment overrides

**Symptoms:** Tests or offline runs keep resolving to an unexpected OHLCV/output directory even after editing `config.yaml`.

**Cause:** Environment variables like `MARKET_APP_NASDAQ_DAILY_DIR` and `MARKET_APP_OHLCV_DIR` override config paths.
On Windows, values set with `setx` persist into new shells until you clear them.

**Fix:** Inspect and clear overrides in the current session, then restart the shell if you used `setx`:

```powershell
Get-ChildItem Env:MARKET_APP_* | Format-Table -AutoSize
Remove-Item Env:MARKET_APP_NASDAQ_DAILY_DIR -ErrorAction SilentlyContinue
Remove-Item Env:MARKET_APP_OHLCV_DIR -ErrorAction SilentlyContinue
```

**Tests are insulated:** The test suite now clears `MARKET_APP_*` overrides automatically. If you need to reproduce a path override issue in a single test run, set the env var in that test (or temporarily set it in your shell) and re-run the target test.

If you previously ran `setx MARKET_APP_NASDAQ_DAILY_DIR ...`, clear it with:

```powershell
setx MARKET_APP_NASDAQ_DAILY_DIR ""
```

## Searching logs and reports on Windows

Use PowerShell’s built-in `Select-String` to search logs or CSVs without extra tools:

```powershell
Select-String -Path .\outputs\logs\run_*.jsonl -Pattern "INSUFFICIENT_HISTORY"
```

If you prefer ripgrep (`rg`), install it with `winget install BurntSushi.ripgrep` and use:

```powershell
rg "INSUFFICIENT_HISTORY" .\outputs\logs
```
