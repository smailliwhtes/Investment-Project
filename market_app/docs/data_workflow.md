# Data Workflow (Offline, Stooq-based)

## Canonical folders
- Raw Stooq dumps: any staged folder containing `*.us.txt` files.
- Canonical OHLCV cache: CSV-per-symbol folder (for example `ohlcv_daily_csv`) with schema:
  `date,open,high,low,close,volume`.
- Available universe output: `market_app/data/universe_available/universe.csv`.

## Recommended update flow
1. Incrementally update canonical cache from a new Stooq dump.
2. Rebuild `universe_available` as the intersection of source universe and cache symbols.
3. Run the offline pipeline against the canonical cache and available universe.

## Commands
```powershell
python tools/stooq_incremental_updater.py --existing-csv-dir "<ohlcv_cache_dir>" --new-stooq-dir "<stooq_dump_dir>" --out-csv-dir "<ohlcv_cache_dir>" --recursive
python tools/build_universe_available.py --ohlcv-dir "<ohlcv_cache_dir>" --universe-in ".\data\universe\universe.csv" --out-dir ".\data\universe_available"
python -m market_app.cli --config .\config\config.yaml --offline --ohlcv-dir "<ohlcv_cache_dir>" --symbols-dir ".\data\universe_available" --run-id "offline_YYYYMMDD_HHMMSS"
```

Or use the single wrapper:

```powershell
.\scripts\update_and_run.ps1 -StooqDumpDir "<stooq_dump_dir>" -OhlcvDir "<ohlcv_cache_dir>"
```

## Artifacts
Each run emits:
- `outputs/<run_id>/eligible.csv`
- `outputs/<run_id>/scored.csv`
- `outputs/<run_id>/report.md`
- `outputs/<run_id>/data_quality.csv`

Updater emits manifest JSON in cache directory:
- `stooq_incremental_update_manifest_<timestamp>.json`.

Converter emits:
- `conversion_manifest.json`.
