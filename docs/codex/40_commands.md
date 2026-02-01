# 40_commands — Commands Codex must run and report

Supported Python versions: 3.10–3.13 (NumPy/Pandas wheels). Use the bootstrap scripts to enforce this range.

## Windows (local)
From repo root:

```powershell
cd market_app
```

### Bootstrap venv (Python 3.12/3.13 required)
.\scripts\bootstrap.ps1

### Install / run (offline)
.\scripts\run.ps1 -WatchlistPath .\watchlists\watchlist_smoke.csv -Offline

### Offline data correctness check (must fail nonzero if missing)
.\scripts\check_watchlist_ohlcv.ps1 -WatchlistPath .\watchlists\watchlist_smoke.csv -DataDir .\outputs\ohlcv_smoke

### ML train/predict (offline)
python -m market_monitor.ml.train_xgb --joined-path .\\data\\features\\joined --output-dir .\\outputs\\<run_id>
python -m market_monitor.ml.predict --joined-path .\\data\\features\\joined --output-dir .\\outputs\\<run_id>

## Linux/macOS (Codex cloud / CI)
From repo root:

```bash
cd market_app
```

### Bootstrap venv (Python 3.12/3.13 required)
bash scripts/bootstrap.sh

### Install / run (offline)
bash scripts/run.sh --watchlist watchlists/watchlist_smoke.csv

### Offline data correctness check (must fail nonzero if missing)
bash scripts/check_watchlist_ohlcv.sh --watchlist watchlists/watchlist_smoke.csv --data-dir outputs/ohlcv_smoke

### ML train/predict (offline)
python -m market_monitor.ml.train_xgb --joined-path data/features/joined --output-dir outputs/<run_id>
python -m market_monitor.ml.predict --joined-path data/features/joined --output-dir outputs/<run_id>

## Python test commands
Run from the `market_app` directory:
python -m pytest -q

### Environment doctor
python -m market_monitor.env_doctor --self-test

### Minimal smoke subset (if you keep explicit smoke tests)
python -m pytest -q tests/test_watchlist_validator.py tests/test_watchlist_smoke_pipeline.py

## Local GDELT (offline)
Run from the `market_app` directory:

```bash
python -m market_monitor.gdelt.profile --raw-dir "C:\\Users\\micha\\OneDrive\\Desktop\\NLP Corpus" --glob "*.csv"
python -m market_monitor.gdelt.ingest --raw-dir "C:\\Users\\micha\\OneDrive\\Desktop\\NLP Corpus" --out-dir data/gdelt --format events --glob "*.csv" --write csv
python -m market_monitor.gdelt.features_daily --gdelt-dir data/gdelt --out data/gdelt/features_daily.csv
python -m market_monitor.features.join_exogenous --market-path data/processed/market_daily_features.parquet --gdelt-path data/gdelt/features_daily.csv --out-dir data/features/joined --lags 1,3,7 --rolling-window 7 --rolling-mean --rolling-sum
```

## Validate Local Corpus (offline)
Run from the `market_app` directory:

```bash
python -m market_monitor.gdelt.doctor audit --raw-dir "C:\\Users\\micha\\OneDrive\\Desktop\\NLP Corpus" --glob "*.csv"
python -m market_monitor.gdelt.doctor normalize --raw-dir "C:\\Users\\micha\\OneDrive\\Desktop\\NLP Corpus" --gdelt-dir "C:\\Users\\micha\\OneDrive\\Desktop\\NLP Corpus_normalized" --format events --glob "*.csv" --write csv
python -m market_monitor.gdelt.doctor verify-cache --gdelt-dir "C:\\Users\\micha\\OneDrive\\Desktop\\NLP Corpus_normalized"
python -m market_monitor.gdelt.features_daily --gdelt-dir "C:\\Users\\micha\\OneDrive\\Desktop\\NLP Corpus_normalized" --out data/gdelt/features_daily.csv
```

## Expected outputs after run.ps1
- outputs/<run_id>/eligible.csv
- outputs/<run_id>/scored.csv
- outputs/<run_id>/report.md

## What to include in PR completion notes
- Exact commands run
- Terminal output (or summarized output + references)
- What changed and why
- Any known limitations
