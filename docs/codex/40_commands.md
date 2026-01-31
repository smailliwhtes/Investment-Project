# 40_commands — Commands Codex must run and report

## Windows PowerShell (preferred for repo scripts)
From repo root:

### Install / run (offline)
.\scripts\run.ps1 -WatchlistPath .\watchlists\watchlist_smoke.csv

### Offline data correctness check (must fail nonzero if missing)
.\scripts\check_watchlist_ohlcv.ps1 -WatchlistPath .\watchlists\watchlist_smoke.csv

## Python test commands
python -m pytest -q

### Minimal smoke subset (if you keep explicit smoke tests)
python -m pytest -q tests/test_watchlist_validator.py tests/test_watchlist_smoke_pipeline.py

## Expected outputs after run.ps1
- outputs/<run_id>/eligible.csv
- outputs/<run_id>/scored.csv
- outputs/<run_id>/report.md

## What to include in PR completion notes
- Exact commands run
- Terminal output (or summarized output + references)
- What changed and why
- Any known limitations
