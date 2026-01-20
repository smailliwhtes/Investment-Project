# Offline Watchlist Checklist (Internal)

This checklist documents which modules are exercised in **offline watchlist mode** versus code that
remains present for future online/bulk workflows. It is intended as a living audit for maintenance.

## Used in Offline Watchlist Runs

- `market_monitor/cli.py` (run + preflight commands)
- `market_monitor/config_schema.py` + `market_monitor/data_paths.py` (config + env overrides)
- `market_monitor/offline.py` (hard network guard)
- `market_monitor/providers/nasdaq_daily.py` (offline CSV adapter)
- `market_monitor/preflight.py` (preflight reports)
- `market_monitor/staging.py` + `market_monitor/features.py` (feature computation)
- `market_monitor/gates.py`, `market_monitor/scoring.py`, `market_monitor/risk.py`
- `market_monitor/report.py` (markdown report)
- `market_monitor/manifest.py` (run manifest)
- `market_monitor/io.py` (deterministic CSV output)
- `market_monitor/macro.py` (optional silver macro enrichment)

## Present but Disabled in Offline Mode

These modules remain in the repository but are **hard-blocked** by `offline.py` when offline mode is
enabled:

- Online providers: `providers/stooq.py`, `providers/twelvedata.py`, `providers/alphavantage.py`,
  `providers/finnhub.py`
- Universe fetch: `market_monitor/universe.py` (network fetches guarded)
- Bulk download tooling: `market_monitor/bulk/`

## Notes

- Offline mode is required for this milestone; any network attempt throws immediately.
- Preflight reports are generated on every watchlist run and stored alongside outputs.
