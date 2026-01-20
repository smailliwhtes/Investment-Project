# FIX_PLAN

## Proposed PR-sized Steps

1. **Config & Offline Wiring**
   - Validate config.yaml + env override paths on Windows.
   - Add helper to auto-resolve external data roots.

2. **Data Ingestion & Cache Hardening**
   - Extend NASDAQ daily provider to support symbol discovery and schema validation logs.
   - Add cache inspection tooling for parquet QA.

3. **Feature & Scoring Expansion**
   - Add more attention proxies and sector-specific macro joins (beyond silver).
   - Expand data quality flags and penalties.

4. **Prediction Diagnostics**
   - Add gradient boosting model with leakage-safe tuning.
   - Add reliability table outputs to CSV.

5. **Reporting & UX**
   - Include run-time config echo + resolved watchlist in reports.
   - Add markdown tables for scenario sensitivity per symbol.

6. **Testing & CI**
   - Add unit tests for offline provider + macro parsing.
   - Add CI workflow with offline-safe checks.