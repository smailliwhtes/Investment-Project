# Fix Plan — Offline Monitor Upgrade

## Checklist (ordered)
1. **Introduce a local offline pipeline + config loader** (risk: medium)
   - Add a `schema_version: v2` config path with CLI/env/YAML precedence.
   - Implement local ingestion, feature engineering, gates, scoring, reporting, and manifest output.
2. **Add bundled sample data + fallback behavior** (risk: low)
   - Commit a tiny symbols listing + 3 OHLCV CSVs under `tests/data`.
   - Add a packaged copy under `market_app/src/market_app/sample_data` for offline demo mode.
3. **Update scripts + docs for new CLI contract** (risk: low)
   - Make `scripts/run.ps1` a thin wrapper around `python -m market_app.cli`.
   - Align `scripts/acceptance.ps1` with the offline acceptance requirements.
   - Update `README.md` with new offline/online behavior and required outputs.
4. **Add required tests** (risk: medium)
   - Unit tests for symbol loader, OHLCV loader, features, and gates/flags.
   - Integration smoke test for CLI offline run with sample data.
5. **Enforce output schemas + manifest hashing strategy** (risk: low)
   - Validate required columns and include schema versions + input hashes in `manifest.json`.

## Status
- [x] Implemented local offline pipeline and config loader.
- [x] Added sample data and fallback behavior.
- [x] Updated scripts and README.
- [x] Added unit and integration tests.
- [x] Enforced output schema checks and manifest hashing.
