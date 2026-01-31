# 30_pr_ladder — What to build, in what order, with acceptance tests

Principle: one PR = one measurable step. Each PR must:
- update docs if flags/files change
- include/adjust tests
- run commands in 40_commands.md and paste terminal outputs in PR notes

## PR2 (next) — Output contract + exit codes + one test update (FAST PATH)
Objective:
- Align outputs to eligible.csv/scored.csv/report.md contract.
- Harden PowerShell exit-code propagation (no silent pass).
- Update smoke test to assert new outputs + columns + score range.

Acceptance:
- scripts/run.ps1 produces the 3 outputs under outputs/<run_id>/
- missing OHLCV in offline mode => nonzero exit
- pytest (smoke subset) passes

Stop conditions:
- Do NOT add GDELT, embeddings, or ML training in PR2.

## PR3 — Offline datastore + manifests (Parquet-ready)
Deliver:
- data/ folder conventions + manifest.json writer/validator
- “data doctor” CLI: validates schema, coverage, duplicates, missingness
Acceptance:
- pytest passes
- doctor runs on fixtures and fails on intentionally corrupted cases

## PR4 — GDELT provisioning (BigQuery export + offline cache)
Deliver:
- BigQuery export tool(s) that create day-partitioned local Parquet/CSV
- manifests for gdelt datasets
Acceptance:
- export for a small date window succeeds
- offline pipeline can read exported files without internet

## PR5 — Feature engineering: GDELT → daily exogenous features
Deliver:
- daily aggregates (counts, theme/entity frequencies, tone proxies if available)
- lagged windows and z-scores
- strict no-leakage join to market dates
Acceptance:
- unit tests confirm no future data is used (date boundary tests)

## PR6 — Offline text corpus indexing + vector retrieval (CPU first, GPU optional)
Deliver:
- embeddings build script, persisted index + metadata
- query CLI that returns top-k analog docs for a date window
Acceptance:
- CPU path works on fixtures
- GPU path is optional and auto-detected

## PR7 — Predictive modeling v1 (GPU optional, CPU fallback required)
Deliver:
- task definitions (vol spike, drawdown breach, regime classification)
- baseline model + walk-forward evaluation
- model artifacts saved under data/models/
Acceptance:
- training/eval runs on fixtures (small)
- inference produces additional columns into scored.csv without breaking contract

## PR8 — Model registry + offline inference engine
Deliver:
- model_card.md per model version
- predict CLI and integration into run pipeline
Acceptance:
- reproducible inference outputs on fixture datasets

## PR9 — Performance + GPU acceleration wiring
Deliver:
- GPU detection, optional acceleration for embedding/indexing and training
- benchmarks and caching improvements
Acceptance:
- CPU tests pass in CI
- GPU path documented and validated locally when available
