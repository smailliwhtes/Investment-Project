# 00_context — Offline-First Market + World Events Modeling System

## What this is
An offline-first monitoring app that consumes:
1) Per-symbol daily OHLCV CSVs from disk (one file per ticker).
2) Optional offline world-events corpora (GDELT exports or precomputed daily features).
3) Local feature caches and trained model artifacts.

Outputs are eligibility gates, risk flags, priority scores, and (later) model-based probabilities and analog explanations.
No financial advice. No buy/sell language.

## Non-goals (for now)
- No real-time trading integration.
- No mandatory online dependencies at runtime.
- No “single mega PR” builds. We ship as a PR ladder (see 30_pr_ladder.md).

## Offline-first + determinism rules
- Runtime assumes no internet.
- Provisioning scripts MAY download/refresh data, but must degrade gracefully and never block offline execution.
- All required artifacts must be discoverable from config + manifests.
- Deterministic outputs: seeded randomness, content hashes for datasets/artifacts, stable output naming.

## What success looks like
- `scripts/run.ps1` (or `scripts/run.sh`) produces `eligible.csv`, `scored.csv`, `report.md` under `outputs/<run_id>/`.
- GDELT corpora (raw events or precomputed daily features) can be audited, normalized, and joined offline.
- Tests and smoke runs pass using the commands in `docs/codex/40_commands.md`.

Codex is explicitly intended to work from clear repo docs and run your tests/commands with verifiable terminal evidence.
