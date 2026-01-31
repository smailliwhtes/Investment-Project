# 00_context — Offline-First Market + World Events Modeling System

## What this is
A monitoring-only application that runs fully offline after provisioning. It consumes:
1) Per-symbol daily OHLCV CSVs from disk (one file per ticker).
2) Optional offline world-events datasets (GDELT exports) stored locally.
3) Local feature caches and trained model artifacts.

Outputs are eligibility gates, risk flags, priority scores, and (later) model-based probabilities and analog explanations.
No financial advice. No buy/sell language.

## Non-goals (for now)
- No real-time trading integration.
- No mandatory online dependencies at runtime.
- No “single mega PR” builds. We ship as a PR ladder (see 30_pr_ladder.md).

## Offline-first rule
- Runtime assumes no internet.
- Provisioning scripts MAY download/refresh data, but must degrade gracefully and never block offline execution.
- All required artifacts must be discoverable from config + manifests.

## Project invariants
- One command to run: scripts/run.ps1
- One command to test: pytest (or documented subset)
- Stable output contract: eligible.csv, scored.csv, report.md

## Local paths (example environment)
- Application root: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app
- US Markets historical data: C:\Users\micha\OneDrive\Desktop\Market_Files
- Current corpus files: C:\Users\micha\OneDrive\Desktop\NLP Corpus


Codex is explicitly intended to work from clear repo docs and run your tests/commands with verifiable terminal evidence.
