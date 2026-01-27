# Architecture (Blueprint-Compatible Wrapper)

## Overview

Market Monitor remains the core **engine** (`market_monitor/`). A lightweight
**compatibility layer** (`src/market_app/`) adapts the engine to the offline-first
blueprint without destructive refactors. The wrapper is responsible for:

- Loading blueprint-shaped configuration from `config/config.yaml`.
- Mapping blueprint config to the engine schema.
- Enforcing offline-only execution.
- Writing blueprint output artifacts to `outputs/runs/<run_id>/`.

## Module Interaction Diagram (Text)

```
[PowerShell runner]
  scripts/run.ps1
        |
        v
[Blueprint CLI]
  src/market_app/cli.py
        |
        +--> config loader (src/market_app/config.py)
        |
        +--> logging config (config/logging.yaml)
        |
        v
[Engine pipeline]
  market_monitor/pipeline.py
        |
        +--> provider_factory -> providers (offline-only)
        +--> staging -> features -> scoring -> gates
        +--> corpus (optional, offline)
        |
        v
[Wrapper outputs]
  src/market_app/outputs.py
        |
        +--> universe.csv
        +--> classified.csv
        +--> features.csv
        +--> eligible.csv
        +--> scored.csv
        +--> regime.json
        +--> report.md
        +--> manifest.json
```

## Determinism & Offline Enforcement

- Offline mode is enforced in both wrapper and engine; any network attempts in
  offline mode raise exceptions.
- Outputs are sorted by symbol and normalized deterministically to ensure
  repeatable artifacts given the same inputs and `run_id`.
- Default run IDs are deterministic when not explicitly provided, enabling
  repeatable test runs across environments.
