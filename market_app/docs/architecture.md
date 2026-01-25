# Architecture (Blueprint-Compatible Wrapper)

## Overview

Market Monitor remains the core **engine** (`market_monitor/`). A lightweight
**compatibility layer** (`src/market_app/`) adapts the engine to the offline‑first
blueprint without destructive refactors. The wrapper is responsible for:

- Loading blueprint-shaped configuration from `config/config.yaml`.
- Mapping blueprint config to the engine schema.
- Enforcing offline-only execution.
- Writing blueprint output artifacts to `outputs/runs/<run_id>/`.

## Module Interactions

1. **Wrapper CLI (`market_app.cli`)**
   - Parses blueprint flags and loads config.
   - Configures logging from `config/logging.yaml`.
   - Calls the engine pipeline entry point.

2. **Engine Pipeline (`market_monitor.pipeline.run_pipeline`)**
   - Executes staged ingestion, features, scoring, and optional corpus pipeline.
   - Returns scored features + metadata for wrapper output mapping.

3. **Wrapper Outputs (`market_app.outputs`)**
   - Builds `universe.csv`, `classified.csv`, `features.csv`, `eligible.csv`,
     `scored.csv`, `regime.json`, `report.md`, and `manifest.json`.

## Determinism & Offline Enforcement

- Offline mode is enforced in both wrapper and engine; any network attempts in
  offline mode raise exceptions.
- Outputs are sorted by symbol and normalized deterministically to ensure
  repeatable artifacts given the same inputs and `run_id`.
