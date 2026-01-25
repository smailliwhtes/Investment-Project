# Blueprint Alignment Review (Market Monitor vs. Offline-First Market App)

## Scope & Review Notes

This review compares the current **Market Monitor** repository against the provided
"offline-first market_app" blueprint and acceptance criteria. The analysis focuses on
core pipeline behavior, CLI/config structure, offline enforcement, outputs, and the
repository scaffold. The current codebase is already a monitoring-only system with
offline support and an offline corpus pipeline, so the comparison emphasizes **naming
conventions, output contract, CLI shape, and determinism guarantees** as required by the
blueprint.

Primary references:

- Repository overview and offline workflow expectations are described in `README.md` and
  `docs/application_overview.md`.【F:README.md†L1-L152】【F:docs/application_overview.md†L1-L48】
- The CLI entry point and offline-mode enforcement are implemented in `market_monitor/cli.py`
  and `market_monitor/offline.py`.【F:market_monitor/cli.py†L200-L270】【F:market_monitor/offline.py†L1-L20】
- Config defaults and environment overrides are defined in `market_monitor/config_schema.py`.
  【F:market_monitor/config_schema.py†L1-L200】
- Report output structure is defined in `market_monitor/report.py`.【F:market_monitor/report.py†L1-L98】
- Run manifest hashing and metadata are implemented in `market_monitor/manifest.py`.
  【F:market_monitor/manifest.py†L1-L200】
- The current PowerShell runner is `run.ps1`.【F:run.ps1†L1-L25】

## High-Level Alignment Summary

**Strong alignment already present**:

- Monitoring-only intent is explicitly stated in README and report text; no trading language is
  used in core outputs.【F:README.md†L1-L9】【F:market_monitor/report.py†L89-L98】
- Offline mode is enforced via `set_offline_mode`/`require_online` and is required for this
  release (CLI rejects online runs).【F:market_monitor/offline.py†L1-L20】【F:market_monitor/cli.py†L230-L255】
- The pipeline produces structured CSV outputs, a markdown report, and a run manifest with hashes
  and versions, supporting reproducibility tracking.【F:README.md†L70-L101】【F:market_monitor/manifest.py†L91-L200】
- Tests and fixtures exist for offline execution, including a smoke test harness.
  【F:README.md†L118-L152】【F:tests/test_smoke_run.py†L1-L200】

**Primary divergences from the blueprint** (summarized):

1. **Repository layout mismatch** — the current package is `market_monitor/` at repo root,
   not `src/market_app/`, and config/docs paths differ from the requested structure.
   【F:README.md†L17-L48】【F:market_monitor/__init__.py†L1-L24】
2. **CLI contract mismatch** — the blueprint requires `python -m market_app.cli` with
   `--config`, `--run_id`, `--offline`, `--top_n`, and `--conservative/--opportunistic`.
   Current CLI uses `python -m market_monitor` subcommands and different flags.
   【F:README.md†L29-L52】【F:market_monitor/cli.py†L900-L1005】
3. **Output contract mismatch** — blueprint requires `outputs/runs/<timestamp>/` with named
   files (`universe.csv`, `classified.csv`, `features.csv`, `eligible.csv`, `scored.csv`,
   `regime.json`, `report.md`, `manifest.json`). Current outputs are `outputs/features_<run_id>.csv`,
   `outputs/scored_<run_id>.csv`, etc., plus additional artifacts.
   【F:README.md†L70-L101】【F:market_monitor/cli.py†L340-L450】
4. **Config schema mismatch** — blueprint expects `config/config.yaml` with specific keys (e.g.,
   `offline`, `run.top_n`, `scoring.weights_conservative`, `regime_overlay`, `themes.theme_weights`).
   Current config uses `config.yaml` and a different schema (`data.offline_mode`, `score.weights`).
   【F:config.yaml†L1-L84】【F:market_monitor/config_schema.py†L1-L200】
5. **Provisioning and provenance** — blueprint requires a strict `provision_data.ps1` and
   per-file provenance metadata in `data/raw/metadata/`. Current repo contains bulk downloader
   design and stubs, but not the exact provisioning script or provenance schema.
   【F:docs/bulk_downloader.md†L1-L120】【F:market_monitor/bulk/manifest.py†L1-L200】
6. **Regime engine & theme classifier shape** — current system has macro/corpus context and
   a theme tagger, but the required `regime.json` and `classified.csv` outputs plus regime overlay
   logic and watchlists.yaml-driven taxonomy are not in the specified format.
   【F:market_monitor/macro.py†L1-L200】【F:market_monitor/themes.py†L1-L200】

## Detailed Gap Matrix (Blueprint → Current Implementation)

### A) Repo scaffold + docs

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| `src/market_app/` package + `docs/architecture.md`, `docs/data_provenance.md`, `docs/usage.md` | Package is `market_monitor/` at repo root. Docs exist but not the requested files/paths. | **Structure + doc file naming mismatch.**【F:README.md†L1-L68】【F:docs/application_overview.md†L1-L48】 |
| README “How to run” section | README contains detailed run/test instructions via scripts and CLI. | **Mostly aligned**, but references different scripts and outputs. 【F:README.md†L11-L152】 |

### B) CLI + configuration

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| `python -m market_app.cli` entry point with flags `--config`, `--run_id`, `--offline`, `--top_n`, `--conservative/--opportunistic` | CLI is `python -m market_monitor` with subcommands (`run`, `validate`, `preflight`, etc.) and different flags. | **Command/flag mismatch**. 【F:README.md†L29-L52】【F:market_monitor/cli.py†L900-L1005】 |
| YAML + pydantic validation | YAML config with custom validation; no pydantic. | **Validation framework mismatch** (schema + tooling).【F:market_monitor/config_schema.py†L1-L200】 |
| Logging via `config/logging.yaml` and `run.log` under run folder | Uses JSONL + console logging configured in code; logs under `outputs/logs`. | **Logging contract mismatch**. 【F:market_monitor/cli.py†L330-L410】 |

### C) Ingestion (offline, file-based)

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| Symbols from `data/raw/symbols/` and OHLCV from `data/raw/ohlcv/` | Offline provider reads from NASDAQ daily CSVs and watchlists; symbol discovery path differs. | **Directory + ingestion format mismatch**. 【F:market_monitor/providers/nasdaq_daily.py†L1-L200】【F:market_monitor/universe.py†L1-L200】 |
| Provenance metadata JSON in `data/raw/metadata/` | Manifests exist for bulk downloads; no per-file provenance schema matching blueprint. | **Provenance format gap**. 【F:market_monitor/bulk/manifest.py†L1-L200】 |

### D) Feature engineering

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| Specific features (returns 1/3/6/12m, SMA ratios, vol/downsides, ADV, drawdown, etc.) | Feature engine computes trend/momentum, vol, drawdown, liquidity, attention, etc., but not all required columns or naming. | **Feature set + naming mismatch**. 【F:market_monitor/features.py†L1-L200】【F:market_monitor/scoring.py†L1-L200】 |
| Explicit normalized columns (z-score, winsorized) | Normalization logic exists but not specified exactly as blueprint. | **Normalization contract mismatch**. 【F:market_monitor/features.py†L1-L200】 |

### E) Regime engine

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| Macro CSVs from `data/raw/macro/` + `regime.json` output | Macro logic exists (e.g., silver series) and corpus context, but no explicit `regime.json` artifact. | **Output missing**. 【F:market_monitor/macro.py†L1-L200】【F:README.md†L70-L101】 |

### F) Theme classification

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| `watchlists.yaml` with themes; output `classified.csv` with evidence JSON | Theme tagging is keyword-based in config, output is embedded in scoring output. | **Inputs/outputs mismatch**. 【F:market_monitor/themes.py†L1-L200】【F:config.yaml†L41-L60】 |

### G) Gates, flags, scoring

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| `eligible.csv`, explicit gates, flags, conservative vs opportunistic weights, theme + regime overlay | Gates and flags exist; scores emitted; no explicit conservative/opportunistic variants or regime overlay format. | **Scoring variant + output mismatch**. 【F:market_monitor/gates.py†L1-L200】【F:market_monitor/scoring.py†L1-L200】 |

### H) Reporting + explainability

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| report.md with macro/regime summary, flags distribution, top candidates, explanation packs | Report has top list, optional prediction diagnostics, context section; explanation packs not emitted. | **Report schema mismatch**. 【F:market_monitor/report.py†L1-L200】 |

### I) Storage + manifest

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| manifest.json includes config hash, git SHA, dataset/model checksums | Manifest includes config hash, git commit, file hashes, corpus files, versions. | **Mostly aligned**, but filename/location and required fields differ. 【F:market_monitor/manifest.py†L91-L200】 |

### J) PowerShell scripts

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| `scripts/run.ps1` takes `-Config .\config\config.yaml` and writes outputs/runs/<timestamp> | Existing `run.ps1` uses `config.yaml` at repo root; acceptance script uses `scripts/acceptance.ps1`. | **Entry point mismatch**. 【F:run.ps1†L1-L25】【F:scripts/acceptance.ps1†L1-L200】 |

### Snowball “forward_outcome_summary”

| Blueprint requirement | Current state | Gap |
| --- | --- | --- |
| Historical forward return distributions for top-N bucket (monitoring-only) | Evaluation pipeline and prediction diagnostics exist, but not the explicit “forward_outcome_summary” output in scored.csv/report.md. | **Missing output**. 【F:market_monitor/evaluate.py†L1-L200】【F:market_monitor/report.py†L1-L200】 |

## Pragmatic Decision Summary

**Most reasonable path** to align with the blueprint is a **structured refactor** that preserves
existing offline-first logic while re-anchoring to the required naming, folders, and output
contract. The core capability is already present; the largest lift is **renaming/restructuring**
for compliance (CLI, outputs, config files, and doc layout).

Suggested order of work (Phase 1 only, per blueprint):

1. **Scaffold alignment:** Introduce `src/market_app/` with a small adapter layer that wraps the
   existing pipeline; add `config/` and `docs/` files expected by the blueprint.
2. **CLI contract:** Implement `python -m market_app.cli` as a wrapper around existing logic;
   map flags (`--top_n`, `--conservative`, `--opportunistic`, `--offline`) to current config.
3. **Output contract:** Write new `outputs/runs/<timestamp>/` artifacts in the required names
   (and optionally keep legacy outputs for backward compatibility).
4. **Regime + classification artifacts:** Adapt existing macro/theme outputs into `regime.json`
   and `classified.csv` so downstream requirements are satisfied.
5. **Forward outcome summary:** Add an offline-only historical outcome band summary to
   `scored.csv` and `report.md` using existing evaluation scaffolding.

This staged approach minimizes risk while keeping the system monitoring-only and deterministic,
and it allows verification against the blueprint’s acceptance criteria without discarding the
current, already-working offline pipeline.
