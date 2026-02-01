# Bulk + Cache Data Locations (Market Monitor)

> **Monitoring-only guardrail:** these data assets are used for eligibility, risk flags, and monitoring priority. They are **not** trading signals or buy/sell guidance.

## Quick Map (Defaults)

These defaults are driven by `config.yaml`.

| Purpose | Config Key | Default Path | Notes |
| --- | --- | --- | --- |
| Raw bulk downloads | `bulk.paths.raw_dir` | `data/raw/` | Per-source folders (e.g., `data/raw/stooq/`). |
| Curated bulk outputs | `bulk.paths.curated_dir` | `data/curated/` | Standardized outputs (one file per input file). |
| Bulk manifests | `bulk.paths.manifest_dir` | `data/manifests/` | JSON plan + provenance for bulk runs. |
| Provider cache | `paths.cache_dir` | `data/cache/` | Per-symbol cached history CSVs. |
| Universe CSV | `paths.universe_csv` | `data/universe/universe.csv` | Nasdaq universe snapshot. |
| Watchlist | `paths.watchlist_file` | `inputs/watchlist.txt` | One symbol per line. |
| Batch cursor | `paths.state_file` | `data/state/batch_state.json` | Cursor for batch mode. |
| Run outputs | `data_roots.outputs_dir` | `outputs/` | Reports + scored/eligible CSVs. |
| OHLCV data | `data_roots.ohlcv_dir` | `data/ohlcv/` | Per-symbol OHLCV CSVs (offline required). |
| GDELT data | `data_roots.gdelt_dir` | `corpus/` | Conflict CSV + raw event zips (optional). |

## Bulk Download Entry Points (Functional)

Bulk download features are implemented and wired to the CLI:

- **Plan:** `python -m market_monitor bulk-plan` — creates a manifest without downloading.
- **Download:** `python -m market_monitor bulk-download` — downloads raw CSVs (per symbol or per archive).
- **Standardize:** `python -m market_monitor bulk-standardize` — converts raw CSVs into curated outputs.

These are **monitoring-only** utilities and do not make recommendations.

## What Gets Downloaded

The current bulk sources in `config.yaml`:

- **Stooq** (`stooq`): per-symbol daily CSVs using a symbol template (example target: `https://stooq.pl/q/d/l/?s=AAPL.us&i=d`).
- **Treasury Yield Curve** (`treasury_yield_curve`): static CSV from a fixed URL.

The planner decides between:

- **Per-symbol downloads** (symbol template), or
- **Bulk archives** when `supports_bulk_archive` + `archive_path` is configured, or
- **Static files** when `static_path` is configured.

## How Often Data Refreshes

- **Provider cache** (`data/cache/`): refreshed when cached files are older than `data.max_cache_age_days` in `config.yaml`.
- **Bulk downloads**: run on demand. Each run overwrites the target file if it already exists (deterministic path). Manifests capture the plan for each run.
- **Curated outputs**: regenerated when you run `bulk-standardize`.

## How History Accumulates

- **Provider cache**: incremental history is merged on fetch (`merge_delta` keeps the most recent record per date).
- **Bulk downloads**: per-file downloads overwrite the same path. If you want an append-only history, preserve older files or copy the raw directory before the next run.
- **Curated outputs**: replace outputs for the same input file; archive prior curated outputs if you need a time series of curated snapshots.

## Manifest Files (Reproducibility)

Manifests are stored under `data/manifests/` and include:

- Timestamped `created_at_utc`
- List of tasks (`source_name`, `url`, `destination`, `symbol`, `is_archive`)

Use them to replay or audit the same download plan.

## Connectivity Checks

The doctor command performs **lightweight** reachability checks (HEAD/Range GET) for each bulk source:

```powershell
python -m market_monitor doctor --config config.yaml
```

To skip connectivity checks in offline mode:

```powershell
$env:MM_OFFLINE = "1"
python -m market_monitor doctor --config config.yaml
```

To treat connectivity warnings as errors (CI/acceptance testing):

```powershell
python -m market_monitor doctor --config config.yaml --strict
```
