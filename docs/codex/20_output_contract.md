# 20_output_contract

Successful runs must preserve the engine contracts defined in `AGENTS.md`.

## Required run artifacts

Every successful `python -m market_monitor.cli run --config <path> --out-dir <dir> --offline --progress-jsonl` invocation must produce:

- `scored.csv`
- `eligible.csv`
- `report.md`
- `run_manifest.json`
- `config_snapshot.yaml`
- `ui_engine.log`
- `logs/engine.log`

## `eligible.csv`

Minimum columns:

- `symbol`
- `eligible`
- `reasons` (recommended; semicolon-delimited when populated)

## `scored.csv`

Minimum columns:

- `symbol`
- `score`
- `rank` or a separately documented deterministic sort rule
- `flags_count` (recommended)
- `theme_labels` (recommended)
- `gates_passed`
- `last_date`
- `lag_days`

Recommended additive columns already supported by the engine:

- `staleness_days_at_run`
- `ml_signal`
- `ml_model_id`
- `ml_featureset_id`

`last_date` and `lag_days` are hard contract fields. They must be present in the final `scored.csv`, whether produced directly by the engine or merged deterministically from `data_quality.csv`.

## `report.md`

Must include:

- run metadata
- counts summary
- top-ranked symbols
- compact symbol-level explanation lines

## `run_manifest.json`

Must include:

- `run_id`
- `started_at`
- `finished_at`
- `duration_s`
- `app`
- `environment`
- `config`
- `counts`
- `artifacts`
- `data_freshness`

`data_freshness` must include:

- `last_date_max`
- `worst_lag_days`
- `median_lag_days`
- `staleness_days_at_run`

## Optional additive artifacts

These may be present but must not replace the required run outputs:

- `data_quality.csv`
- `flags.csv`
- `progress.jsonl`
- `explain/`
- `ml/`
- `ml/benchmark/`
- `cause_effect_manifest.json`
- `cause_effect_summary.json`
- `linked_market_gdelt/`
- `event_impact_library.csv`
- `analog_outcomes.csv`

## Policy simulation artifacts

`python -m market_monitor.cli policy simulate --config <path> --scenario <name> --outdir <dir> --offline --progress-jsonl` is additive to the core run contract. It should produce:

- `policy_event_study.csv`
- `policy_analogs.csv`
- `policy_scenario_rankings.csv`
- `policy_report.md`
- `policy_manifest.json`
- `policy_summary.json`

These policy artifacts are scenario-analysis outputs and do not replace the core run outputs.
The wrapper entrypoint `python -m market_app.cli policy simulate ...` should delegate to the same engine contract.

## ML benchmark artifacts

`python -m market_monitor.cli ml benchmark --joined-path <path> --output-dir <run_dir> ...` is additive to the core run contract. It should produce:

- `ml/benchmark/benchmark_metrics.csv`
- `ml/benchmark/benchmark_summary.json`
- `ml/benchmark/benchmark_report.md`
- `ml/benchmark/<model_type>/` per-model train/predict artifact bundles

`benchmark_metrics.csv` minimum columns:

- `model_type`
- `fold`
- `rmse`
- `mae`
- `r2`
- `train_start`
- `train_end`
- `val_start`
- `val_end`
- `model_id`
- `featureset_id`

`benchmark_summary.json` minimum fields:

- `schema_version`
- `models`
- `winner`
- `primary_metric`
- `promotion_recommended`
- `promotion_reason`
- `thresholds`
- `seed`
- `dataset_hash`
- `featureset_id`

These benchmark artifacts must not overwrite the canonical promoted-model outputs under `<run_dir>/ml/`.
The wrapper entrypoint `python -m market_app.cli ml benchmark ...` should delegate to the same engine contract.

## Linked cause/effect artifacts

`python -m market_monitor.cli corpus build-linked --config <path> ...` may produce:

- `cause_effect_manifest.json`
- `cause_effect_summary.json`
- `market_daily.csv`
- `gdelt_daily_features.csv`
- `linked_market_gdelt/manifest.json` plus partitions
- `event_impact_library.csv`
- `analog_outcomes.csv`

These files must remain deterministic for the same offline inputs and config.

## Storage audit/migration artifacts

`python -m market_monitor.cli storage audit-parquet --market-root <path> --corpus-root <path> --working-root <path> --out-dir <dir>` should produce:

- `inventory.json`
- `inventory.csv`
- `migration_plan.json`
- `migration_report.md`

`python -m market_monitor.cli storage migrate-parquet --market-root <path> --corpus-root <path> --working-root <path> --out-dir <dir> [--archive-root <dir>] --dry-run|--apply` should produce:

- `conversion_manifest.json`
- `conversion_report.md`
- `rollback_manifest.json`
- `parity_checks.json`
- `conversion_checkpoint.jsonl` (append-only resume/checkpoint log for apply mode)

These storage artifacts are additive and must not change the core run/evaluate/GUI contracts.
