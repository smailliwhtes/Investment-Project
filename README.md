# Investment-Project

Primary application: `market_app/`.

## First Run on Windows

1. **Bootstrap local dev environment** (discovers paths, creates directories, generates watchlist):

```powershell
.\scripts\bootstrap_local_dev.ps1
```

2. **Launch the GUI** (requires .NET SDK 8 + MAUI workload):

```powershell
.\scripts\run_gui.ps1
```

3. **Run the engine pipeline**:

```powershell
cd market_app
python -m market_monitor.cli run --config .\config\config.yaml --out-dir ..\outputs\runs\manual_run --offline --progress-jsonl
```

### Environment Variables

Set these to override paths from `config.yaml` (or use `.env.local` for the bootstrap script):

| Variable | Description |
|---|---|
| `MARKET_APP_OHLCV_DAILY_DIR` | Path to OHLCV daily CSV directory |
| `MARKET_APP_CORPUS_DIR` | Path to NLP corpus directory |
| `MARKET_APP_EXOGENOUS_DAILY_DIR` | Path to exogenous daily features directory |

See `.env.local.example` for a template (copy to `.env.local` — it is gitignored).

### Watchlist

The engine requires a watchlist at `market_app/config/watchlists/watchlist_core.csv`.
Format: CSV with columns `symbol,theme_bucket,asset_type` (one symbol per row).
The bootstrap script generates one automatically from OHLCV files.

### Exogenous Data

Exogenous data is **optional by default** (`exogenous.enabled: false` in config).
Set `exogenous.enabled: true` in your config to require exogenous daily features.

### Preflight

```powershell
cd market_app
python -m market_monitor.cli preflight --config .\config\config.yaml
```

The `--offline` flag is accepted for compatibility but has no effect (preflight is always offline).

## Provisioning (may use network)

```bash
cd market_app
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Offline runtime (no network)

```powershell
cd market_app
./scripts/run.ps1 -Config ./config/config.yaml -Offline
```

Deterministic direct CLI run:

```bash
cd market_app
python -m market_monitor.cli run --config ./config/config.yaml --out-dir ../outputs/runs/manual_2025_01_31 --offline --progress-jsonl
```

Policy simulator:

```powershell
.\scripts\provision_policy_data.ps1
cd market_app
python -m market_monitor.cli policy simulate --config .\config\config.yaml --scenario tariff-shock --outdir ..\outputs\policy\tariff-shock --offline --progress-jsonl
```

The wrapper entrypoint delegates engine-owned commands, so `python -m market_app.cli policy simulate ...` is also supported when you want one CLI surface for both wrapper and engine tasks.

## Parquet-first desktop storage

The engine now supports a Parquet-first cutover for the three Desktop data roots:

- `C:\Users\micha\OneDrive\Desktop\Market_Files`
- `C:\Users\micha\OneDrive\Desktop\NLP Corpus`
- `C:\Users\micha\OneDrive\Desktop\Working CSV Files`

Audit the current layout before changing anything:

```powershell
cd market_app
python -m market_monitor.cli storage audit-parquet --market-root "C:\Users\micha\OneDrive\Desktop\Market_Files" --corpus-root "C:\Users\micha\OneDrive\Desktop\NLP Corpus" --working-root "C:\Users\micha\OneDrive\Desktop\Working CSV Files" --out-dir ..\outputs\storage_audit
```

Plan the migration without moving files:

```powershell
cd market_app
python -m market_monitor.cli storage migrate-parquet --market-root "C:\Users\micha\OneDrive\Desktop\Market_Files" --corpus-root "C:\Users\micha\OneDrive\Desktop\NLP Corpus" --working-root "C:\Users\micha\OneDrive\Desktop\Working CSV Files" --out-dir ..\outputs\storage_migrate --dry-run
```

Apply the migration once the dry-run and parity outputs look correct:

```powershell
cd market_app
python -m market_monitor.cli storage migrate-parquet --market-root "C:\Users\micha\OneDrive\Desktop\Market_Files" --corpus-root "C:\Users\micha\OneDrive\Desktop\NLP Corpus" --working-root "C:\Users\micha\OneDrive\Desktop\Working CSV Files" --out-dir ..\outputs\storage_migrate --apply
```

During the transition release, OHLCV readers prefer `.parquet` and fall back to `.csv`. The canonical normalized OHLCV store is `Working CSV Files`, while duplicate normalized market folders are converted only for parity checks before archive. Raw market sources are mirrored into `raw_market_parquet/symbol=<SYMBOL>/<source_key>.parquet`, and apply mode writes an append-only `conversion_checkpoint.jsonl` so a long migration can be resumed safely with the same `--out-dir`.

## Audit command

```bash
cd market_app
python -m market_app.audit
```

## How to run tests (offline-safe)

**Do NOT paste Python test source into PowerShell.**
Always execute tests through `python -m pytest` or helper scripts.

Full suite:

```bash
python -m pytest -q
```

Offline E2E (repo-root test path):

```powershell
python -m pytest -q tests\test_offline_e2e_market_and_corpus.py -k offline_e2e --maxfail=1
```

Helpers:

```powershell
.\scripts\run_e2e.ps1
```

```bash
./scripts/run_e2e.sh
```

Desktop shortcut installer (Windows):

```powershell
.\scripts\install_e2e_desktop_shortcut.ps1
```

GUI desktop shortcut (Windows):

```powershell
.\scripts\install_gui_desktop_shortcut.ps1
```
This creates/overwrites **MarketApp GUI.lnk** on your Desktop. The shortcut launches `scripts/run_gui.ps1` (prefers `pwsh`, falls back to `powershell`) with the working directory set to the repo root so build/test output remains visible in the console.

If you don't see the icon, run:

```powershell
$desktop = [Environment]::GetFolderPath("Desktop")
Test-Path (Join-Path $desktop "MarketApp GUI.lnk")
```

Note: on OneDrive-redirected Windows profiles, `$HOME\Desktop` can point to a different folder than your actual Desktop. Use `[Environment]::GetFolderPath("Desktop")` for reliable verification.

If this returns `False`, rerun the installer and confirm the printed `Created Desktop shortcut:` path.

Optional smoke helpers:

```powershell
.\scripts\run_smoke.ps1
```

```bash
./scripts/run_smoke.sh
```

## Tests

```bash
cd market_app
pytest -q
```

## Neural Networks / Deep Learning

The repo now ships an additive offline neural backend under `market_app/market_monitor/ml/` alongside the existing classical models. Use `python -m market_monitor.ml.train_xgb --model-type numpy_mlp ...` to train the deterministic NumPy MLP while preserving the same `ml/` artifacts, `predictions_latest.csv`, and scored-output fields (`ml_signal`, `ml_model_id`, `ml_featureset_id`).

To compare the deterministic NumPy MLP against the current classical baseline without changing promoted production artifacts, run:

```powershell
cd market_app
python -m market_monitor.cli ml benchmark --joined-path .\data\features\joined --output-dir ..\outputs\runs\<run_id> --model-types sklearn_gb,numpy_mlp --horizon-days 5 --folds 3 --gap 0 --seed 42
```

This writes additive benchmark outputs under `<run_dir>\ml\benchmark\`, updates the existing `run_manifest.json`, and leaves the canonical promoted-model outputs under `<run_dir>\ml\` untouched. The wrapper CLI also delegates this command, so `python -m market_app.cli ml benchmark ...` is supported. The broader deep-learning integration path is documented in [docs/codex/30_deep_learning_strategy.md](docs/codex/30_deep_learning_strategy.md).
