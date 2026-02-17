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
python -m market_app.cli --config ./config/config.yaml --offline --as-of 2025-01-31
```

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
Test-Path "$HOME\Desktop\MarketApp GUI.lnk"
```

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
