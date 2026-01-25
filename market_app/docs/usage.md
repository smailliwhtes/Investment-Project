# Usage (Blueprint Wrapper)

## One-Command Run

From the repository root:

```powershell
.\scripts\run.ps1 -Config .\config\config.yaml
```

This writes outputs to:

```
outputs/runs/<run_id>/
```

## CLI Usage

```bash
python -m market_app.cli --config config/config.yaml --offline --run_id demo_run --top_n 15 --conservative
```

Flags:

- `--config`: Path to blueprint config YAML.
- `--run_id`: Output folder name under `outputs/runs/`.
- `--offline`: Force offline mode (no network).
- `--top_n`: Override report top‑N ranking.
- `--conservative` / `--opportunistic`: Select scoring variant.

## Outputs

Required artifacts per run:

- `universe.csv`
- `classified.csv`
- `features.csv`
- `eligible.csv`
- `scored.csv`
- `regime.json`
- `report.md`
- `manifest.json`

All outputs are monitoring-only and contain **no trading recommendations**.
