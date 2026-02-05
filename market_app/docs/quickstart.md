# Market Monitor Quickstart (Offline, Deterministic)

## Install

```bash
cd market_app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

## Data layout (defaults)

```
market_app/
  data/
    ohlcv_raw/              # optional raw OHLCV per-symbol CSVs
    ohlcv_daily/            # normalized OHLCV per-symbol CSVs
    exogenous/daily_features/  # partitioned daily cache (GDELT doctor)
  outputs/
    <run_id>/
```

You can override these via `config.yaml` or CLI flags; CLI takes precedence.

## One command run

```bash
cd market_app
market-monitor run --watchlist watchlists/watchlist_smoke.csv --asof 2025-01-31 --run-id smoke
```

Outputs are written to `outputs/smoke/` and include `results.csv`, `results.jsonl`, `run_manifest.json`, plus the required `eligible.csv`, `scored.csv`, and `report.md` outputs.

## Validate inputs (offline)

```bash
market-monitor validate-data --watchlist watchlists/watchlist_smoke.csv --asof 2025-01-31
```

## Provision data (offline imports)

```bash
market-monitor provision init-dirs --root data
market-monitor provision import-ohlcv --src tests/fixtures/provision/ohlcv_raw --dest data/ohlcv_raw --normalize
market-monitor provision import-exogenous --src tests/fixtures/provision/exogenous_raw --dest data/exogenous/raw
```

## What the outputs mean

- `results.csv` / `results.jsonl`: monitoring results with gates, scores, regime context, risk flags, and explanations.
- `run_manifest.json`: run metadata, hashes, resolved paths, and determinism fingerprint.
- `eligible.csv` / `scored.csv` / `report.md`: legacy contract outputs required for every run.
