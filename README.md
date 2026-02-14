# Investment-Project

Primary application: `market_app/`.

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

**Do NOT paste Python code into PowerShell.**
Always run tests through `pytest` so Python code is executed by the Python interpreter.

```powershell
.\scripts\run_e2e.ps1
```

```bash
./scripts/run_e2e.sh
```

```bash
python -m pytest -q tests/test_offline_e2e_market_and_corpus.py -k offline_e2e --maxfail=1
```

## Tests

```bash
cd market_app
pytest -q
```
