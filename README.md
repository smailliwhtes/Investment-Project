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
