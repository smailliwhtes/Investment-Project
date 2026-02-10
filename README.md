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

## Audit command

```bash
cd market_app
python -m market_app.audit
```

## Tests

```bash
cd market_app
pytest -q
```
