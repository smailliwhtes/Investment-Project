# Offline Quickstart (Windows)

This walkthrough sets up a fully offline runtime using local Stooq TXT data and cached metadata.

## 1) Build inventory + security master

```
python -m market_monitor.tools.inventory `
  --stooq-root "C:\path\to\d_us_txt\data\daily\us" `
  --write-out out

python tools/build_security_master.py `
  --stooq-root "C:\path\to\d_us_txt\data\daily\us" `
  --output out\security_master.csv `
  --path-mode relative
```

## 2) Provision metadata (manual, online step)

```
.\scripts\provision_metadata.ps1 `
  -PackRoot "C:\path\to\repo" `
  -OutDir "out\metadata_cache" `
  -IncludeSubmissionsZip `
  -UserAgent "OfflineMarketMonitor/1.0 (contact: you@example.com)"
```

## 3) Enrich the security master offline

```
python tools/enrich_security_master.py `
  --input out\security_master.csv `
  --output out\security_master.csv `
  --metadata-cache out\metadata_cache
```

## 4) One-command workflow (end-to-end)

```
.\scripts\offline_one_command.ps1 `
  -PackRoot "C:\path\to\repo" `
  -StooqRoot "C:\path\to\d_us_txt\data\daily\us" `
  -Provision `
  -IncludeSubmissionsZip
```

The one-command script:

- Creates a virtual environment.
- Installs dependencies.
- Runs inventory.
- Builds `security_master.csv`.
- Optionally provisions metadata.
- Enriches the security master using local caches.
- Runs unit tests.
- Runs one offline monitor pass.
