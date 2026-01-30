# Offline Metadata Provisioning (Nasdaq + SEC)

This project keeps runtime fully offline. Metadata snapshots are downloaded only during a
manual provisioning step, then cached locally for offline enrichment.

## What gets cached

Provisioning downloads:

- Nasdaq SymbolDirectory:
  - `nasdaqlisted.txt`
  - `otherlisted.txt`
- SEC static mappings:
  - `company_tickers.json`
  - `company_tickers_exchange.json`
- (Optional) SEC bulk submissions:
  - `submissions.zip` (expanded into `submissions/CIK*.json`)
- SEC SIC codes HTML (converted into `sic_codes.csv`)

## PowerShell provisioning (Windows)

```
.\scripts\provision_metadata.ps1 `
  -PackRoot "C:\path\to\repo" `
  -OutDir "out\metadata_cache" `
  -IncomingRoot "incoming_metadata" `
  -IncludeSubmissionsZip `
  -UserAgent "OfflineMarketMonitor/1.0 (contact: you@example.com)"
```

Notes:

- The script resolves `PackRoot` even when `$PSScriptRoot` is empty (interactive shells).
- Outputs are written into `incoming_metadata\<YYYY-MM-DD>\` and normalized into
  `out\metadata_cache\`.
- `User-Agent` is required for SEC downloads. The script pauses briefly between SEC requests.

## Build SIC cache from submissions.zip

If you downloaded `submissions.zip`, build a compact offline cache:

```
python tools/build_sic_cache.py --submissions-zip incoming_metadata/2025-01-01/sec/submissions.zip
```

This writes `out/sec/sic_by_cik.csv`, which the enrichment pipeline uses without any network.

## Enrich the security master

```
python tools/enrich_security_master.py `
  --input out/security_master.csv `
  --output out/security_master.csv `
  --metadata-cache out/metadata_cache
```

The enrichment pipeline merges metadata into existing `security_master.csv` using:

- Nasdaq SymbolDirectory for names/exchanges/ETF flags.
- SEC ticker/CIK mappings for CIKs.
- SEC submissions cache for SIC codes.
- Sector bucket rules and manual overrides (`config/sector_overrides.csv`).
