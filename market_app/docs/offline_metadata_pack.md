# Offline Metadata Pack (Security Master + SIC)

## Goals
This app is **offline-first** during normal runs. All metadata enrichment is done in an explicit
provisioning step that downloads and caches snapshots locally.

## Directory layout (offline snapshots)
```
incoming_metadata/
  nasdaq_trader/
    nasdaqlisted.txt
    otherlisted.txt
  sec/
    company_tickers.json
    company_tickers_exchange.json
    sic_codes.html
    submissions/
      CIK##########.json
out/
  security_master.csv
  sic_codes.csv
```

## Build security_master.csv (Stooq TXT pack)
```
python tools/build_security_master.py `
  --stooq-root "C:\path\to\d_us_txt\data\daily\us" `
  --output out/security_master.csv `
  --metadata-root incoming_metadata `
  --path-mode auto
```

Notes:
- `--path-mode auto` stores **repo-relative paths when possible**, otherwise paths are stored
  relative to the Stooq root. This keeps outputs portable while still functioning for external
  data directories.
- Add `--filter-required` to restrict the build to `config/universe_required.csv`.

## Provision SEC metadata + SIC lookup (explicit, online)
```
.\scripts\provision_sec_metadata.ps1 -Mode zip -UserAgent "OfflineMarketMonitor/1.0 (contact: you@example.com)"
```

- `-Mode zip` downloads `submissions.zip` and extracts to `incoming_metadata/sec/submissions/`.
- `-Mode cik` downloads per-CIK JSON for the CIKs found in `out/security_master.csv`.
- Always provide a proper `UserAgent` per SEC fair-access guidance.

## Required symbol set
The repo ships with `config/universe_required.csv` for a minimal offline universe. Extend or replace
this file to tune your default symbol coverage.
