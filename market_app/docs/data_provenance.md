# Data Provenance (Offline-First)

The blueprint wrapper assumes **all datasets are pre-provisioned** and stored
locally under `data/raw/` (symbols, OHLCV, macro, and optional corpora). The
runtime pipeline never downloads data when offline mode is enabled.

## Expected Locations

- `data/raw/symbols/` – exchange symbol lists
- `data/raw/ohlcv/` – per‑symbol OHLCV CSVs
- `data/raw/macro/` – macro series CSVs (date/value)
- `data/raw/metadata/` – provenance JSON entries (per file)

## Provenance Metadata

For every external file, store a JSON record under `data/raw/metadata/`:

```json
{
  "dataset": "nasdaq_symbols",
  "provider": "NASDAQ Trader",
  "retrieval_date": "2025-12-31",
  "source_url": "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
  "license": "public domain",
  "checksum": "<sha256>"
}
```

The wrapper’s `manifest.json` includes dataset checksums and paths for each
referenced input file. This provides traceability without requiring network
access during runs.
