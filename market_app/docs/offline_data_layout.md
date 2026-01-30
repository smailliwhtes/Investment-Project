# Offline Data Layout (Stooq TXT + MetaStock)

This project runs fully offline at runtime. Data provisioning happens separately, and the
runtime consumes local snapshots only. This document describes the supported on-disk layout
for the Stooq TXT daily pack and the optional MetaStock pack, plus how the inventory tool
verifies those paths.

## Stooq TXT daily US pack

Root example:

```
C:\Users\micha\OneDrive\Desktop\Market_Files\d_us_txt\data\daily\us
```

Observed subfolders:

- `nasdaq etfs` (contains `*.us.txt` directly)
- `nasdaq stocks` (numeric subfolders `1`, `2`, `3`)
- `nyse etfs` (numeric subfolders `1`, `2`)
- `nyse stocks` (numeric subfolders `1`, `2`)
- `nysemkt stocks` (contains `*.us.txt` directly)
- (optionally) `nysemkt etfs`

Each file is CSV-like with angle-bracket headers:

```
<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
AADR.US,D,20100721,000000,23.1646,23.1646,22.7969,22.7969,45503.680330826,0
```

The parser expects the angle-bracket header format, reads daily OHLCV, ignores `TIME`, and
tolerates missing or zero volumes.

## MetaStock daily US pack (optional metadata)

Root example:

```
C:\Users\micha\OneDrive\Desktop\Market_Files\d_us_ms\data\daily\us
```

Expected files:

- `XMASTER` (contains symbol/name strings; can be parsed as ASCII)
- `EMASTER`, `MASTER`
- Many `F*.DOP` files

The MetaStock pack is optional metadata used for name enrichment. The inventory tool checks
for `XMASTER` and `F*.DOP` counts but does not require this data for minimal operation.

## Inventory verification

Run:

```
python -m market_monitor.tools.inventory --stooq-root "<stooq_root>" --write-out out
```

The tool writes:

- `out/inventory.json`
- `out/inventory.md`

Inventory includes:

- Existence and file counts per Stooq bucket (e.g., `nasdaq stocks/1`).
- MetaStock file counts (XMASTER, `F*.DOP`).
- NLP corpus top-level file list with sizes (if provided).
- Validation of `out/security_master.csv` schema.
