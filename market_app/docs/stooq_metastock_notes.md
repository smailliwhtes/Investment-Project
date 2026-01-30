# Stooq MetaStock Pack Notes

The MetaStock pack under `d_us_ms\data\daily\us` is **optional** metadata. In observed user
snapshots:

- `EMASTER`, `MASTER`, and `XMASTER` exist in leaf directories.
- `F*.DOP` files exist.
- `F*.DAT` and `F*.MWD` are missing, so OHLCV data is likely **not present** in MetaStock format.

## Current handling
- The security master builder attempts to parse `XMASTER` as **best-effort** symbol/name text.
- If `XMASTER` is binary or not parseable, the build continues without it.
- NASDAQ Trader symbol directory files remain the primary source of name/exchange/ETF data.

## Recommendation
Treat the MetaStock pack as a **non-blocking enrichment** only. If future snapshots include
readable name mappings, they can improve coverage where Nasdaq Trader data is missing, but the
offline pipeline does not depend on it.
