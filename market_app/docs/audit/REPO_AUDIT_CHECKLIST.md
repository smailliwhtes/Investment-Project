# Repo Alignment Checklist (Offline Monitor + NLP)

Use this checklist when verifying alignment with the offline market monitor + geopolitical/NLP scope.

## Security master build path

- [ ] `tools/build_security_master.py` builds `out/security_master.csv` from local Stooq TXT data.
- [ ] `tools/enrich_security_master.py` enriches the security master from offline caches.
- [ ] `out/security_master.csv` matches the required schema.

## Corpus configuration (offline)

- [ ] `CORPUS_DIR` environment variable points to the offline NLP corpus root.
- [ ] The corpus indexer runs offline against the local corpus folder.
- [ ] Inventory reports corpus file names and sizes (no network calls).

## GDELT ingestion (offline)

- [ ] GDELT CSV ingestion uses local files (no network required).
- [ ] Paths for GDELT data are documented and validated in inventory.

## Offline runtime guarantees

- [ ] Runtime code path makes no network calls.
- [ ] Provisioning scripts are the only network-enabled steps.
- [ ] Metadata caches are stored under `out/metadata_cache/`.
