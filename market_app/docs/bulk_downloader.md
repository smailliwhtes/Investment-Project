# Bulk CSV Downloader: Design + Integration Plan

This document proposes a **bulk historical data downloader** that works with free CSV sources (governmental and non‑governmental) and integrates cleanly with the existing `market_monitor` pipeline.

> **Important**: This project is a monitoring and research tool. It does not provide investment advice or execute trades.

---

## Goals (Aligned to Your Request)

1. **Bulk CSV collection** from free sources (gov + non‑gov).
2. **Reproducible, structured data layouts** for ML/analytics workflows.
3. **Simple onboarding** for a novice investor (clear outputs, plain‑English insights).
4. **Guardrails** that avoid advice and encourage user‑controlled decisions.

---

## Proposed Directory Layout (Additive)

```
market_app/
├─ market_monitor/
│  ├─ bulk/
│  │  ├─ __init__.py
│  │  ├─ models.py        # BulkSource, BulkDownloadTask
│  │  ├─ planner.py       # build_download_plan(...)
│  │  └─ manifest.py      # read/write download manifests
│  └─ ...
├─ docs/
│  ├─ bulk_downloader.md  # this document
│  └─ product_roadmap.md  # novice-first experience
├─ data/
│  ├─ raw/
│  │  ├─ gov/             # government CSVs
│  │  └─ non_gov/         # non‑gov CSVs
│  ├─ curated/            # ML-ready datasets
│  └─ manifests/          # download manifests
```

---

## Bulk Download Model (Code Stubs Included)

**Design concepts** (already added as stubs):

- `BulkSource`: describes a CSV source (base URL, symbol template, optional archives).
- `BulkDownloadTask`: a resolved download target (source + URL + local destination).
- `build_download_plan(...)`: builds tasks for symbol‑by‑symbol or archive downloads.
- `BulkManifest`: a JSON manifest for reproducible download runs.

These stubs live under `market_monitor/bulk/`.

---

## Source Catalog (Examples Only)

**Government (free/public)**:
- **US Treasury** (yield curves / rates data).
- **BLS** (CPI, unemployment, labor stats).
- **BEA** (GDP, macroeconomic accounts).
- **SEC** (filings metadata; fundamentals require transformation).

**Non‑government (free)**:
- **Stooq** (current default in `market_monitor`).
- **Nasdaq Trader** symbol directories (already used for the universe).

> Note: Some sources expose CSVs but not bulk archives. The planner supports both modes.

---

## Data Lifecycle (End‑to‑End)

1. **Universe**: get symbol list (watchlist or Nasdaq universe).
2. **Bulk plan**: map symbols to source URLs.
3. **Download**: fetch CSVs and write to `data/raw/<source>/`.
4. **Standardize**: align columns, parse dates, enforce numeric types.
5. **Curate**: merge with macro data into `data/curated/`.
6. **Modeling**: feed clean, aligned data into ML pipelines.

---

## Minimal CLI Extension (Future)

Suggested CLI interface for bulk CSV runs:

```
python -m market_monitor bulk-download --config config.yaml --sources stooq,treasury
```

This would:
1. Build a download plan
2. Write a manifest
3. Execute downloads with retries and throttling

---

## Guardrails for a Novice‑First Experience

To keep the app **streamlined and no‑nonsense**:

- **Plain‑language notes** (no jargon).
- **Clear risk flags** (already in the pipeline).
- **Education cards** for each metric (what it is, why it matters).
- **No “buy/sell” language** anywhere.
- **Explainable scores** showing exactly what drove each rank.

---

## Next Steps (If You Want Full Implementation)

1. **Add a bulk downloader CLI command**.
2. **Add a source registry** in config (gov + non‑gov).
3. **Add a standardization pass** for CSV schema alignment.
4. **Add dataset build outputs** (CSV/Parquet).
5. **Add a UI/report format** tailored to novice readability.

If you want, I can implement each of these phases incrementally.
