# Offline ML (Monitoring-Oriented, No Advice)

This document defines the **offline-only** ML training and inference workflow for `market_app` and the leakage rules used when joining geopolitical signals to market data.

## Label definition (MVP)
- **Label:** `label_high_vol` (monitoring-oriented volatility risk flag).
- **Definition:** For each symbol/day, compute the next-5-day realized volatility using daily returns. The label is **true** when that realized volatility exceeds the 75th percentile of the training window distribution.
- **Purpose:** Produces a probability-like `predicted_risk_signal` in scoring runs (no buy/sell language).

## Geopolitics feature contract (GDELT Events)
- Input: local CSV/Parquet files containing GDELT Events columns (SQLDATE/Date, EventRootCode, QuadClass, AvgTone, GoldsteinScale, NumMentions, NumSources, NumArticles).
- Output daily features:
  - `events_count`
  - `tone_mean`, `tone_std`
  - `goldstein_mean`
  - `mentions_sum`, `sources_sum`, `articles_sum`
  - `root_01` … `root_20` (EventRootCode counts)
  - `quad_1` … `quad_4` (QuadClass counts)

## No-lookahead rule
- For market features on day **D**, geopolitics features must come from **D-1 or earlier**.
- The pipeline enforces this by shifting GDELT daily features **forward by 1 day** before joining to market data.

## Training command
```
python -m market_app.cli train --config config/config.yaml --asof-end YYYY-MM-DD
```

Outputs:
- `outputs/training/<run_id>/metrics.json`
- `outputs/training/<run_id>/feature_importance.csv`
- `outputs/training/<run_id>/report.md`
- `models/<model_id>/model.joblib`
- `models/<model_id>/model_manifest.json`

## Inference integration
- Scoring runs look for the newest compatible model under `models/`.
- If found, `scored.csv` is extended with:
  - `predicted_risk_signal` (0–1)
  - `model_id`
  - `model_schema_version`
- If no model exists or features are incompatible, scoring proceeds without predictions and leaves these columns blank.

## Determinism & Offline Constraints
- Training and inference run **offline** by default; network calls are blocked when offline.
- Input hashes for geopolitics data are stored in the model manifest to support reproducibility.
