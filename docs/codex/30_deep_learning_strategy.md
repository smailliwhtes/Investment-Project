# Deep Learning Strategy

This repo already has an additive ML seam:

- `market_app/market_monitor/prediction.py` writes `ml_signal`, `ml_model_id`, and `ml_featureset_id` into `scored.csv`.
- `market_app/market_monitor/ml/` owns dataset assembly, train/predict manifests, and offline artifact writing.
- The GUI already treats model outputs as diagnostics layered on top of the engine, not as duplicated UI logic.

That makes neural networks or deeper sequence models feasible without breaking the current contracts, as long as they stay additive and offline-first.

## Recommended Integration Path

1. Add a new backend under `market_app/market_monitor/ml/` rather than creating a parallel modeling stack.
2. Keep the output contract identical:
   - write under `<out_dir>/ml/`
   - continue emitting `predictions_latest.csv`
   - continue attaching `ml_signal`, `ml_model_id`, and `ml_featureset_id` in `scored.csv`
3. Gate everything behind config:
   - `policy.deep_learning.enabled` for policy lanes
   - `prediction.enabled` and a future backend selector for the general forecasting lane
4. Preserve deterministic training and inference:
   - fixed seeds
   - persisted train/predict manifests
   - no runtime internet fetches

## Best-Fit Deep Learning Use Cases

- Sequence models on OHLCV plus exogenous context:
  - temporal CNN
  - small LSTM/GRU
  - lightweight transformer encoder
- Event-conditioned policy simulation:
  - encode policy-event text or ontology tags into dense vectors
  - concatenate with regime and market features
  - predict forward return distributions or impact buckets
- Cross-sectional ranking:
  - use a shallow MLP over engineered factors plus policy context
  - keep the final ranking contract unchanged

## Constraints

- Do not move scoring or eligibility logic into the GUI.
- Do not require GPU availability.
- Do not replace the current deterministic classical models unless the deep-learning lane proves better in offline walk-forward tests.
- Keep explainability available:
  - integrated gradients, SHAP-on-embeddings, or deterministic feature-attribution proxies

## Practical Next Step

The clean next step is a small PyTorch backend under `market_app/market_monitor/ml/` that consumes the existing dataset contract and writes the same `ml/` artifacts as the sklearn/xgboost lane. That preserves the run folder contract, keeps the GUI unchanged, and lets the repo compare classical versus neural models in the same offline evaluation harness.
