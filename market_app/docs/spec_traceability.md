# Specification-to-Code Traceability

| Requirement | Implementation | Test Evidence |
|---|---|---|
| Liquidity gate (ADV20$ minimum) | `apply_gates` checks `adv20_usd < min_adv20_usd`; `build_risk_flags` emits `adv20_below_min`. | `tests/test_local_gates.py` (existing gate tests) and integration in `tests/test_mini_dataset_offline_integration.py`. |
| Minimum history length gate | `apply_gates` checks `history_days < min_history_days`; risk flag includes `insufficient_history`. | `tests/test_local_gates.py`; `tests/test_mini_dataset_offline_integration.py`. |
| Price floor gate | `apply_gates` computes price proxy and checks `< price_floor`; risk flags mirror same check. | `tests/test_local_gates.py`. |
| Volatility / drawdown / tail-risk proxies | `compute_features` computes `volatility_60d`, `max_drawdown_6m`, `worst_5d_return_6m`; `build_risk_flags` maps thresholds to RED/AMBER flags. | `tests/test_local_features.py` and existing risk/scoring tests. |
| Missing volume and zero-volume fraction handling | `compute_features` computes `volume_missing` and `zero_volume_fraction_60d`; `apply_gates` checks `zero_volume_max_frac`; scoring penalizes `volume_missing`. | `tests/test_local_gates.py`; `tests/test_mini_dataset_offline_integration.py`. |
| No look-ahead behavior | `compute_features` uses trailing windows ending at latest row and no forward labels in runtime scoring path. | Deterministic reruns in `tests/test_mini_dataset_offline_integration.py`. |
| Offline runtime guard | `enforce_offline_network_block` monkeypatches socket connect/create_connection/getaddrinfo and raises `OfflineNetworkError`. | `tests/test_offline_guard.py` and existing `tests/test_no_network.py`. |
| Manifest/provenance with hashes | `run_offline_pipeline` writes `manifest.json` from `manifest_local.py` with config hash/input hashes/git SHA. | `tests/test_manifest_digest_determinism.py`; integration smoke in `tests/test_mini_dataset_offline_integration.py`. |
