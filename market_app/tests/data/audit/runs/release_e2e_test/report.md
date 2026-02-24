# Offline Monitor Report

- Run ID: `release_e2e_test`
- Generated (UTC): `2026-02-24T00:57:09.132610+00:00`
- Offline mode: `True`

## Universe Summary
- Symbols: 4
- Eligible: 4
- Ineligible: 0

## Theme Distribution
- strategic_tech: 2
- defense: 1

## Eligibility Breakdown

| symbol | eligible | gate_fail_reasons |
| --- | --- | --- |
| AAPL | True |  |
| DEFN | True |  |
| MSFT | True |  |
| SPY | True |  |

## Top Eligible Symbols

| symbol | monitor_score | risk_level | risk_flags | themes |
| --- | --- | --- | --- | --- |
| SPY | 10 | AMBER | theme_uncertain |  |
| MSFT | 7 | GREEN |  | strategic_tech |
| AAPL | 4 | GREEN |  | strategic_tech |
| DEFN | 1 | GREEN |  | defense |

## Data Quality

- as_of_date chosen from SPY benchmark date (SPY if available, else global max).

| symbol | last_date | as_of_date | lag_days | missing_data | stale_data | volume_missing |
| --- | --- | --- | --- | --- | --- | --- |
| AAPL | 2023-12-29 | 2025-01-31 | 399 | False | False | False |
| DEFN | 2023-12-29 | 2025-01-31 | 399 | False | False | False |
| MSFT | 2023-12-29 | 2025-01-31 | 399 | False | False | False |
| SPY | 2023-02-24 | 2025-01-31 | 707 | False | False | False |

## Context Summary

- No local corpus features available for this run.