# Market Monitor Report

Generated: 2026-02-08T15:25:41.026949Z
Run ID: gui_ready_check_2
Run timestamp: 20260208_152540

## Stage Summary

- Universe size: 3
- Stage 1 survivors: 3
- Stage 2 eligible prelim: 3
- Stage 3 scored: 3

## Data Source Usage

- Offline mode: True
- Provider: nasdaq_daily
- MARKET_APP_NASDAQ_DAILY_DIR: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\tests\fixtures\ohlcv
- NASDAQ data found: True
- MARKET_APP_SILVER_PRICES_DIR: unset
- Silver data found: False
- MARKET_APP_GDELT_CONFLICT_DIR: unset
- GDELT corpus found: False

## Missing Field Handling

- Volume-missing symbols are scored with explicit penalties and liquidity overrides.
- Volume-dependent features are set to NA when volume is unavailable.

## Top Monitor Priority

| Symbol | Name | Monitor Score | Risk | Confidence | Notes |
| --- | --- | --- | --- | --- | --- |
| AAA | AAA | 10 | AMBER | 0.59 | Data issue |
| BBB | BBB | 5 | AMBER | 0.52 | Data issue |
| SPY | SPY | 1 | AMBER | 0.52 | Data issue |

## Notes
This report is monitoring-only and contains no trading recommendations.