# Output Schemas (market_app local v2)

Schema version: **1.0.0**

## universe.csv
Columns (ordered):
1. symbol
2. name
3. exchange
4. asset_type
5. is_etf
6. is_test_issue
7. is_leveraged
8. is_inverse
9. country
10. source_file

## classified.csv
Columns:
1. symbol
2. name
3. themes
4. theme_confidence
5. theme_evidence
6. theme_uncertain

## features.csv
Columns:
1. symbol
2. as_of_date
3. history_days
4. return_1m
5. return_3m
6. return_6m
7. return_12m
8. sma20
9. sma50
10. sma200
11. close_to_sma20
12. close_to_sma50
13. close_to_sma200
14. pct_days_above_sma200_6m
15. volatility_20d
16. volatility_60d
17. downside_volatility
18. worst_5d_return_6m
19. max_drawdown_6m
20. adv20_usd
21. zero_volume_fraction_60d
22. missing_data
23. stale_data
24. split_suspect
25. volume_missing

## eligible.csv
Columns:
1. symbol
2. eligible
3. gate_fail_reasons

## scored.csv
Columns:
1. symbol
2. monitor_score
3. total_score
4. risk_flags
5. risk_level
6. themes
7. theme_confidence
8. predicted_risk_signal
9. model_id
10. model_schema_version
