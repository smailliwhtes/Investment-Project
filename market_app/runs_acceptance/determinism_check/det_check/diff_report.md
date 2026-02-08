# Determinism Check Report

As-of date: 2026-02-08
Run A: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\det_check_1
Run B: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\det_check_2

## Result: PASS

No disallowed diffs detected.

## Per-file Summary
- eligible.csv: match (diff_keys=[])
- scored.csv: diff (diff_keys=['run_id'])
  - examples: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\determinism_check\det_check\diff_examples_scored.csv
- features.csv: diff (diff_keys=['run_id'])
  - examples: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\determinism_check\det_check\diff_examples_features.csv
- classified.csv: match (diff_keys=[])
- universe.csv: match (diff_keys=[])
- manifest.json: diff (diff_keys=['run_id'])
- digest.json: match (diff_keys=[])
- report.md: match (diff_keys=[])
- report.html: absent (diff_keys=[])
