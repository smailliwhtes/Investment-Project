# Determinism Check Report

As-of date: 2026-02-08
Run A: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\gui_ready_check_1
Run B: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\gui_ready_check_2

## Result: FAIL

Disallowed diffs detected:
- manifest.json: manifest_hash, outputs.digest.json.sha256, outputs.manifest.json.bytes, outputs.manifest.json.sha256
- digest.json: outputs.digest.json.sha256, outputs.manifest.json.sha256

## Per-file Summary
- eligible.csv: match (diff_keys=[])
- scored.csv: diff (diff_keys=['run_id'])
  - examples: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\determinism_check\gui_ready_check\diff_examples_scored.csv
- features.csv: diff (diff_keys=['run_id'])
  - examples: C:\Users\micha\OneDrive\Desktop\Investment-Project\market_app\runs_acceptance\determinism_check\gui_ready_check\diff_examples_features.csv
- classified.csv: match (diff_keys=[])
- universe.csv: match (diff_keys=[])
- manifest.json: diff (diff_keys=['manifest_hash', 'outputs.digest.json.sha256', 'outputs.manifest.json.bytes', 'outputs.manifest.json.sha256', 'run_id'])
- digest.json: diff (diff_keys=['outputs.digest.json.sha256', 'outputs.manifest.json.sha256'])
- report.md: match (diff_keys=[])
- report.html: absent (diff_keys=[])
