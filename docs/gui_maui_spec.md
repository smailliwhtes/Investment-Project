# .NET MAUI GUI Spec (Phase 0)

## AGENTS.md confirmation
Loaded the repository-root `AGENTS.md`. No nested `AGENTS.md` overrides were found under touched paths.

## PDF-driven actionable requirements
`GUI design guidelines.pdf` is treated as primary guidance. In this environment, full text extraction tooling was unavailable, but extracted headings and references confirm these priorities:
- virtualization-first symbol table architecture;
- async run orchestration with throttled progress updates;
- explainability-first details (gates, flags, feature evidence + recency);
- secure local secret handling;
- settings editor with schema validation;
- timeline/diffing + data staleness indicators (`last_date`, `lag_days`).

## Required MVP screens
1. Dashboard split-pane: virtualized symbol list + detail tabs (Summary / Charts / Explain).
2. Runs timeline with run metadata and run-to-run diffing.
3. Settings with guided config editing and immediate validation.
4. Logs page with redacted diagnostics copy.

## Interaction flows
- Load run artifacts from `outputs/runs/<run_id>/...` and render immediately.
- Start/cancel run from UI via external Python process.
- Stream progress JSONL events and update progress bar, stage, and logs without blocking UI thread.
- Enable early browsing when `scored.csv` appears.

## Performance constraints
- Symbol list must stay smooth at 5k+ rows using virtualization/incremental loading.
- Charts must remain interactive with downsampling (target 2k–5k plotted points max).
- Progress events throttled at engine/UI boundary (~200 ms) to avoid UI stalls.

## Engine integration contract (minimal)
CLI:
- `market-monitor run --config <path> --out-dir <run_dir> [--offline] [--progress-jsonl]`
- `market-monitor validate-config --config <path> --format json`

Progress JSONL schema:
```json
{"ts":"2026-01-19T01:02:03Z","stage":"features","pct":40,"message":"start: symbols=5000"}
```

Run artifact contract:
- required: `eligible.csv`, `scored.csv`, `report.md`
- plus diagnostics: `run_manifest.json`, `diagnostics.json`, `logs/run.log`

## MVP vs Later
### MVP
- Windows-first MAUI shell + core services.
- LiveCharts2 default provider behind `IChartProvider` abstraction.
- secure storage via MAUI `SecureStorage`.
- settings validation via CLI call.

### Later
- Syncfusion/Telerik providers (compile-time optional).
- richer diff visualizations and advanced chart annotations.
- deeper explainability provenance tracing.
