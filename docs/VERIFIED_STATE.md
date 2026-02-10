## CURRENT STATE VERIFIED

Verified against repo root `/workspace/Investment-Project` and app root `market_app/` (no nested `AGENTS.md` overrides found). The primary CLI entrypoint is `python -m market_app.cli`, and the PowerShell wrapper is `market_app/scripts/run.ps1`. The default working directory is the repo root; paths in `market_app/config/config.yaml` are relative to the config file unless overridden by env/CLI. Config precedence for the local (v2) pipeline is **CLI args > environment variables > YAML defaults**.

### Entrypoints
- `market_app/src/market_app/cli.py` (local pipeline + blueprint-compatible pipeline)
- `market_app/scripts/acceptance.ps1` (fresh-clone acceptance harness)
- `market_app/scripts/run.ps1` (one-command wrapper)

### Expected working directory
- Repo root (`/workspace/Investment-Project`) for both bash and PowerShell.

### Config precedence
- `market_app/local_config.py` loads defaults, merges YAML, applies env overrides, then CLI overrides (CLI wins).
- Offline defaults are `offline: true`, `online: false`.

### Current outputs (local v2 pipeline)
- `outputs/runs/<run_id>/universe.csv`
- `outputs/runs/<run_id>/classified.csv`
- `outputs/runs/<run_id>/features.csv`
- `outputs/runs/<run_id>/eligible.csv`
- `outputs/runs/<run_id>/scored.csv`
- `outputs/runs/<run_id>/report.md`
- `outputs/runs/<run_id>/manifest.json`

### Current tests
- Primary test command: `python -m pytest -q` (see `docs/codex/40_commands.md`).
- Existing tests live under `market_app/tests/`.

### Known limitations (pre-change)
- Local symbol ingestion accepts pipe/csv listings but does not fully enforce Nasdaq Symbol Directory variants.
- Local OHLCV ingestion does not report granular validation issues (duplicate dates, staleness, malformed rows).
- No `validate` CLI command for offline data hygiene reports.
