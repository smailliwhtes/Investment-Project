# Findings

Each finding includes severity (P0–P2), affected locations, why it matters, and the fix.

## Architecture Drift

### P2 — Multiple wrapper entrypoints
- **Location(s)**: `monitor.py`, `monitor_v2.py`, `scripts/runner.py`
- **Why it matters**: Multiple launchers can confuse operators and drift over time.
- **Fix**: Keep wrappers as thin shims that call `python -m market_monitor` (already enforced). Document the canonical CLI in README. (Applied in this refactor.) 

### P2 — Batch cursor file name implies JSON, but stores a scalar
- **Location(s)**: `config.json` (`paths.state_file`), `market_monitor/cli.py`
- **Why it matters**: File extension implies JSON, but content is a plain integer cursor, which can mislead users and tooling.
- **Fix**: Rename to `batch_state.txt` or update logic to store JSON metadata. (Documented; not changed to preserve backward compatibility.)

## Broken Wiring

### P1 — Tracked virtual environment files
- **Location(s)**: `.venv/` (tracked despite `.gitignore`)
- **Why it matters**: Checked-in virtual environments bloat the repo, make audits noisy, and risk stale dependencies.
- **Fix**: Remove `.venv/` from version control, keep `.gitignore`, and rebuild locally with `setup.ps1`. (Recommended.)

## Reliability Issues

### P1 — Providers lacked retry/backoff on transient HTTP failures
- **Location(s)**: `market_monitor/providers/*.py`
- **Why it matters**: Transient 429/5xx responses can halt runs and make caching less effective.
- **Fix**: Added shared HTTP backoff helper and wired throttling config into providers. (Applied in this refactor.)

### P2 — Doctor did not explain provider/network failures in actionable steps
- **Location(s)**: `market_monitor/doctor.py`
- **Why it matters**: Operators need clear fixes when diagnostics fail.
- **Fix**: Added plain-English failure messages with explicit fix steps and log pointers. (Applied in this refactor.)

## Usability Issues

### P2 — Missing `.env.example` for required API keys
- **Location(s)**: repo root
- **Why it matters**: Onboarding users can miss required environment variables.
- **Fix**: Added `.env.example` with required key names. (Applied in this refactor.)

### P2 — Lack of tool configuration for lint/format/type/security
- **Location(s)**: repo root
- **Why it matters**: No consistent tooling for enforcing style and reliability gates.
- **Fix**: Added `pyproject.toml` and `.pre-commit-config.yaml`, and documented usage in README. (Applied in this refactor.)

## Code Quality Issues

### P2 — Config schema and sample config drift
- **Location(s)**: `market_monitor/config_schema.py`, `config.json`
- **Why it matters**: Sample configs can omit new defaults, confusing users.
- **Fix**: Synced `config.json` with the `jitter_s` throttling setting. (Applied in this refactor.)

## Security Hygiene Issues

### P2 — Secrets handling documentation incomplete
- **Location(s)**: README, repo root
- **Why it matters**: Without explicit guidance, keys may be hard-coded.
- **Fix**: Documented environment variables and added `.env.example`. (Applied in this refactor.)
