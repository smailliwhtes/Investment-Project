# AGENTS.md — market_app / market_monitor

## Working agreements (must follow)
- Offline-first is the default. Never require internet at runtime. Any network features must be optional and strictly gated behind explicit flags.
- Deterministic outputs: given the same cached data + config, the pipeline must produce identical results.
- CPU fallback is mandatory for all functionality. GPU acceleration is optional and must be auto-detected.
- Keep changes small and testable: one task = one PR. Include tests and run commands listed in docs/codex/40_commands.md.

## Repo navigation
- Primary entrypoint: scripts/run.ps1
- Data provisioning (optional): scripts/provision_data.ps1
- Watchlist runner: tools/run_watchlist.py (or python -m <module> if applicable)
- Core code: market_monitor/ (or market_app/), tests/ for pytest

## Required checks before finishing any task
- Run: pytest (or the specific subset listed in docs/codex/40_commands.md)
- Run: the offline smoke pipeline with watchlists/watchlist_smoke.csv
- If editing PowerShell scripts: verify nonzero exit code on failure cases (missing data, pipeline errors)

## Output contract (non-negotiable)
All runs must write:
- outputs/<run_id>/eligible.csv
- outputs/<run_id>/scored.csv
- outputs/<run_id>/report.md
See docs/codex/20_output_contract.md for exact columns.

## Review guidelines (P0/P1)
P0 (must fix):
- Silent success on failure (wrong exit codes)
- Non-deterministic outputs without justification
- Breaking the output contract or CLI signatures
- Introducing mandatory network dependencies
- Missing/removed tests for touched modules

P1 (should fix):
- Weak error messages (no file/row context)
- Missing docs updates for new flags/files
- Unused config keys or dead code paths

## Dependency rules
- Prefer stdlib where practical.
- New dependencies require: justification + lock/update + minimal usage + tests.
- Avoid heavy ML deps unless the PR is explicitly a ML PR (see docs/codex/30_pr_ladder.md).
