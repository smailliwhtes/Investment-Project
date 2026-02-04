# AGENTS.md — market_app / market_monitor

## A) Repository expectations
- **Offline-first**: No mandatory runtime internet calls. Tests must be hermetic and runnable offline.
- **Determinism**: Seed randomness, use content hashing for datasets/artifacts, and keep output naming stable.
- **Platform**: Windows-first, but must support Linux/macOS runners. PowerShell wrappers must exit nonzero on failure.
- **Change scope**: One task = one PR. Keep changes small and testable.

## B) Entry points and commands
- **Repo root**: `/workspace/Investment-Project`
- **App root**: `market_app/`
- **Primary entrypoint**: `scripts/run.ps1`
- **Data provisioning (optional)**: `scripts/provision_data.ps1`
- **Watchlist runner**: `tools/run_watchlist.py`
- **Authoritative commands after changes**: see `docs/codex/40_commands.md` (pytest + offline smoke run required).

## C) Output contracts
- Required outputs for every run:
  - `outputs/<run_id>/eligible.csv`
  - `outputs/<run_id>/scored.csv`
  - `outputs/<run_id>/report.md`
- **Fail fast** if outputs or columns are missing.
- Full contract: `docs/codex/20_output_contract.md`.

## D) Data contracts
- Schemas, manifests, and partitioning rules: `docs/codex/10_data_contracts.md`.
- Corpus roots are configured via config and env overrides (see `10_data_contracts.md`).

## E) Review guidelines
**P0 (must fix)**
- Silent success on failure (wrong exit codes)
- Non-deterministic outputs without justification
- Breaking the output contract or CLI signatures
- Introducing mandatory network dependencies
- Missing/removed tests for touched modules

**P1 (should fix)**
- Weak error messages (no file/row context)
- Missing docs updates for new flags/files
- Unused config keys or dead code paths

## Verification
Before coding, confirm you loaded this `AGENTS.md` and call out any nested `AGENTS.md` overrides found in subdirectories.
