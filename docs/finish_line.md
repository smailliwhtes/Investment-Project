# Finish line gate

## One-command local gate

Run:

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\release_verify.ps1
```

This is the strict default path (no skip switches). It fails fast and writes logs/report files under `audit/`.

## One-workflow CI gate

Run workflow: **`release-verify`** (`.github/workflows/release-verify.yml`).

Triggers:
- pull_request
- push to `main`
- workflow_dispatch

## What must pass

1. Python unit/contract tests.
2. .NET GUI tests (Windows).
3. Offline E2E engine run.
4. Hard contract: `scored.csv` has `last_date` and `lag_days` and they match `data_quality.csv` row-for-row by `symbol`.
5. SBOMs in `audit/sbom/` for Python and .NET (CycloneDX JSON).
6. Python dependency vulnerability audit (`pip-audit --strict`).

## Release/provenance gate

Use workflow: **`release-build`** (`.github/workflows/release-build.yml`) via tag (`v*`) or manual dispatch.

It builds release artifacts, uploads them, and attempts provenance attestations (`actions/attest-build-provenance`) and SBOM attestations (`actions/attest-sbom`) when supported by repository settings/plan.

If attestations are unsupported in the runtime environment, the workflow emits a clear warning and continues with artifacts + SBOMs.

## Audit notes (what existed / what was missing)

### Existed
- Local launch-readiness gate script: `scripts/release_verify.ps1`.
- Offline E2E entrypoint: `python -m market_monitor.cli run --config ... --offline --progress-jsonl`.
- CI workflows for tests and GUI build.
- Existing finish-line doc (`docs/release/finish_line.md`).

### Missing before this change
- Hard post-E2E staleness contract verification against `data_quality.csv`.
- Python + .NET SBOM generation together in predictable `audit/sbom/` location.
- pip-audit as a default strict gate.
- CodeQL workflow enabled in-repo.
- Dedicated release workflow for artifact provenance attestations.
- Single doc that maps local gate and CI gate to a unified definition of done.

## Why these gates exist (SSDF alignment)

These controls map to NIST SSDF practices: verify software integrity, detect known vulnerabilities, enforce reproducible build outputs, and retain evidence (logs/artifacts/SBOMs) for auditability.
