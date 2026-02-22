# Release finish line (v0.1)

If the command below exits `0`, this repository is **launch-ready** for v0.1:

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\release_verify.ps1
```

## Binary Definition of Done

`release_verify.ps1` must PASS all required gates:

1. **Inventory + drift**: regenerate tracked file inventory and SHA256 table from `git ls-files`.
2. **Runtime manifest**: validate `docs/runtime_required_files.yaml` required/either rules.
3. **Engine tests**: Python tests pass (`pytest -q`) when engine is present.
4. **Offline E2E**: deterministic offline engine run succeeds and writes run artifacts.
5. **GUI smoke**: MAUI app starts with `--smoke`, emits deterministic READY signal, holds, exits `0`.
6. **Artifacts**: `audit/verify_report.json` + gate logs are written and CI uploads `audit/**`.

Optional gates may be `skipped` with explicit reason (for example SBOM tooling missing), but required gates cannot be skipped.

## Why SBOM is included

An SBOM is a nested inventory of software components. It is standard software supply-chain hygiene and gives an auditable dependency snapshot for release review.
