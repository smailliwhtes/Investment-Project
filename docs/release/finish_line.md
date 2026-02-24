# Release finish line

Canonical document moved to [`docs/finish_line.md`](../finish_line.md).

## Local

```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File .\scripts\release_verify.ps1
```

## CI

Run workflow `release-verify` (`.github/workflows/release-verify.yml`).
