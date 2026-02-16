param(
  [switch]$PauseAlways
)

$ErrorActionPreference = "Stop"

function Resolve-PythonExe {
  $repo = Split-Path -Parent $PSScriptRoot
  if ($env:VIRTUAL_ENV) {
    $py = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
    if (Test-Path $py) { return $py }
  }
  $repoVenv = Join-Path $repo ".venv\Scripts\python.exe"
  if (Test-Path $repoVenv) { return $repoVenv }
  return "python"
}

$pyExe = Resolve-PythonExe
Write-Host "[info] Using Python: $pyExe" -ForegroundColor Gray

Write-Host "== market_app gate: unit suite ==" -ForegroundColor Cyan
& $pyExe -m pytest -q
$code = $LASTEXITCODE
if ($code -ne 0) {
  Write-Host "Unit tests FAILED (exit $code)" -ForegroundColor Red
  if ($PauseAlways) { Read-Host "Press Enter to close" | Out-Null }
  exit $code
}

Write-Host "== market_app gate: E2E ==" -ForegroundColor Cyan
$e2e = Join-Path $PSScriptRoot "run_e2e.ps1"
if (!(Test-Path $e2e)) { throw "Missing E2E script: $e2e" }

& $e2e
$code = $LASTEXITCODE

if ($PauseAlways -or $code -ne 0) {
  if ($code -eq 0) { Write-Host "E2E PASSED (exit 0)" -ForegroundColor Green }
  else { Write-Host "E2E FAILED (exit $code)" -ForegroundColor Red }
  Read-Host "Press Enter to close" | Out-Null
}

exit $code