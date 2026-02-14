param(
  [switch]$PauseOnFail
)

$ErrorActionPreference = "Stop"

function Write-Warn($msg) { Write-Host "[warn] $msg" -ForegroundColor Yellow }
function Write-Info($msg) { Write-Host "[info] $msg" -ForegroundColor Cyan }
function Write-Err($msg)  { Write-Host "[error] $msg" -ForegroundColor Red }

# Resolve repo root from this script location
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Resolve-Path (Join-Path $scriptDir "..")

Set-Location $repoRoot

# Prefer repo-local venv python if present
$venvPy = Join-Path $repoRoot ".venv\Scripts\python.exe"
$python = $null

if (Test-Path $venvPy) {
  $python = $venvPy
} else {
  $cmd = Get-Command python -ErrorAction SilentlyContinue
  if ($cmd) { $python = $cmd.Source }
}

if (-not $python) {
  Write-Err "Python not found. Install Python 3.x and/or create .venv."
  exit 127
}

# Warn if no active venv (still run)
if (-not $env:VIRTUAL_ENV -and -not $env:CONDA_PREFIX) {
  if (Test-Path (Join-Path $repoRoot ".venv")) {
    Write-Warn "No active venv detected (VIRTUAL_ENV/CONDA_PREFIX empty). A repo .venv exists; consider activating it."
  } else {
    Write-Warn "No active venv detected (VIRTUAL_ENV/CONDA_PREFIX empty)."
  }
}

Write-Info "Repo root: $repoRoot"
Write-Info "Using Python: $python"

$testPath = "tests\test_offline_e2e_market_and_corpus.py"
if (-not (Test-Path $testPath)) {
  Write-Err "E2E test not found at expected path: $testPath (repo root tests/)."
  exit 2
}

# Run the hermetic offline E2E test via pytest
& $python -m pytest -q $testPath -k offline_e2e --maxfail=1
$code = $LASTEXITCODE

if ($code -ne 0) {
  Write-Err "E2E test failed with exit code $code."
  if ($PauseOnFail) {
    Write-Host ""
    Read-Host "Press ENTER to close"
  }
  exit $code
}

Write-Info "E2E test passed."
exit 0
