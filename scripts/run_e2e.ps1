$ErrorActionPreference = 'Stop'

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Write-Error "python was not found in PATH. Install Python and retry."
  exit 1
}

if (-not $env:VIRTUAL_ENV) {
  Write-Warning "No active virtual environment detected (VIRTUAL_ENV is empty)."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..')
$appRoot = Join-Path $repoRoot 'market_app'

if (-not (Test-Path $appRoot)) {
  Write-Error "market_app directory not found at expected path: $appRoot"
  exit 1
}

Push-Location $appRoot
python -m pytest -q tests/test_offline_e2e_market_and_corpus.py -k offline_e2e --maxfail=1
$exitCode = $LASTEXITCODE
Pop-Location

exit $exitCode
