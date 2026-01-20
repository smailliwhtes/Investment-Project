param(
  [string]$Config = ".\config.yaml",
  [string]$OutRoot = ".\outputs\acceptance"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

function Get-PythonSpec {
  $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
  if ($pyLauncher) {
    try {
      & py -3.11 -c "import sys" | Out-Null
      if ($LASTEXITCODE -eq 0) { return @{ exe="py"; args=@("-3.11") } }
    } catch {}
  }
  $py = Get-Command python -ErrorAction SilentlyContinue
  if ($py) { return @{ exe="python"; args=@() } }
  throw "No Python found. Install Python 3.11 and reopen PowerShell."
}

$PySpec = Get-PythonSpec
$VenvPath = Join-Path $Root "..\.venv"
$VenvPy   = Join-Path $VenvPath "Scripts\python.exe"

if (-not (Test-Path $VenvPy)) {
  Write-Host "[stage] creating venv"
  & $PySpec.exe @($PySpec.args) -m venv $VenvPath
}

Write-Host "[stage] upgrading pip"
& $VenvPy -m pip install --upgrade pip 1>$null
Write-Host "[stage] installing requirements"
& $VenvPy -m pip install -r ".\requirements.txt" 1>$null

Write-Host "[stage] running tests"
& $VenvPy -m pytest -q
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Tests failed. Fix issues and rerun .\scripts\acceptance.ps1"
  exit $LASTEXITCODE
}

if (-not $env:NASDAQ_DAILY_DIR -and -not $env:MARKET_APP_DATA_ROOT) {
  Write-Host "[skip] NASDAQ_DAILY_DIR not set. Skipping pipeline run."
  exit 0
}

$RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$RunDir = Join-Path $OutRoot "run_$RunId"
if (-not (Test-Path $RunDir)) { New-Item -ItemType Directory -Path $RunDir | Out-Null }

Write-Host "[stage] running watchlist pipeline"
& $VenvPy -m market_monitor run --config $Config --mode watchlist --outdir $RunDir
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Pipeline run failed. See logs in .\outputs\logs"
  exit $LASTEXITCODE
}

$Expected = @("features_*.csv", "scored_*.csv", "eligible_*.csv", "run_report*.md")
foreach ($pattern in $Expected) {
  $found = Get-ChildItem -Path $RunDir -Filter $pattern | Select-Object -First 1
  if (-not $found) {
    Write-Host "[error] Missing expected output ($pattern) in $RunDir"
    exit 1
  }
}

Write-Host "[done] Acceptance run completed -> $RunDir"
