param(
  [string]$Config = ".\config\config.yaml",
  [string]$OutRoot = ".\outputs\runs"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

Write-Host "[stage] checking git hygiene"
& (Join-Path $Root "git_hygiene_check.ps1")
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Git hygiene check failed. Fix tracked outputs/data/venv and rerun."
  exit $LASTEXITCODE
}

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
& $VenvPy -m pip install -r ".\requirements.txt" | Out-Null

$UseFixtures = $false
if (-not $env:MARKET_APP_NASDAQ_DAILY_DIR) {
  $UseFixtures = $true
}

if ($UseFixtures) {
  Write-Host "[stage] using fixture data paths"
  $env:MARKET_APP_NASDAQ_DAILY_DIR = (Resolve-Path ".\tests\fixtures\ohlcv")
  $WatchlistOverride = (Resolve-Path ".\tests\fixtures\watchlist.txt")
}

Write-Host "[stage] running tests"
& $VenvPy -m pytest -q
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Tests failed. Fix issues and rerun .\scripts\acceptance.ps1"
  exit $LASTEXITCODE
}

$RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$RunDir = Join-Path $OutRoot $RunId
if (-not (Test-Path $RunDir)) { New-Item -ItemType Directory -Path $RunDir | Out-Null }

Write-Host "[stage] running blueprint wrapper"
if ($WatchlistOverride) {
  $env:MARKET_APP_WATCHLIST_FILE = $WatchlistOverride
}
& $VenvPy -m market_app.cli --config $Config --offline --run_id $RunId --top_n 5 --conservative
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Pipeline run failed. See logs in outputs\runs\$RunId"
  exit $LASTEXITCODE
}

$Expected = @(
  "universe.csv",
  "classified.csv",
  "features.csv",
  "eligible.csv",
  "scored.csv",
  "regime.json",
  "report.md",
  "manifest.json"
)
foreach ($pattern in $Expected) {
  $path = Join-Path $RunDir $pattern
  if (-not (Test-Path $path)) {
    Write-Host "[error] Missing expected output ($pattern) in $RunDir"
    exit 1
  }
  $file = Get-Item $path
  if ($file.Length -le 0) {
    Write-Host "[error] Output file is empty ($path)"
    exit 1
  }
}

Write-Host "[done] Acceptance run completed -> $RunDir"
