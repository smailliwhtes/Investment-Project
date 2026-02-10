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

$ExternalOhlcv = $env:MARKET_APP_OHLCV_DIR
$ExternalSymbols = $env:MARKET_APP_SYMBOLS_DIR
if (-not (Test-Path $OutRoot)) {
  New-Item -ItemType Directory -Path $OutRoot | Out-Null
}
$env:MARKET_APP_OUTPUT_DIR = (Resolve-Path $OutRoot)

Write-Host "[stage] using bundled sample data (unless external paths are provided)"
if (-not $ExternalOhlcv) {
  $env:MARKET_APP_OHLCV_DIR = (Resolve-Path ".\tests\data\ohlcv")
}
if (-not $ExternalSymbols) {
  $env:MARKET_APP_SYMBOLS_DIR = (Resolve-Path ".\tests\data\symbols")
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

Write-Host "[stage] running offline pipeline with sample data"
& $VenvPy -m market_app.cli --config $Config --offline --run-id $RunId --top-n 5
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Pipeline run failed. See logs in outputs\runs\$RunId"
  exit $LASTEXITCODE
}

$ValidateCmd = @(
  "-m", "market_app.cli", "validate", "--config", $Config, "--offline"
)
Write-Host "[stage] validating sample data"
& $VenvPy @ValidateCmd
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Validation failed. Fix data issues and rerun."
  exit $LASTEXITCODE
}

$Expected = @(
  "universe.csv",
  "classified.csv",
  "features.csv",
  "eligible.csv",
  "scored.csv",
  "report.md",
  "manifest.json",
  "run.log"
)
function Test-RunOutputs {
  param(
    [string]$RunDirectory
  )
  foreach ($pattern in $Expected) {
    $path = Join-Path $RunDirectory $pattern
    if (-not (Test-Path $path)) {
      Write-Host "[error] Missing expected output ($pattern) in $RunDirectory"
      exit 1
    }
    $file = Get-Item $path
    if ($file.Length -le 0) {
      Write-Host "[error] Output file is empty ($path)"
      exit 1
    }
  }
}

Test-RunOutputs -RunDirectory $RunDir

if ($ExternalOhlcv) {
  Write-Host "[stage] running offline pipeline with external OHLCV"
  $env:MARKET_APP_OHLCV_DIR = $ExternalOhlcv
  if ($ExternalSymbols) {
    $env:MARKET_APP_SYMBOLS_DIR = $ExternalSymbols
  }
  $ExternalRunId = "${RunId}_external"
  & $VenvPy -m market_app.cli --config $Config --offline --run-id $ExternalRunId --top-n 5
  if ($LASTEXITCODE -ne 0) {
    Write-Host "[error] External pipeline run failed. See logs in outputs\runs\$ExternalRunId"
    exit $LASTEXITCODE
  }
  $ExternalRunDir = Join-Path $OutRoot $ExternalRunId
  Test-RunOutputs -RunDirectory $ExternalRunDir

  Write-Host "[stage] validating external data"
  & $VenvPy -m market_app.cli validate --config $Config --offline
  if ($LASTEXITCODE -ne 0) {
    Write-Host "[error] External validation failed. Fix data issues and rerun."
    exit $LASTEXITCODE
  }
}

Write-Host "[done] Acceptance run completed -> $RunDir"
