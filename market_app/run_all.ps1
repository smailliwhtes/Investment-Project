param(
  [string]$Config = ".\config.yaml",
  [ValidateSet("universe","watchlist","themed","batch")]
  [string]$Mode = "watchlist",
  [string]$Watchlist,
  [string]$Themes,
  [string]$Provider,
  [int]$BatchSize,
  [string]$BatchCursorFile,
  [string]$OutRoot = ".\outputs\runs",
  [switch]$SkipInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

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
$VenvPath = Join-Path $Root ".venv"
$VenvPy   = Join-Path $VenvPath "Scripts\python.exe"

if (-not (Test-Path $VenvPy)) {
  Write-Host "[stage] creating venv"
  & $PySpec.exe @($PySpec.args) -m venv $VenvPath
}

if (-not $SkipInstall) {
  Write-Host "[stage] upgrading pip"
  & $VenvPy -m pip install --upgrade pip 1>$null
  Write-Host "[stage] installing requirements"
  & $VenvPy -m pip install -r ".\requirements.txt" 1>$null
  if (Test-Path ".\requirements-dev.txt") {
    Write-Host "[stage] installing dev requirements"
    & $VenvPy -m pip install -r ".\requirements-dev.txt" 1>$null
  }
}

Write-Host "[stage] running tests"
& $VenvPy -m pytest -q
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Tests failed. Fix issues and rerun .\run_all.ps1"
  exit $LASTEXITCODE
}

$RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$RunDir = Join-Path $OutRoot "run_$RunId"
if (-not (Test-Path $RunDir)) { New-Item -ItemType Directory -Path $RunDir | Out-Null }

Write-Host "[stage] running pipeline"
$args = @("-m","market_monitor","run","--config",$Config,"--mode",$Mode,"--outdir",$RunDir)
if ($Watchlist) { $args += @("--watchlist",$Watchlist) }
if ($Themes) { $args += @("--themes",$Themes) }
if ($Provider) { $args += @("--provider",$Provider) }
if ($BatchSize) { $args += @("--batch-size",$BatchSize) }
if ($BatchCursorFile) { $args += @("--batch-cursor-file",$BatchCursorFile) }

& $VenvPy @args
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Pipeline run failed. See logs in .\outputs\logs"
  exit $LASTEXITCODE
}

$Scored = Get-ChildItem -Path $RunDir -Filter "scored_*.csv" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
$SymbolCount = 0
if ($Scored) {
  $SymbolCount = (Import-Csv $Scored.FullName | Measure-Object).Count
}

Write-Host "[done] Outputs written to $RunDir"
Write-Host "[done] Symbols processed: $SymbolCount"
