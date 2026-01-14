param(
  [string]$Config = ".\config.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Ensure-Dir([string]$p) { if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null } }

$dirs = @(".\data", ".\data\universe", ".\data\cache", ".\data\state", ".\outputs", ".\inputs")
foreach ($d in $dirs) { Ensure-Dir $d }

function Get-PythonSpec {
  $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
  if ($pyLauncher) {
    try { & py -3.11 -c "import sys" | Out-Null; if ($LASTEXITCODE -eq 0) { return @{ exe="py"; args=@("-3.11") } } } catch {}
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

Write-Host "[stage] upgrading pip"
& $VenvPy -m pip install --upgrade pip

Write-Host "[stage] installing requirements"
& $VenvPy -m pip install -r ".\requirements.txt"

Write-Host "[stage] running doctor"
& $VenvPy -m market_monitor doctor --config $Config

Write-Host "[stage] running pipeline"
& $VenvPy -m market_monitor run --config $Config --mode watchlist

Write-Host "Outputs are in .\outputs"
