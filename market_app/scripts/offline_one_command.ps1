param(
  [string]$Config = ".\tests\fixtures\minimal_config.yaml",
  [string]$StooqRoot = ".\tests\fixtures\stooq_txt",
  [string]$RunOut = ".\outputs\offline_demo"
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

Write-Host "[stage] installing requirements"
& $VenvPy -m pip install --upgrade pip 1>$null
& $VenvPy -m pip install -r ".\requirements.txt" | Out-Null

Write-Host "[stage] building security_master.csv from fixtures"
& $VenvPy .\tools\build_security_master.py `
  --stooq-root $StooqRoot `
  --output .\out\security_master.csv `
  --filter-required `
  --path-mode relative

Write-Host "[stage] running offline monitor pass"
& $VenvPy -m market_monitor run --config $Config --mode watchlist --outdir $RunOut

Write-Host "[done] offline run complete -> $RunOut"
