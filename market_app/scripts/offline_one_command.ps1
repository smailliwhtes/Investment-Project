param(
  [string]$Config = ".\tests\fixtures\minimal_config.yaml",
  [string]$StooqRoot = ".\tests\fixtures\stooq_txt",
  [string]$MetastockRoot,
  [string]$CorpusRoot,
  [string]$RunOut = ".\outputs\offline_demo",
  [string]$PackRoot,
  [string]$MetadataCache = "out\metadata_cache",
  [switch]$Provision,
  [switch]$IncludeSubmissionsZip,
  [switch]$FilterRequired
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptRoot = $PSScriptRoot
if (-not $ScriptRoot) {
  $ScriptRoot = (Get-Location).Path
}
if (-not $PackRoot) {
  $PackRoot = (Resolve-Path (Join-Path $ScriptRoot "..")).Path
}
Set-Location $PackRoot

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
$VenvPath = Join-Path $PackRoot ".venv"
$VenvPy   = Join-Path $VenvPath "Scripts\python.exe"

if (-not (Test-Path $VenvPy)) {
  Write-Host "[stage] creating venv"
  & $PySpec.exe @($PySpec.args) -m venv $VenvPath
}

Write-Host "[stage] installing requirements"
& $VenvPy -m pip install --upgrade pip 1>$null
& $VenvPy -m pip install -r ".\requirements.txt" | Out-Null

Write-Host "[stage] inventorying offline assets"
& $VenvPy -m market_monitor.tools.inventory `
  --stooq-root $StooqRoot `
  --metastock-root $MetastockRoot `
  --corpus-root $CorpusRoot `
  --write-out .\out

Write-Host "[stage] building security_master.csv from Stooq TXT"
$filterArg = @()
if ($FilterRequired) { $filterArg = @("--filter-required") }
& $VenvPy .\tools\build_security_master.py `
  --stooq-root $StooqRoot `
  --output .\out\security_master.csv `
  --path-mode relative `
  @filterArg

if ($Provision) {
  Write-Host "[stage] provisioning metadata snapshots"
  $provArgs = @(
    "-PackRoot", $PackRoot,
    "-OutDir", $MetadataCache
  )
  if ($IncludeSubmissionsZip) { $provArgs += "-IncludeSubmissionsZip" }
  & $PackRoot\scripts\provision_metadata.ps1 @provArgs
}

Write-Host "[stage] enriching security_master.csv"
& $VenvPy .\tools\enrich_security_master.py `
  --input .\out\security_master.csv `
  --output .\out\security_master.csv `
  --metadata-cache $MetadataCache

Write-Host "[stage] running unit tests"
& $VenvPy -m pytest

Write-Host "[stage] running offline monitor pass"
& $VenvPy -m market_monitor run --config $Config --mode watchlist --outdir $RunOut

Write-Host "[done] offline run complete -> $RunOut"
