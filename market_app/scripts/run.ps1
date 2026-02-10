param(
  [string]$Config = ".\config\config.yaml",
  [string]$RunId = "",
  [switch]$Offline,
  [switch]$Online,
  [string]$SymbolsDir = "",
  [string]$OhlcvDir = "",
  [string]$OutputDir = "",
  [int]$TopN = 50
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

$VenvPy = Join-Path $Root "..\.venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

try {
  $ResolvedConfig = (Resolve-Path $Config).Path
  $args = @(
    "--config",$ResolvedConfig,
    "--top-n",$TopN
  )
  if ($RunId) { $args += @("--run-id",$RunId) }
  if ($Offline) { $args += "--offline" }
  if ($Online) { $args += "--online" }
  if ($SymbolsDir) { $args += @("--symbols-dir",$SymbolsDir) }
  if ($OhlcvDir) { $args += @("--ohlcv-dir",$OhlcvDir) }
  if ($OutputDir) { $args += @("--output-dir",$OutputDir) }

  & $VenvPy -m market_app.cli @args
  if ($LASTEXITCODE -ne 0) {
    throw "Pipeline failed with exit code $LASTEXITCODE."
  }
  Write-Host "[done] Pipeline completed"
  exit 0
} catch {
  Write-Host "[error] $($_.Exception.Message)"
  exit 1
}
