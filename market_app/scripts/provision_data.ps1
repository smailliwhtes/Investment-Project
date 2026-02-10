param(
  [string]$Config = ".\config\config.yaml"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

Write-Host "[info] Provisioning script placeholder (offline-first)."
Write-Host "[info] Populate local symbol listings in MARKET_APP_SYMBOLS_DIR."
Write-Host "[info] Populate OHLCV CSVs in MARKET_APP_OHLCV_DIR."
Write-Host "[info] This script would download and normalize listings + OHLCV when online."
Write-Host "[done] No provisioning performed."
