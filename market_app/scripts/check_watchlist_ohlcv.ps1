param(
  [string]$Config = ".\config.example.yaml",
  [string]$WatchlistPath = "",
  [string]$DataDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

$VenvPy = Join-Path $Root "..\.venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

try {
  if ($DataDir) {
    $resolvedDataDir = (Resolve-Path $DataDir).Path
    $env:MARKET_APP_OHLCV_DIR = $resolvedDataDir
    $env:NASDAQ_DAILY_DIR = $resolvedDataDir
  }

  $ArgsList = @("-m", "market_monitor.tools.check_watchlist_ohlcv", "--config", $Config)
  if ($WatchlistPath) { $ArgsList += @("--watchlist", $WatchlistPath) }

  & $VenvPy @ArgsList
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
  exit 0
} catch {
  Write-Host "[error] $($_.Exception.Message)"
  exit 1
}
