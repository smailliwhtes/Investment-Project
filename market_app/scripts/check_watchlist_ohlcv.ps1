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

if ($DataDir) {
  $env:NASDAQ_DAILY_DIR = (Resolve-Path $DataDir).Path
}

$ArgsList = @("-m", "market_monitor.tools.check_watchlist_ohlcv", "--config", $Config)
if ($WatchlistPath) { $ArgsList += @("--watchlist", $WatchlistPath) }

& $VenvPy @ArgsList
