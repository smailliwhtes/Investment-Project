param(
  [string]$Config = ".\config\config.yaml",
  [string]$WatchlistPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

$VenvPy = Join-Path $Root "..\.venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

$ArgsList = @("-m", "market_monitor.tools.check_watchlist_ohlcv", "--config", $Config)
if ($WatchlistPath) { $ArgsList += @("--watchlist", $WatchlistPath) }

& $VenvPy @ArgsList
