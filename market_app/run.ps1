param(
  [string]$Config = ".\config.json",
  [ValidateSet("universe","watchlist","themed","batch")]
  [string]$Mode = "watchlist",
  [string]$Watchlist,
  [string]$Themes,
  [string]$Provider
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

$VenvPy = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

$args = @("-m","market_monitor","run","--config",$Config,"--mode",$Mode)
if ($Watchlist) { $args += @("--watchlist",$Watchlist) }
if ($Themes) { $args += @("--themes",$Themes) }
if ($Provider) { $args += @("--provider",$Provider) }

& $VenvPy @args
