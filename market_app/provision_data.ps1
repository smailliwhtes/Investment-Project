param(
  [string]$Config = ".\config\config.yaml",
  [string]$WatchlistPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

& (Join-Path $Root "scripts\provision_data.ps1") -Config $Config -WatchlistPath $WatchlistPath
