param(
  [string]$Config = ".\config\config.yaml",
  [string]$RunId = "",
  [int]$TopN = 15,
  [switch]$Offline,
  [ValidateSet("conservative","opportunistic")]
  [string]$Variant = "conservative",
  [string]$WatchlistPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

& (Join-Path $Root "scripts\run.ps1") -Config $Config -RunId $RunId -TopN $TopN -Offline:$Offline -Variant $Variant -WatchlistPath $WatchlistPath
