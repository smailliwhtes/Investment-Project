param(
  [Parameter(Mandatory=$true)][string]$StooqDumpDir,
  [Parameter(Mandatory=$true)][string]$OhlcvDir,
  [string]$UniverseIn = ".\data\universe\universe.csv",
  [string]$UniverseOutDir = ".\data\universe_available",
  [string]$Config = ".\config\config.yaml",
  [string]$RunId = "",
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

$python = "python"
& $python --version | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Python is required on PATH." }

$runIdVal = if ($RunId) { $RunId } else { "offline_$(Get-Date -Format 'yyyyMMdd_HHmmss')" }

$updateArgs = @("tools/stooq_incremental_updater.py", "--existing-csv-dir", $OhlcvDir, "--new-stooq-dir", $StooqDumpDir, "--out-csv-dir", $OhlcvDir, "--recursive")
if ($DryRun) { $updateArgs += "--dry-run" }

Write-Host "[step] Incremental update"
& $python @updateArgs
if ($LASTEXITCODE -ne 0) { throw "stooq_incremental_updater failed" }

Write-Host "[step] Build universe_available"
& $python tools/build_universe_available.py --ohlcv-dir $OhlcvDir --universe-in $UniverseIn --out-dir $UniverseOutDir
if ($LASTEXITCODE -ne 0) { throw "build_universe_available failed" }

if (-not $DryRun) {
  Write-Host "[step] Run offline pipeline"
  & $python -m market_app.cli --config $Config --offline --run-id $runIdVal --ohlcv-dir $OhlcvDir --symbols-dir $UniverseOutDir
  if ($LASTEXITCODE -ne 0) { throw "offline pipeline failed" }
}

Write-Host "[done] update_and_run complete"
exit 0
