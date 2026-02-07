param(
  [string]$Config = "",
  [switch]$Offline = $true,
  [string]$RunsDir = "runs",
  [switch]$OpenReport,
  [switch]$Verbose
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot ".."))
Set-Location $Root

if (-not $Config) {
  $DefaultConfig = Join-Path $Root "config.yaml"
  if (Test-Path $DefaultConfig) {
    $Config = $DefaultConfig
  } else {
    $Config = Read-Host "Enter path to config.yaml"
  }
}

$ConfigPath = (Resolve-Path $Config)

$VenvPy = Join-Path $Root ".venv\Scripts\python.exe"
if (Test-Path $VenvPy) {
  $Python = $VenvPy
} else {
  $Python = "python"
  Write-Host "[warn] .venv not found. Using python from PATH. Consider running .\scripts\bootstrap.ps1"
}

Write-Host "[info] python version"
& $Python --version

$DoctorCmd = @(
  $Python,
  "-m",
  "market_app.cli",
  "doctor",
  "--config",
  $ConfigPath
)
if ($Offline) {
  $DoctorCmd += "--offline"
}

if ($Verbose) { Write-Host "[cmd] $($DoctorCmd -join ' ')" }
& $DoctorCmd
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Doctor reported blocking issues. Fix them and rerun."
  exit $LASTEXITCODE
}

$RunCmd = @(
  $Python,
  "-m",
  "market_app.cli",
  "run",
  "--config",
  $ConfigPath,
  "--runs-dir",
  $RunsDir
)
if ($Offline) {
  $RunCmd += "--offline"
}

if ($Verbose) { Write-Host "[cmd] $($RunCmd -join ' ')" }
& $RunCmd
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Run failed. Check logs in the run directory."
  exit $LASTEXITCODE
}

$RunsPath = (Resolve-Path (Join-Path $Root $RunsDir))
$LatestRun = Get-ChildItem -Path $RunsPath -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $LatestRun) {
  Write-Host "[warn] Unable to locate the latest run directory under $RunsPath"
  exit 1
}

Write-Host "[done] Run artifacts: $($LatestRun.FullName)"

if ($OpenReport) {
  $ReportMd = Join-Path $LatestRun.FullName "report.md"
  $ReportHtml = Join-Path $LatestRun.FullName "report.html"
  if (Test-Path $ReportMd) {
    Start-Process $ReportMd | Out-Null
  } elseif (Test-Path $ReportHtml) {
    Start-Process $ReportHtml | Out-Null
  } else {
    Write-Host "[warn] No report.md or report.html found in $($LatestRun.FullName)"
  }
}
