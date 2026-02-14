$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[info] $msg" -ForegroundColor Cyan }
function Write-Err($msg)  { Write-Host "[error] $msg" -ForegroundColor Red }

# Resolve repo root from this installer script location
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Resolve-Path (Join-Path $scriptDir "..")

$runScript = Join-Path $repoRoot "scripts\run_e2e.ps1"
if (-not (Test-Path $runScript)) {
  Write-Err "Missing scripts\run_e2e.ps1. Create it first."
  exit 2
}

# Choose pwsh if available, else Windows PowerShell
$targetExe = $null
$pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
if ($pwsh) {
  $targetExe = $pwsh.Source
} else {
  $ps = Get-Command powershell -ErrorAction SilentlyContinue
  if ($ps) { $targetExe = $ps.Source }
}

if (-not $targetExe) {
  Write-Err "Neither pwsh nor powershell was found on PATH."
  exit 127
}

$desktop = [Environment]::GetFolderPath("Desktop")
$lnkPath = Join-Path $desktop "market_app - Offline E2E Test.lnk"

# Build arguments: run E2E and pause on failure so errors remain visible
# NOTE: Use -ExecutionPolicy Bypass for convenience on local machines.
$arg = "-NoProfile -ExecutionPolicy Bypass -File `"$runScript`" -PauseOnFail"

$wsh = New-Object -ComObject WScript.Shell
$sc  = $wsh.CreateShortcut($lnkPath)
$sc.TargetPath = $targetExe
$sc.Arguments  = $arg
$sc.WorkingDirectory = $repoRoot.Path
# Optional: PowerShell icon
$sc.IconLocation = "$targetExe,0"
$sc.Save()

Write-Info "Created Desktop shortcut:"
Write-Host "  $lnkPath"
Write-Info "Double-click it to run the offline E2E test in a PowerShell terminal."
