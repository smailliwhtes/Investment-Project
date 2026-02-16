$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[info] $msg" -ForegroundColor Cyan }
function Write-Err($msg)  { Write-Host "[error] $msg" -ForegroundColor Red }

# Resolve repo root from this installer script location
$scriptDir = $PSScriptRoot
if (-not $scriptDir) {
  $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
$repoRoot  = Resolve-Path (Join-Path $scriptDir "..")

$runScript = Join-Path $repoRoot "scripts\run_gui.ps1"
if (-not (Test-Path $runScript)) {
  Write-Err "Missing scripts\run_gui.ps1. Create it first."
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
$lnkPath = Join-Path $desktop "MarketApp GUI.lnk"

# Use the helper run script so builds/restores happen automatically.
$arg = "-NoProfile -ExecutionPolicy Bypass -File `"$runScript`""

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
Write-Info "Double-click it to launch the MarketApp GUI (builds if needed)."
