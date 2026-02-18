$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Write-Err($msg)  { Write-Host "[error] $msg" -ForegroundColor Red }

function Resolve-DesktopPath {
  $desktop = [Environment]::GetFolderPath("Desktop")
  if ($desktop -and (Test-Path $desktop)) { return $desktop }

  $fallbacks = @(
    (Join-Path $env:USERPROFILE "Desktop"),
    (Join-Path $env:OneDrive "Desktop")
  )
  foreach ($candidate in $fallbacks) {
    if ($candidate -and (Test-Path $candidate)) { return $candidate }
  }

  if ($fallbacks[0]) {
    New-Item -ItemType Directory -Path $fallbacks[0] -Force | Out-Null
    return $fallbacks[0]
  }
  throw "Could not resolve a Desktop folder path for the current user."
}

# Resolve repo root from this installer script location
$scriptDir = $PSScriptRoot
if (-not $scriptDir) {
  $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
}
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).ProviderPath

$runScript = Join-Path $repoRoot "scripts\run_gui.ps1"
if (-not (Test-Path $runScript)) {
  Write-Err "Missing scripts\run_gui.ps1. Create it first."
  exit 2
}

# Choose pwsh if available, else Windows PowerShell
$pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
$targetExe = if ($pwsh) {
  $pwsh.Source
} else {
  Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
}

if (-not (Test-Path $targetExe)) {
  Write-Err "No PowerShell executable found. Expected: $targetExe"
  exit 127
}

$desktop = Resolve-DesktopPath
$lnkPath = Join-Path $desktop "MarketApp GUI.lnk"

# Use the helper run script so builds/restores happen automatically.
$arguments = "-NoExit -ExecutionPolicy Bypass -File `"$runScript`""

$wsh = New-Object -ComObject WScript.Shell
$sc  = $wsh.CreateShortcut($lnkPath)
$sc.TargetPath = $targetExe
$sc.Arguments  = $arguments
$sc.WorkingDirectory = $repoRoot
# Optional: PowerShell icon
$sc.IconLocation = "$targetExe,0"
$sc.Save()

if (-not (Test-Path $lnkPath)) {
  Write-Err "Shortcut creation failed. Expected file not found: $lnkPath"
  exit 4
}

$saved = $wsh.CreateShortcut($lnkPath)
Write-Host "Created Desktop shortcut: $lnkPath"
Write-Host "TargetPath: $($saved.TargetPath)"
Write-Host "Arguments: $($saved.Arguments)"
Write-Host "WorkingDirectory: $($saved.WorkingDirectory)"
