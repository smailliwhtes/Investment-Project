param(
  [string]$Config = ".\config.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

# Folders
$dirs = @(".\data", ".\data\universe", ".\data\stooq", ".\data\state", ".\outputs", ".\inputs", ".\scripts")
foreach ($d in $dirs) { if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d | Out-Null } }

# Python venv
$Py = "python"
if (-not (Get-Command $Py -ErrorAction SilentlyContinue)) {
  throw "python not found on PATH. Install Python 3.11+ and reopen PowerShell."
}

$VenvPath = Join-Path $Root ".venv"
$VenvPy   = Join-Path $VenvPath "Scripts\python.exe"
$VenvPip  = Join-Path $VenvPath "Scripts\pip.exe"

if (-not (Test-Path $VenvPy)) {
  & $Py -m venv $VenvPath
}

# Dependencies
& $VenvPip install --upgrade pip | Out-Null
& $VenvPip install -r ".\requirements.txt" | Out-Null

# Run pipeline
& $VenvPy ".\scripts\runner.py" --config $Config
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Outputs:"
Write-Host "  .\outputs\features_.csv"
Write-Host "  .\outputs\scored_.csv"
Write-Host "  .\outputs\eligible.csv"
Write-Host "  .\outputs\report.md"
