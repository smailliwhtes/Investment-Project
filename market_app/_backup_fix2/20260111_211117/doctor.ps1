param(
  [string]$Config = ".\config.json"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Ensure-Dir([string]$p) { if (-not (Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null } }

# Folders
$dirs = @(".\data", ".\data\universe", ".\data\stooq", ".\data\state", ".\outputs", ".\inputs", ".\scripts")
foreach ($d in $dirs) { Ensure-Dir $d }

function Get-PythonCommand {
  # Prefer the Windows Python Launcher if present
  $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
  if ($pyLauncher) {
    foreach ($v in @("3.12","3.11","3.10")) {
      try {
        & py "-$v" -c "import sys; print(sys.version_info[:2])" | Out-Null
        if ($LASTEXITCODE -eq 0) { return @("py","-$v") }
      } catch { }
    }
  }

  # Fallback: plain python on PATH
  $py = Get-Command python -ErrorAction SilentlyContinue
  if ($py) { return @("python") }

  throw "No Python found. Install Python 3.12 (recommended) or 3.11 and reopen PowerShell."
}

function Get-PyMajorMinor($cmdParts) {
  $out = & $cmdParts[0] @($cmdParts[1..($cmdParts.Length-1)]) -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
  if ($LASTEXITCODE -ne 0) { throw "Failed to query Python version." }
  return $out.Trim()
}

$PyCmd = Get-PythonCommand
$ver = Get-PyMajorMinor $PyCmd

# Block Python >= 3.13 (wheels may not exist -> builds from source -> failure)
$major = [int]($ver.Split('.')[0]); $minor = [int]($ver.Split('.')[1])
if ($major -ne 3 -or $minor -ge 13) {
  throw "Detected Python $ver. This project requires Python 3.12 or 3.11 so pip can install binary wheels (no Visual Studio build). Install Python 3.12, then re-run."
}

$VenvPath = Join-Path $Root ".venv"
$VenvPy   = Join-Path $VenvPath "Scripts\python.exe"
$VenvPip  = Join-Path $VenvPath "Scripts\pip.exe"

# If an existing venv was created with a bad version, delete it
$cfgPath = Join-Path $VenvPath "pyvenv.cfg"
if (Test-Path $cfgPath) {
  $cfg = Get-Content $cfgPath -Raw
  if ($cfg -match 'version\s*=\s*3\.(\d+)') {
    $venvMinor = [int]$Matches[1]
    if ($venvMinor -ge 13) {
      Remove-Item -Recurse -Force $VenvPath
    }
  }
}

if (-not (Test-Path $VenvPy)) {
  & $PyCmd[0] @($PyCmd[1..($PyCmd.Length-1)]) -m venv $VenvPath
}

# Dependencies (binary wheels only)
& $VenvPip install --upgrade pip | Out-Null
& $VenvPip install --only-binary=:all: -r ".\requirements.txt"

# Run pipeline
& $VenvPy ".\scripts\runner.py" --config $Config
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Outputs:"
Write-Host "  .\outputs\features_.csv"
Write-Host "  .\outputs\scored_.csv"
Write-Host "  .\outputs\eligible.csv"
Write-Host "  .\outputs\report.md"