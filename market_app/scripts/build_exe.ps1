param(
  [string]$OutDir = ".\\dist"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\\..")

$VenvPy = Join-Path $Root "..\\.venv\\Scripts\\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

& $VenvPy -m pip show pyinstaller > $null 2>&1
if ($LASTEXITCODE -ne 0) {
  Write-Host "[build] installing pyinstaller..."
  & $VenvPy -m pip install pyinstaller==6.8.0
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to install pyinstaller."
  }
}

Write-Host "[build] creating executable..."
& $VenvPy -m PyInstaller --name market-monitor --onefile -m market_monitor --distpath $OutDir --clean
if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller build failed with exit code $LASTEXITCODE."
}

Write-Host "[done] Build output: $(Resolve-Path $OutDir)\\market-monitor.exe"
