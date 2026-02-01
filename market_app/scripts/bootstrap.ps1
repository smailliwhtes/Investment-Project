param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

try {
  function Resolve-PythonCommand {
    if (Get-Command py -ErrorAction SilentlyContinue) {
      & py -3.13 -c "import sys; print(sys.executable)" 2>$null
      if ($LASTEXITCODE -eq 0) { return @("py", "-3.13") }
      & py -3.12 -c "import sys; print(sys.executable)" 2>$null
      if ($LASTEXITCODE -eq 0) { return @("py", "-3.12") }
    }
    throw "Python 3.13/3.12 not found. Install Python 3.13 or 3.12 and ensure the Windows py launcher is available."
  }

  $PyCommand = Resolve-PythonCommand
  $VenvPath = Join-Path (Get-Location) ".venv"
  & $PyCommand -m venv $VenvPath

  $VenvPy = Join-Path $VenvPath "Scripts\python.exe"
  if (-not (Test-Path $VenvPy)) {
    throw "Virtual environment python not found at $VenvPy"
  }

  & $VenvPy -m pip install --upgrade pip
  & $VenvPy -m pip install --only-binary=:all: -e ".[dev]"

  & $VenvPy -m market_monitor.env_doctor --self-test
  if ($LASTEXITCODE -ne 0) {
    throw "Environment doctor self-test failed with exit code $LASTEXITCODE."
  }

  & $VenvPy -m pytest -q
  if ($LASTEXITCODE -ne 0) {
    throw "pytest failed with exit code $LASTEXITCODE."
  }

  Write-Host "[done] bootstrap completed successfully."
  exit 0
} catch {
  Write-Host "[error] $($_.Exception.Message)"
  exit 1
}
