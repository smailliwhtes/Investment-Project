param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptRoot "..")
Set-Location $RepoRoot

try {
  function Invoke-Checked {
    param([string[]]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
      throw "Command failed: $($Command -join ' ')"
    }
  }

  $VenvPath = Join-Path $RepoRoot ".venv"
  Invoke-Checked @("python", "-m", "venv", $VenvPath)

  $VenvPy = Join-Path $VenvPath "Scripts\python.exe"
  if (-not (Test-Path $VenvPy)) {
    throw "Virtual environment python not found at $VenvPy"
  }

  Invoke-Checked @($VenvPy, "-m", "pip", "install", "-U", "pip", "setuptools", "wheel")
  Invoke-Checked @($VenvPy, "-m", "pip", "install", "-e", ".[test]", "--no-build-isolation")

  Invoke-Checked @($VenvPy, "-m", "pytest", "-q")

  Write-Host "[done] bootstrap completed successfully."
  exit 0
} catch {
  Write-Host "[error] $($_.Exception.Message)"
  exit 1
}
