param(
  [Parameter(Mandatory=$true)][string]$DailyDir,
  [string]$ConfigPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
  $ConfigPath = Join-Path $RepoRoot "config.yaml"
}

if (-not (Test-Path $DailyDir)) {
  throw "Daily data directory not found at $DailyDir"
}

if (-not (Test-Path $ConfigPath)) {
  throw "Config file not found at $ConfigPath"
}

$DailyDir = [System.IO.Path]::GetFullPath($DailyDir)
$ConfigPath = [System.IO.Path]::GetFullPath($ConfigPath)

function Invoke-Checked {
  param([string[]]$Command)
  & $Command
  if ($LASTEXITCODE -ne 0) {
    throw "Command failed: $($Command -join ' ')"
  }
}

try {
  $VenvPath = Join-Path $RepoRoot ".venv"
  $VenvPy = Join-Path $VenvPath "Scripts\python.exe"

  if (-not (Test-Path $VenvPy)) {
    $BasePy = Get-Command python -ErrorAction SilentlyContinue
    if (-not $BasePy) {
      throw "Python not found. Install Python 3.11+ and reopen PowerShell."
    }
    Write-Host "[stage] creating venv"
    Invoke-Checked @($BasePy.Source, "-m", "venv", $VenvPath)
    if (-not (Test-Path $VenvPy)) {
      throw "Virtual environment python not found at $VenvPy"
    }
  }

  Write-Host "[stage] upgrading pip"
  Invoke-Checked @($VenvPy, "-m", "pip", "install", "--upgrade", "pip")
  Write-Host "[stage] installing requirements"
  Invoke-Checked @($VenvPy, "-m", "pip", "install", "-r", "requirements.txt")

  Write-Host "[stage] running tests"
  Invoke-Checked @($VenvPy, "-m", "pytest", "-q")

  Write-Host "[stage] running offline acceptance"
  $AcceptanceScript = Join-Path $RepoRoot "scripts\acceptance_offline_spy.ps1"
  & $AcceptanceScript -DailyDir $DailyDir -ConfigPath $ConfigPath
  if ($LASTEXITCODE -ne 0) {
    throw "Offline acceptance failed."
  }

  Write-Host "[done] verify_local completed successfully."
  exit 0
} catch {
  Write-Host "[error] $($_.Exception.Message)"
  exit 1
}
