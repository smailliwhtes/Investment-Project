param(
  [string]$Config = ".\config\config.yaml",
  [string]$RunId = "",
  [switch]$Offline,
  [switch]$Online,
  [string]$SymbolsDir = "",
  [string]$OhlcvDir = "",
  [string]$OutputDir = "",
  [int]$TopN = 50
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

$VenvDir = Join-Path $RepoRoot ".venv"
$VenvPy = Join-Path $VenvDir "Scripts\python.exe"

try {
  if (-not (Test-Path $VenvPy)) {
    Write-Host "[setup] Creating virtual environment..."
    python -m venv $VenvDir
  }

  if (-not (Test-Path $VenvPy)) {
    throw "Unable to locate Python executable in virtual environment: $VenvPy"
  }

  Write-Host "[setup] Installing dependencies..."
  & $VenvPy -m pip install --upgrade pip
  & $VenvPy -m pip install -r (Join-Path $RepoRoot "requirements.txt")
  if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

  $ResolvedConfig = (Resolve-Path $Config).Path
  $args = @("--config", $ResolvedConfig, "--top-n", $TopN)

  if ($RunId) { $args += @("--run-id", $RunId) }
  if ($Offline) { $args += "--offline" }
  if ($Online) { $args += "--online" }
  if ($SymbolsDir) { $args += @("--symbols-dir", $SymbolsDir) }
  if ($OhlcvDir) { $args += @("--ohlcv-dir", $OhlcvDir) }
  if ($OutputDir) { $args += @("--output-dir", $OutputDir) }

  Write-Host "[run] python -m market_app.cli $($args -join ' ')"
  & $VenvPy -m market_app.cli @args
  if ($LASTEXITCODE -ne 0) {
    throw "Pipeline failed with exit code $LASTEXITCODE."
  }

  Write-Host "[done] Pipeline completed"
  exit 0
} catch {
  Write-Host "[error] $($_.Exception.Message)"
  exit 1
}
