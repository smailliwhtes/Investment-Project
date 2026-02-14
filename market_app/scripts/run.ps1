param(
  [string]$Config = ".\config\config.yaml",
  [string]$RunId = "",
  [switch]$Offline,
  [string]$SymbolsDir = "",
  [string]$OhlcvDir = "",
  [string]$OutputDir = "",
  [string]$AsOf = "",
  [int]$TopN = 200
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $RepoRoot

$VenvPy = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) {
  python -m venv (Join-Path $RepoRoot ".venv")
}

$args = @("-m", "market_app.cli", "--config", (Resolve-Path $Config).Path, "--top-n", $TopN)
if ($Offline -or -not $PSBoundParameters.ContainsKey('Offline')) { $args += "--offline" }
if ($RunId) { $args += @("--run-id", $RunId) }
if ($SymbolsDir) { $args += @("--symbols-dir", $SymbolsDir) }
if ($OhlcvDir) { $args += @("--ohlcv-dir", $OhlcvDir) }
if ($OutputDir) { $args += @("--output-dir", $OutputDir) }
if ($AsOf) { $args += @("--as-of", $AsOf) }

Write-Host "[run] python $($args -join ' ')"
& $VenvPy @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
exit 0
