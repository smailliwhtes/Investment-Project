param(
  [string]$Config = ".\config.yaml"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

$VenvPy = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

& $VenvPy -m market_monitor run --config $Config --mode universe
