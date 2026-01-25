param(
  [string]$Config = ".\config\config.yaml",
  [string]$RunId = "",
  [int]$TopN = 15,
  [switch]$Offline,
  [ValidateSet("conservative","opportunistic")]
  [string]$Variant = "conservative"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

$VenvPy = Join-Path $Root "..\.venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

$args = @("-m","market_app.cli","--config",$Config,"--top_n",$TopN)
if ($RunId) { $args += @("--run_id",$RunId) }
if ($Offline) { $args += "--offline" }
if ($Variant -eq "opportunistic") { $args += "--opportunistic" }
else { $args += "--conservative" }

& $VenvPy @args
