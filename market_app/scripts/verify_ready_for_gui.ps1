param(
  [Parameter(Mandatory=$true)]
  [string]$Config,

  [string]$RunsDir = ".\runs_acceptance",

  [switch]$Offline
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($s) { Write-Host $s }
function Warn($s) { Write-Host $s -ForegroundColor Yellow }
function Fail($s) { Write-Host $s -ForegroundColor Red; exit 1 }
function Pass($s) { Write-Host $s -ForegroundColor Green }

# Always resolve paths relative to *current working directory* (the caller),
# not relative to some assumed repo root.
try {
  $ConfigPath = (Resolve-Path -LiteralPath $Config -ErrorAction Stop).Path
} catch {
  Fail "[fail] Config not found: $Config`n[error] Config missing."
}

# Runs dir: create if missing, then resolve to full path
New-Item -ItemType Directory -Force $RunsDir | Out-Null
$RunsDirPath = (Resolve-Path -LiteralPath $RunsDir).Path

# Pick python: prefer current venv if present in CWD
$Py = "python"
if (Test-Path ".\.venv\Scripts\python.exe") {
  $Py = ".\.venv\Scripts\python.exe"
}

Info "[step] CLI help"
& $Py -m market_app.cli --help | Out-Host
if ($LASTEXITCODE -ne 0) { Fail "[error] CLI help failed (exit $LASTEXITCODE)" }

Info "[step] Doctor"
$doctorArgs = @("-m","market_app.cli","doctor","--config",$ConfigPath)
if ($Offline) { $doctorArgs += "--offline" }
& $Py @doctorArgs | Out-Host
if ($LASTEXITCODE -ne 0) { Fail "[error] doctor failed (exit $LASTEXITCODE)" }

Info "[step] Determinism check"
$asOfDate = & $Py -c "import datetime, yaml, pathlib; cfg=yaml.safe_load(pathlib.Path(r'$ConfigPath').read_text(encoding='utf-8')) or {}; det=cfg.get('determinism', {}); value=det.get('as_of_date') or datetime.datetime.now(datetime.timezone.utc).date().isoformat(); print(value)"

$detArgs = @(
  "-m","market_app.cli","determinism-check",
  "--config",$ConfigPath,
  "--runs-dir",$RunsDirPath,
  "--as-of-date",$asOfDate,
  "--run-id","gui_ready_check"
)
if ($Offline) { $detArgs += "--offline" }
& $Py @detArgs | Out-Host
if ($LASTEXITCODE -ne 0) { Fail "[summary] FAIL" }

Pass "[summary] PASS"
