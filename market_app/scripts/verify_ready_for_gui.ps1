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

Info "[step] Run #1"
$runId1 = "gui_ready_check_1"
$runArgs1 = @("-m","market_app.cli","run","--config",$ConfigPath,"--runs-dir",$RunsDirPath,"--run-id",$runId1)
if ($Offline) { $runArgs1 += "--offline" }
& $Py @runArgs1 | Out-Host
if ($LASTEXITCODE -ne 0) { Fail "[error] run #1 failed (exit $LASTEXITCODE)" }

Info "[step] Run #2"
$runId2 = "gui_ready_check_2"
$runArgs2 = @("-m","market_app.cli","run","--config",$ConfigPath,"--runs-dir",$RunsDirPath,"--run-id",$runId2)
if ($Offline) { $runArgs2 += "--offline" }
& $Py @runArgs2 | Out-Host
if ($LASTEXITCODE -ne 0) { Fail "[error] run #2 failed (exit $LASTEXITCODE)" }

$runDir1 = Join-Path $RunsDirPath $runId1
$runDir2 = Join-Path $RunsDirPath $runId2

foreach ($d in @($runDir1,$runDir2)) {
  if (-not (Test-Path $d)) { Fail "[error] missing run dir: $d" }
  foreach ($f in @("manifest.json","digest.json")) {
    $p = Join-Path $d $f
    if (-not (Test-Path $p)) { Fail "[error] missing $f in $d" }
  }

  $report = Get-ChildItem $d -File -Filter "report.*" -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $report) { Warn "[warn] no report.* found in $d" }

  $log = Get-ChildItem $d -Recurse -File -Filter "run.log" -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $log) { Warn "[warn] no run.log found under $d" }
}

Info "[step] Determinism: compare digest.json SHA256"
$h1 = (Get-FileHash (Join-Path $runDir1 "digest.json") -Algorithm SHA256).Hash
$h2 = (Get-FileHash (Join-Path $runDir2 "digest.json") -Algorithm SHA256).Hash

if ($h1 -ne $h2) {
  Warn "[warn] digest mismatch:"
  Warn "  $runDir1 -> $h1"
  Warn "  $runDir2 -> $h2"
  Fail "[summary] FAIL"
}

Pass "[summary] PASS"
Pass "Runs:"
Info "  $runDir1"
Info "  $runDir2"
