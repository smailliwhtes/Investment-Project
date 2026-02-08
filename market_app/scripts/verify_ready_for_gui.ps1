param(
  [Parameter(Mandatory=$true)]
  [string]$Config,

  [string]$RunsDir = ".\runs_acceptance",

  [switch]$Offline
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($s) { Write-Host $s }
function Fail($s) { Write-Host $s -ForegroundColor Red; exit 1 }
function Pass($s) { Write-Host $s -ForegroundColor Green }

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptRoot "..\")

function Resolve-RepoPath([string]$Candidate) {
  if ([System.IO.Path]::IsPathRooted($Candidate)) {
    return (Resolve-Path -LiteralPath $Candidate -ErrorAction Stop).Path
  }

  foreach ($root in @($RepoRoot, $ScriptRoot)) {
    $tryPath = Join-Path $root $Candidate
    if (Test-Path -LiteralPath $tryPath) {
      return (Resolve-Path -LiteralPath $tryPath -ErrorAction Stop).Path
    }
  }

  throw "Path not found: $Candidate"
}

try {
  $ConfigPath = Resolve-RepoPath $Config
} catch {
  Fail "[fail] Config not found: $Config`n[error] Config missing."
}

# Runs dir: create if missing, then resolve to full path
$RunsDirPath = $RunsDir
if (-not [System.IO.Path]::IsPathRooted($RunsDir)) {
  $RunsDirPath = Join-Path $RepoRoot $RunsDir
}
New-Item -ItemType Directory -Force $RunsDirPath | Out-Null
$RunsDirPath = (Resolve-Path -LiteralPath $RunsDirPath).Path

# Pick python: prefer current venv if present in CWD
$Py = "python"
if (Test-Path ".\.venv\Scripts\python.exe") {
  $Py = ".\.venv\Scripts\python.exe"
}

Info "[step] Determinism check"
$detArgs = @(
  "-m","market_app.cli","determinism-check",
  "--config",$ConfigPath,
  "--runs-dir",$RunsDirPath
)
if ($Offline) { $detArgs += "--offline" }
& $Py @detArgs | Out-Host
if ($LASTEXITCODE -ne 0) { Fail "[summary] FAIL" }

Pass "[summary] PASS"
