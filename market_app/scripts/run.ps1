param(
  [string]$Config = ".\config.example.yaml",
  [string]$RunId = "",
  [int]$TopN = 15,
  [switch]$Offline,
  [ValidateSet("conservative","opportunistic")]
  [string]$Variant = "conservative",
  [string]$WatchlistPath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

$VenvPy = Join-Path $Root "..\.venv\Scripts\python.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }

function Resolve-WatchlistPath {
  param(
    [string]$ConfigPath,
    [string]$OverridePath
  )

  if ($OverridePath) {
    $candidate = $OverridePath
  } else {
    $candidate = & $VenvPy -c "import yaml,sys; from pathlib import Path; data=yaml.safe_load(Path(sys.argv[1]).read_text()) or {}; print(data.get('watchlist_path') or data.get('paths', {}).get('watchlist_file') or 'watchlists/watchlist_core.csv')" $ConfigPath
  }

  $candidate = $candidate.Trim()
  if (-not [System.IO.Path]::IsPathRooted($candidate)) {
    $candidate = (Resolve-Path (Join-Path (Split-Path -Parent $ConfigPath) $candidate)).Path
  } else {
    $candidate = (Resolve-Path $candidate).Path
  }
  return $candidate
}

$ResolvedConfig = (Resolve-Path $Config).Path
$ResolvedWatchlist = Resolve-WatchlistPath -ConfigPath $ResolvedConfig -OverridePath $WatchlistPath
if (-not (Test-Path $ResolvedWatchlist)) {
  throw "Watchlist file not found: $ResolvedWatchlist"
}

if (-not $Offline) {
  Write-Host "[stage] provisioning data for watchlist"
  & (Join-Path $Root "provision_data.ps1") -Config $ResolvedConfig -WatchlistPath $ResolvedWatchlist
  if ($LASTEXITCODE -ne 0) {
    throw "Provisioning failed with exit code $LASTEXITCODE."
  }
} else {
  Write-Host "[skip] offline mode enabled; skipping data provisioning"
}

$OutputsRoot = Join-Path $Root "..\outputs"
$TempOutdir = Join-Path $OutputsRoot "tmp_run"
$CacheDir = Join-Path $OutputsRoot "cache"

if (Test-Path $TempOutdir) {
  Remove-Item -Recurse -Force $TempOutdir
}
New-Item -ItemType Directory -Path $TempOutdir | Out-Null

$args = @(
  "-m","market_monitor","run",
  "--config",$ResolvedConfig,
  "--watchlist",$ResolvedWatchlist,
  "--outdir",$TempOutdir,
  "--cache-dir",$CacheDir,
  "--mode","watchlist",
  "--log-level","INFO"
)
if ($RunId) { $args += @("--run-id",$RunId) }
if ($Offline) { $args += "--offline" }

& $VenvPy @args
if ($LASTEXITCODE -ne 0) {
  throw "Pipeline failed with exit code $LASTEXITCODE."
}

$ResolvedRunId = & $VenvPy -c "import json,sys; from pathlib import Path; manifest=Path(sys.argv[1])/'run_manifest.json'; data=json.loads(manifest.read_text()); print(data.get('run_id',''))" $TempOutdir
if (-not $ResolvedRunId) {
  throw "run_id missing from run_manifest.json"
}

$FinalOutdir = Join-Path $OutputsRoot $ResolvedRunId
if (-not (Test-Path $FinalOutdir)) {
  New-Item -ItemType Directory -Path $FinalOutdir | Out-Null
}

Get-ChildItem -Path $TempOutdir -Force | Move-Item -Destination $FinalOutdir

$EligiblePath = Join-Path $FinalOutdir "eligible_$ResolvedRunId.csv"
if (Test-Path $EligiblePath) {
  Rename-Item -Path $EligiblePath -NewName "eligible.csv"
}

$ScoredPath = Join-Path $FinalOutdir "scored_$ResolvedRunId.csv"
if (Test-Path $ScoredPath) {
  Rename-Item -Path $ScoredPath -NewName "scored.csv"
}

$ReportPath = Join-Path $FinalOutdir "run_report.md"
if (Test-Path $ReportPath) {
  Rename-Item -Path $ReportPath -NewName "report.md"
}

Remove-Item -Recurse -Force $TempOutdir

Write-Host "[done] Outputs written to $FinalOutdir"
