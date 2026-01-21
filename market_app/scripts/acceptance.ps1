param(
  [string]$Config = ".\config.yaml",
  [string]$OutRoot = ".\outputs\acceptance"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

function Get-PythonSpec {
  $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
  if ($pyLauncher) {
    try {
      & py -3.11 -c "import sys" | Out-Null
      if ($LASTEXITCODE -eq 0) { return @{ exe="py"; args=@("-3.11") } }
    } catch {}
  }
  $py = Get-Command python -ErrorAction SilentlyContinue
  if ($py) { return @{ exe="python"; args=@() } }
  throw "No Python found. Install Python 3.11 and reopen PowerShell."
}

$PySpec = Get-PythonSpec
$VenvPath = Join-Path $Root "..\.venv"
$VenvPy   = Join-Path $VenvPath "Scripts\python.exe"

if (-not (Test-Path $VenvPy)) {
  Write-Host "[stage] creating venv"
  & $PySpec.exe @($PySpec.args) -m venv $VenvPath
}

Write-Host "[stage] upgrading pip"
& $VenvPy -m pip install --upgrade pip 1>$null
Write-Host "[stage] installing requirements"
& $VenvPy -m pip install -r ".\requirements.txt" 1>$null

$UseFixtures = $false
if (-not $env:MARKET_APP_NASDAQ_DAILY_DIR) {
  $ConfigHasPath = & $VenvPy - <<'PY'
import yaml
from pathlib import Path
config_path = Path(r".\config.yaml")
if not config_path.exists():
    print("MISSING")
    raise SystemExit(0)
data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
paths = data.get("data", {}).get("paths", {})
value = paths.get("nasdaq_daily_dir")
print("SET" if value else "EMPTY")
PY
  if ($ConfigHasPath -eq "EMPTY" -or $ConfigHasPath -eq "MISSING") {
    $UseFixtures = $true
  }
}

if ($UseFixtures) {
  Write-Host "[stage] using fixture data paths"
  $env:MARKET_APP_NASDAQ_DAILY_DIR = (Resolve-Path ".\tests\fixtures\ohlcv")
  if (-not $env:MARKET_APP_GDELT_CONFLICT_DIR) {
    $env:MARKET_APP_GDELT_CONFLICT_DIR = (Resolve-Path ".\tests\fixtures\corpus")
  }
  $WatchlistOverride = (Resolve-Path ".\tests\fixtures\watchlist.txt")
}

Write-Host "[stage] running tests"
& $VenvPy -m pytest -q
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Tests failed. Fix issues and rerun .\scripts\acceptance.ps1"
  exit $LASTEXITCODE
}

Write-Host "[stage] running doctor"
& $VenvPy -m market_monitor doctor --config $Config --offline
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Doctor checks failed. Fix issues and rerun .\scripts\acceptance.ps1"
  exit $LASTEXITCODE
}

$RunId = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss")
$RunDir = Join-Path $OutRoot "run_$RunId"
if (-not (Test-Path $RunDir)) { New-Item -ItemType Directory -Path $RunDir | Out-Null }

Write-Host "[stage] running preflight"
& $VenvPy -m market_monitor preflight --config $Config --outdir $RunDir
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Preflight failed. Fix issues and rerun .\scripts\acceptance.ps1"
  exit $LASTEXITCODE
}

Write-Host "[stage] running watchlist pipeline"
if ($WatchlistOverride) {
  & $VenvPy -m market_monitor run --config $Config --mode watchlist --outdir $RunDir --watchlist $WatchlistOverride
} else {
  & $VenvPy -m market_monitor run --config $Config --mode watchlist --outdir $RunDir
}
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Pipeline run failed. See logs in .\outputs\logs"
  exit $LASTEXITCODE
}

$Expected = @(
  "features_*.csv",
  "scored_*.csv",
  "eligible_*.csv",
  "run_report*.md",
  "preflight_report.csv",
  "preflight_report.md",
  "run_manifest.json"
)
foreach ($pattern in $Expected) {
  $found = Get-ChildItem -Path $RunDir -Filter $pattern | Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if (-not $found) {
    Write-Host "[error] Missing expected output ($pattern) in $RunDir"
    exit 1
  }
  if ($found.Length -le 0) {
    Write-Host "[error] Output file is empty ($($found.FullName))"
    exit 1
  }
}

if ($env:MARKET_APP_GDELT_CONFLICT_DIR) {
  $CorpusExpected = @(
    "corpus\daily_features.csv",
    "corpus\analogs_report.md",
    "corpus\corpus_manifest.json"
  )
  foreach ($pattern in $CorpusExpected) {
    $path = Join-Path $RunDir $pattern
    if (-not (Test-Path $path)) {
      Write-Host "[error] Missing expected corpus output ($pattern) in $RunDir"
      exit 1
    }
    $file = Get-Item $path
    if ($file.Length -le 0) {
      Write-Host "[error] Corpus output file is empty ($path)"
      exit 1
    }
  }
}

Write-Host "[done] Acceptance run completed -> $RunDir"
