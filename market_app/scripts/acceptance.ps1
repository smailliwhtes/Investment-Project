param(
  [string]$Python = "python",
  [string]$VenvDir = ".venv",
  [string]$Watchlist = "inputs\watchlist.txt"
)

$ErrorActionPreference = "Stop"

Write-Host "==> Creating venv"
& $Python -m venv $VenvDir

$py = Join-Path $VenvDir "Scripts\python.exe"
$pip = Join-Path $VenvDir "Scripts\pip.exe"

Write-Host "==> Installing requirements (quiet)"
& $pip install -r requirements.txt | Out-Null

Write-Host "==> Running tests"
& $py -m pytest -q

if (-not $env:NASDAQ_DAILY_DIR -and -not $env:MARKET_APP_DATA_ROOT) {
  Write-Host "==> NASDAQ_DAILY_DIR not set. Skipping pipeline run."
  Write-Host "==> Acceptance OK (tests-only)"
  exit 0
}

Write-Host "==> Running pipeline (offline default)"
$env:OFFLINE_MODE = "true"
& $py -m market_monitor run --config config.yaml --mode watchlist --watchlist $Watchlist

Write-Host "==> Verifying outputs exist"
if (-not (Test-Path "outputs")) { throw "Missing outputs/ directory" }
$must = @("features", "scored", "eligible", "run_report")
foreach ($m in $must) {
  $found = Get-ChildItem -Path "outputs" -Recurse -File | Where-Object { $_.Name -like "$m*" }
  if (-not $found) { throw "Missing expected output matching: $m*" }
}

Write-Host "==> Acceptance OK"
