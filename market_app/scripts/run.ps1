param(
  [string]$Config = ".\config.example.yaml",
  [string]$RunId = "",
  [switch]$Offline,
  [string]$WatchlistPath = "",
  [string]$AsOf = "",
  [string]$OhlcvRawDir = "",
  [string]$OhlcvDailyDir = "",
  [string]$ExogenousDailyDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Resolve-Path "$Root\..")

$VenvPy = Join-Path $Root "..\.venv\Scripts\python.exe"
$VenvCli = Join-Path $Root "..\.venv\Scripts\market-monitor.exe"
if (-not (Test-Path $VenvPy)) { $VenvPy = "python" }
if (-not (Test-Path $VenvCli)) { $VenvCli = "market-monitor" }

try {
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
  $ResolvedRunId = $RunId
  if (-not $ResolvedRunId) {
    $ResolvedRunId = "run_{0:yyyyMMdd_HHmmss}" -f (Get-Date)
  }

  $args = @(
    "run",
    "--config",$ResolvedConfig,
    "--watchlist",$ResolvedWatchlist,
    "--run-id",$ResolvedRunId,
    "--outputs-dir",$OutputsRoot
  )
  if ($AsOf) { $args += @("--asof",$AsOf) }
  if ($OhlcvRawDir) { $args += @("--ohlcv-raw-dir",$OhlcvRawDir) }
  if ($OhlcvDailyDir) { $args += @("--ohlcv-daily-dir",$OhlcvDailyDir) }
  if ($ExogenousDailyDir) { $args += @("--exogenous-daily-dir",$ExogenousDailyDir) }

  & $VenvCli @args
  if ($LASTEXITCODE -ne 0) {
    throw "Pipeline failed with exit code $LASTEXITCODE."
  }

  $FinalOutdir = Join-Path $OutputsRoot $ResolvedRunId
  Write-Host "[done] Outputs written to $FinalOutdir"
  exit 0
} catch {
  Write-Host "[error] $($_.Exception.Message)"
  exit 1
}
