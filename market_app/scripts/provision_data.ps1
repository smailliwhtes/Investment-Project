param(
  [string]$Config = ".\config\config.yaml",
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

Write-Host "[stage] provisioning data for watchlist: $ResolvedWatchlist"
& $VenvPy -m market_monitor bulk-download --config $ResolvedConfig --mode watchlist --watchlist $ResolvedWatchlist
if ($LASTEXITCODE -ne 0) {
  throw "Data provisioning failed with exit code $LASTEXITCODE."
}

Write-Host "[done] watchlist provisioning complete"
