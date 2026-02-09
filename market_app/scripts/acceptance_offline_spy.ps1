param(
  [string]$Config = ".\config.yaml",
  [string]$RunsDir = ".\runs_daily",
  [string]$RunId = "daily_fix_spy",
  [string]$NowUtc = "2026-02-08T21:00:00Z"
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
& $VenvPy -m pip install -r ".\requirements.txt" | Out-Null

Write-Host "[stage] running tests"
& $VenvPy -m pytest -q
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Tests failed. Fix issues and rerun .\scripts\acceptance_offline_spy.ps1"
  exit $LASTEXITCODE
}

Write-Host "[stage] running offline run"
& $VenvPy -m market_app.cli run --config $Config --offline --runs-dir $RunsDir --run-id $RunId --now-utc $NowUtc --top-n 50
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] Pipeline run failed. See logs in $RunsDir\$RunId"
  exit $LASTEXITCODE
}

$env:MARKET_APP_ACCEPT_CONFIG = [System.IO.Path]::GetFullPath($Config)
$env:MARKET_APP_ACCEPT_RUNS_DIR = [System.IO.Path]::GetFullPath($RunsDir)
$env:MARKET_APP_ACCEPT_RUN_ID = $RunId
$env:MARKET_APP_ACCEPT_NOW_UTC = $NowUtc

$checkScript = @'
import os
from pathlib import Path

import pandas as pd

from market_monitor.config_schema import load_config
from market_monitor.data_paths import resolve_data_paths
from market_monitor.providers.nasdaq_daily import NasdaqDailyProvider, NasdaqDailySource
from market_monitor.timebase import parse_now_utc

config_path = Path(os.environ["MARKET_APP_ACCEPT_CONFIG"]).resolve()
config = load_config(config_path).config
base_dir = config_path.parent
paths = resolve_data_paths(config, base_dir)
if not paths.nasdaq_daily_dir:
    raise SystemExit("nasdaq_daily_dir is not configured. Set MARKET_APP_NASDAQ_DAILY_DIR or config paths.")

provider = NasdaqDailyProvider(
    NasdaqDailySource(directory=paths.nasdaq_daily_dir, cache_dir=base_dir / config["paths"]["cache_dir"])
)
spy_path = provider.resolve_symbol_file("SPY")
if not spy_path:
    raise SystemExit("SPY.csv not found in nasdaq_daily_dir.")

df = pd.read_csv(spy_path)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
anchor = parse_now_utc(os.environ["MARKET_APP_ACCEPT_NOW_UTC"]).date()
filtered = df[df["Date"].dt.date <= anchor]
if filtered.empty:
    raise SystemExit("SPY history is empty after applying now_utc anchor.")
latest = filtered["Date"].max().date()
expected_asof = latest.isoformat()
expected_history = min(int(config["staging"]["stage3_deep_days"]), len(filtered))
expected_freshness = max((anchor - latest).days, 0)

run_dir = Path(os.environ["MARKET_APP_ACCEPT_RUNS_DIR"]) / os.environ["MARKET_APP_ACCEPT_RUN_ID"]
scored_path = run_dir / "scored.csv"
if not scored_path.exists():
    raise SystemExit(f"scored.csv not found at {scored_path}")
scored = pd.read_csv(scored_path)
row = scored[scored["symbol"] == "SPY"]
if row.empty:
    raise SystemExit("SPY row missing from scored.csv")

as_of = str(row["as_of_date"].iloc[0])
history_days = int(row["history_days"].iloc[0])
freshness = int(row["data_freshness_days"].iloc[0])
status = str(row["data_status"].iloc[0]) if "data_status" in row else "UNKNOWN"

preflight_path = run_dir / "preflight_report.csv"
if preflight_path.exists():
    preflight = pd.read_csv(preflight_path)
    spy_preflight = preflight[(preflight["section"] == "symbol") & (preflight["name"] == "SPY")]
    if not spy_preflight.empty:
        diag = spy_preflight[
            [
                "name",
                "file_path",
                "min_date",
                "max_date",
                "freshness_days",
                "stage3_as_of_date",
                "stage3_history_days",
                "stage3_status",
            ]
        ].rename(columns={"name": "symbol"})
        print("\nPreflight snapshot:")
        print(diag.to_string(index=False))

print(
    row[
        [
            "symbol",
            "as_of_date",
            "history_days",
            "data_freshness_days",
            "as_of_date_deep",
            "history_days_deep",
        ]
    ].assign(status=status).to_string(index=False)
)
if as_of != expected_asof:
    raise SystemExit(f"Expected as_of_date {expected_asof}, got {as_of}.")
if history_days != expected_history:
    raise SystemExit(f"Expected history_days {expected_history}, got {history_days}.")
if freshness != expected_freshness:
    raise SystemExit(f"Expected data_freshness_days {expected_freshness}, got {freshness}.")

print("SPY acceptance check passed.")
'@

$checkScript | & $VenvPy -
if ($LASTEXITCODE -ne 0) {
  Write-Host "[error] SPY acceptance checks failed."
  exit $LASTEXITCODE
}

Write-Host "[done] Acceptance run completed -> $RunsDir\$RunId"
