param(
  [ValidateSet("themed","all","watchlist")]
  [string]$Universe = "themed",

  [string]$Watchlist = ".\watchlist.txt",

  [double]$PriceCap = 10.0,
  [double]$PriceFloor = 1.0,
  [double]$AdvMinUsd = 100000.0,

  [switch]$Strict,

  # strict-mode thresholds (passed only if -Strict is set)
  [double]$MaxDd6m = -0.50,
  [double]$Vol60Max = 1.25,
  [double]$ZeroVolMax = 0.02,
  [double]$PctAboveSma200Min = 0.30
)

$ErrorActionPreference = "Stop"

$here = $PSScriptRoot
if (-not $here) { $here = $PWD.Path }
Set-Location $here

if (!(Test-Path ".\.venv")) { python -m venv .venv }

$py = ".\.venv\Scripts\python.exe"
& $py -m pip install --upgrade pip | Out-Null
& $py -m pip install pandas numpy requests python-dateutil | Out-Null

$outDir = Join-Path $here "output"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

$script = ".\monitor_v2.py"
if (!(Test-Path $script)) { throw "Missing $script in $here" }

$args = @(
  $script,
  "--universe", $Universe,
  "--watchlist", $Watchlist,
  "--outdir", $outDir,
  "--price-cap", "$PriceCap",
  "--price-floor", "$PriceFloor",
  "--adv-min-usd", "$AdvMinUsd"
)

if ($Strict) {
  $args += @(
    "--strict",
    "--max-dd-6m", "$MaxDd6m",
    "--vol60-max", "$Vol60Max",
    "--zero-vol-max", "$ZeroVolMax",
    "--pct-above-sma200-min", "$PctAboveSma200Min"
  )
}

& $py @args
