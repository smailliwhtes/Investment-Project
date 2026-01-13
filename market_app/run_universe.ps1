param(
  [string]$ConfigPath = ".\config.json",
  [int]$BatchSize = 500,
  [int]$Offset = 0,
  [switch]$UseFinnhubForCandidatesOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Prefer venv python
$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

if (-not (Test-Path $ConfigPath)) { throw "Missing config: $ConfigPath" }
$cfg = Get-Content $ConfigPath -Raw | ConvertFrom-Json

$dataRoot = Join-Path $PSScriptRoot $cfg.data_root
$rawDir   = Join-Path $dataRoot "raw"
$stooqDir = Join-Path $rawDir "stooq"
$procDir  = Join-Path $dataRoot "processed"
$logDir   = Join-Path $dataRoot "logs"
New-Item -ItemType Directory -Force -Path $stooqDir,$procDir,$logDir | Out-Null

$today = Get-Date -Format "yyyy-MM-dd"
$tag = "{0}_o{1}_n{2}" -f $today, $Offset, $BatchSize

$featuresCsv = Join-Path $procDir "features_$tag.csv"
$scoredCsv   = Join-Path $procDir "scored_$tag.csv"
$eligibleCsv = Join-Path $procDir "eligible_$tag.csv"
$reportMd    = Join-Path $procDir "run_report_$tag.md"
$universeCsv = Join-Path $procDir "universe.csv"

if (-not (Test-Path $universeCsv)) {
  throw "Missing $universeCsv. Run .\run.ps1 once first (it builds universe.csv)."
}

$universe = Import-Csv $universeCsv
$symbolsAll = @($universe | ForEach-Object { $_.symbol } | Where-Object { $_ -and $_.Trim().Length -gt 0 })

$symbols = @($symbolsAll | Select-Object -Skip $Offset -First $BatchSize)
if ($symbols.Count -eq 0) {
  Write-Host "No symbols in this window (Offset=$Offset, BatchSize=$BatchSize)."
  exit 0
}

if (Test-Path $featuresCsv) { Remove-Item $featuresCsv -Force }

# Finnhub throttle only if we’re actually calling it
$minDelaySec = 0
if (-not $UseFinnhubForCandidatesOnly) {
  $minDelaySec = [math]::Ceiling(60.0 / [double]$cfg.finnhub.calls_per_minute)
}

foreach ($sym in $symbols) {
  $symU = $sym.Trim().ToUpper()
  if (-not $symU) { continue }

  # Download Stooq daily CSV
  $stooqSymbol = ($symU.ToLower() + ".us")
  $stooqUrl = "https://stooq.com/q/d/l/?s=$stooqSymbol&i=d"
  $stooqPath = Join-Path $stooqDir "$symU.csv"

  try {
    Invoke-WebRequest -Uri $stooqUrl -OutFile $stooqPath | Out-Null
  } catch {
    continue
  }

  # Validate header (avoid HTML/error downloads breaking pandas)
  $first = Get-Content $stooqPath -TotalCount 1 -ErrorAction SilentlyContinue
  if (-not $first -or ($first -notmatch '^(Date|date),')) { continue }

  $price = $null
  $quoteUnix = $null

  if (-not $UseFinnhubForCandidatesOnly) {
    $quoteUrl = "$($cfg.finnhub.base_url)/quote?symbol=$symU&token=$($cfg.finnhub.api_key)"
    try {
      $q = Invoke-RestMethod -Uri $quoteUrl -Method Get
      $price = $q.c
      $quoteUnix = $q.t
    } catch {
      $price = $null
      $quoteUnix = $null
    }
    if ($minDelaySec -gt 0) { Start-Sleep -Seconds $minDelaySec }
  }

  # Build args safely: only pass --price/--quote_unix if they have values
  $args = @("--symbol",$symU,"--stooq_csv",$stooqPath,"--config",$ConfigPath)
  if ($null -ne $price -and "$price" -ne "") { $args += @("--price",$price) }
  if ($null -ne $quoteUnix -and "$quoteUnix" -ne "") { $args += @("--quote_unix",$quoteUnix) }

  $json = & $py ".\py\compute_features.py" @args
  if (-not $json) { continue }

  $json | & $py ".\py\append_json_row.py" "--out_csv" $featuresCsv
}

& $py ".\py\score_security.py" `
  "--features_csv" $featuresCsv `
  "--universe_csv" $universeCsv `
  "--config" $ConfigPath `
  "--scored_csv" $scoredCsv `
  "--eligible_csv" $eligibleCsv `
  "--report_md" $reportMd

Write-Host "Wrote:"
Write-Host "  $featuresCsv"
Write-Host "  $scoredCsv"
Write-Host "  $eligibleCsv"
Write-Host "  $reportMd"