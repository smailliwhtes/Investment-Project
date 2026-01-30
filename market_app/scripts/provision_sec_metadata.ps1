param(
  [string]$PackRoot,
  [ValidateSet("zip", "cik")] [string]$Mode = "zip",
  [string]$SecurityMasterPath = "out/security_master.csv",
  [string]$UserAgent = "OfflineMarketMonitor/1.0 (contact: you@example.com)"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $PackRoot) {
  if ($PSScriptRoot) {
    $PackRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
  } else {
    $PackRoot = (Get-Location).Path
  }
}

$packRoot = (Resolve-Path $PackRoot).Path
$secRoot = Join-Path $packRoot "incoming_metadata\sec"
$submissionsDir = Join-Path $secRoot "submissions"
$outDir = Join-Path $packRoot "out"

New-Item -ItemType Directory -Path $secRoot -Force | Out-Null
New-Item -ItemType Directory -Path $submissionsDir -Force | Out-Null
New-Item -ItemType Directory -Path $outDir -Force | Out-Null

$headers = @{ "User-Agent" = $UserAgent }

Write-Host "[info] Downloading SEC ticker snapshots..."
Invoke-WebRequest -Uri "https://www.sec.gov/files/company_tickers.json" `
  -OutFile (Join-Path $secRoot "company_tickers.json") -Headers $headers
Invoke-WebRequest -Uri "https://www.sec.gov/files/company_tickers_exchange.json" `
  -OutFile (Join-Path $secRoot "company_tickers_exchange.json") -Headers $headers

Write-Host "[info] Downloading SIC code list..."
$sicHtml = Join-Path $secRoot "sic_codes.html"
Invoke-WebRequest -Uri "https://www.sec.gov/info/edgar/siccodes.htm" `
  -OutFile $sicHtml -Headers $headers

Write-Host "[info] Building sic_codes.csv..."
python (Join-Path $packRoot "tools\build_sic_codes.py") `
  --html $sicHtml `
  --output (Join-Path $outDir "sic_codes.csv")

if ($Mode -eq "zip") {
  $zipPath = Join-Path $secRoot "submissions.zip"
  Write-Host "[info] Downloading submissions.zip..."
  Invoke-WebRequest -Uri "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip" `
    -OutFile $zipPath -Headers $headers
  Write-Host "[info] Extracting submissions.zip..."
  Expand-Archive -Path $zipPath -DestinationPath $submissionsDir -Force
} else {
  $masterPath = Join-Path $packRoot $SecurityMasterPath
  if (-not (Test-Path $masterPath)) {
    throw "Security master CSV not found at $masterPath (needed for per-CIK downloads)."
  }
  $rows = Import-Csv $masterPath | Where-Object { $_.cik }
  $ciks = $rows | ForEach-Object { $_.cik.ToString().PadLeft(10, "0") } | Sort-Object -Unique
  Write-Host "[info] Downloading per-CIK submissions (" $ciks.Count " entries)..."
  foreach ($cik in $ciks) {
    $dest = Join-Path $submissionsDir ("CIK{0}.json" -f $cik)
    if (Test-Path $dest) {
      continue
    }
    $uri = "https://data.sec.gov/submissions/CIK{0}.json" -f $cik
    Invoke-WebRequest -Uri $uri -OutFile $dest -Headers $headers
    Start-Sleep -Milliseconds 200
  }
}

Write-Host "[info] SEC metadata provisioning complete."
