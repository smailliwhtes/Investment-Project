param(
  [string]$PackRoot,
  [string]$OutDir = "out\metadata_cache",
  [string]$IncomingRoot = "incoming_metadata",
  [string]$AsOf = (Get-Date -Format "yyyy-MM-dd"),
  [string]$UserAgent = "OfflineMarketMonitor/1.0 (contact: you@example.com)",
  [switch]$IncludeSubmissionsZip
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
$incomingAsOf = Join-Path $packRoot (Join-Path $IncomingRoot $AsOf)
$incomingNasdaq = Join-Path $incomingAsOf "nasdaq_trader"
$incomingSec = Join-Path $incomingAsOf "sec"
$outRoot = Join-Path $packRoot $OutDir
$outNasdaq = Join-Path $outRoot "nasdaq_trader"
$outSec = Join-Path $outRoot "sec"
$outSubmissions = Join-Path $outSec "submissions"

New-Item -ItemType Directory -Force -Path $incomingNasdaq, $incomingSec, $outNasdaq, $outSec | Out-Null
New-Item -ItemType Directory -Force -Path $outSubmissions | Out-Null

Write-Host "[info] Using PackRoot: $packRoot"
Write-Host "[info] Incoming snapshot path: $incomingAsOf"
Write-Host "[info] Metadata cache path: $outRoot"

$headers = @{ "User-Agent" = $UserAgent }

Write-Host "[stage] Downloading Nasdaq SymbolDirectory..."
Invoke-WebRequest -Uri "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt" `
  -OutFile (Join-Path $incomingNasdaq "nasdaqlisted.txt")
Invoke-WebRequest -Uri "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt" `
  -OutFile (Join-Path $incomingNasdaq "otherlisted.txt")

Write-Host "[stage] Downloading SEC ticker snapshots..."
Invoke-WebRequest -Uri "https://www.sec.gov/files/company_tickers.json" `
  -OutFile (Join-Path $incomingSec "company_tickers.json") -Headers $headers
Start-Sleep -Milliseconds 200
Invoke-WebRequest -Uri "https://www.sec.gov/files/company_tickers_exchange.json" `
  -OutFile (Join-Path $incomingSec "company_tickers_exchange.json") -Headers $headers
Start-Sleep -Milliseconds 200

Write-Host "[stage] Downloading SEC SIC codes HTML..."
$sicHtml = Join-Path $incomingSec "sic_codes.html"
Invoke-WebRequest -Uri "https://www.sec.gov/info/edgar/siccodes.htm" `
  -OutFile $sicHtml -Headers $headers

Write-Host "[stage] Building sic_codes.csv..."
python (Join-Path $packRoot "tools\build_sic_codes.py") `
  --html $sicHtml `
  --output (Join-Path $outSec "sic_codes.csv")

if ($IncludeSubmissionsZip) {
  $zipPath = Join-Path $incomingSec "submissions.zip"
  Write-Host "[stage] Downloading submissions.zip..."
  Invoke-WebRequest -Uri "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip" `
    -OutFile $zipPath -Headers $headers
  Write-Host "[stage] Extracting submissions.zip..."
  Expand-Archive -Path $zipPath -DestinationPath $outSubmissions -Force
}

Write-Host "[stage] Normalizing snapshots into metadata cache..."
Copy-Item -Path (Join-Path $incomingNasdaq "*.txt") -Destination $outNasdaq -Force
Copy-Item -Path (Join-Path $incomingSec "*.json") -Destination $outSec -Force

Write-Host "[done] Provisioning complete. Cache -> $outRoot"
