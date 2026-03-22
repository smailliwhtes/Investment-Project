param(
    [string]$PolicyDataRoot = "",
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$marketAppRoot = Join-Path $repoRoot "market_app"

if ([string]::IsNullOrWhiteSpace($PolicyDataRoot)) {
    $PolicyDataRoot = Join-Path $marketAppRoot "data"
}

$policyDataRoot = [System.IO.Path]::GetFullPath($PolicyDataRoot)
$fredDir = Join-Path $policyDataRoot "fred_cache"
$eventsPath = Join-Path $policyDataRoot "policy_events.jsonl"
$gdeltPath = Join-Path $policyDataRoot "policy_gdelt_daily_features.csv"
$holdingsPath = Join-Path $policyDataRoot "etf_holdings.csv"

New-Item -ItemType Directory -Force -Path $policyDataRoot | Out-Null
New-Item -ItemType Directory -Force -Path $fredDir | Out-Null

function Write-SeedFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string]$Content
    )

    if ((Test-Path -LiteralPath $Path) -and -not $Force) {
        Write-Host "Keeping existing $Path"
        return
    }

    Set-Content -LiteralPath $Path -Value $Content -Encoding utf8NoBOM
    Write-Host "Wrote $Path"
}

$policyEvents = @'
{"agency":"ustr","countries":["USA","CHN"],"event_date":"2024-02-20","event_id":"evt_tariff_20240220","event_type":"tariff","severity":0.7,"sectors":["industrials","supply_chain"],"source":"seed","summary":"Seed tariff proposal.","tickers":["AAA","BBB"],"title":"Tariff proposal"}
{"agency":"ustr","countries":["USA","CHN"],"event_date":"2024-06-17","event_id":"evt_tariff_20240617","event_type":"tariff","severity":0.78,"sectors":["industrials","metals"],"source":"seed","summary":"Seed tariff implementation.","tickers":["AAA"],"title":"Tariff implementation"}
{"agency":"treasury","countries":["USA","RUS"],"event_date":"2024-09-09","event_id":"evt_sanction_20240909","event_type":"sanction","severity":0.55,"sectors":["energy"],"source":"seed","summary":"Seed sanctions package.","tickers":["GLD"],"title":"Sanctions package"}
'@

$gdeltDaily = @'
Date,conflict_event_count_total,energy_stress_score
2024-02-20,3,0.35
2024-06-17,4,0.45
2024-09-09,5,0.58
2025-01-31,5,0.61
'@

$etfHoldings = @'
as_of_date,etf_symbol,constituent_symbol,weight,sector,theme
2025-01-31,XLI,AAA,0.55,industrials,supply_chain
2025-01-31,XLI,BBB,0.45,industrials,metals
2025-01-31,GLD,GLD,1.00,metals,commodities
'@

$fedFunds = @'
date,value
2024-01-01,4.75
2024-02-01,4.75
2024-03-01,4.75
2024-04-01,4.75
2024-05-01,4.75
2024-06-01,4.75
2024-07-01,4.75
2024-08-01,4.50
2024-09-01,4.25
2024-10-01,4.25
2024-11-01,4.25
2024-12-01,4.00
2025-01-01,4.00
'@

$unrate = @'
date,value
2024-01-01,4.0
2024-02-01,4.0
2024-03-01,3.9
2024-04-01,3.9
2024-05-01,4.0
2024-06-01,4.1
2024-07-01,4.1
2024-08-01,4.2
2024-09-01,4.2
2024-10-01,4.2
2024-11-01,4.1
2024-12-01,4.1
2025-01-01,4.0
'@

$cpi = @'
date,value
2024-01-01,300.0
2024-02-01,300.6
2024-03-01,301.4
2024-04-01,302.1
2024-05-01,302.8
2024-06-01,303.7
2024-07-01,304.5
2024-08-01,305.4
2024-09-01,306.2
2024-10-01,307.0
2024-11-01,307.9
2024-12-01,308.8
2025-01-01,309.6
'@

$indpro = @'
date,value
2024-01-01,100.0
2024-02-01,100.2
2024-03-01,100.5
2024-04-01,100.7
2024-05-01,101.0
2024-06-01,101.2
2024-07-01,101.5
2024-08-01,101.7
2024-09-01,102.0
2024-10-01,102.3
2024-11-01,102.5
2024-12-01,102.8
2025-01-01,103.0
'@

Write-SeedFile -Path $eventsPath -Content $policyEvents
Write-SeedFile -Path $gdeltPath -Content $gdeltDaily
Write-SeedFile -Path $holdingsPath -Content $etfHoldings
Write-SeedFile -Path (Join-Path $fredDir "FEDFUNDS.csv") -Content $fedFunds
Write-SeedFile -Path (Join-Path $fredDir "UNRATE.csv") -Content $unrate
Write-SeedFile -Path (Join-Path $fredDir "CPIAUCSL.csv") -Content $cpi
Write-SeedFile -Path (Join-Path $fredDir "INDPRO.csv") -Content $indpro

Write-Host ""
Write-Host "Seed policy datasets are ready under $policyDataRoot"
Write-Host "Run policy simulations with:"
Write-Host "  cd market_app"
Write-Host "  python -m market_monitor.cli policy simulate --config .\\config\\config.yaml --scenario tariff-shock --outdir ..\\outputs\\policy\\tariff-shock --offline"
