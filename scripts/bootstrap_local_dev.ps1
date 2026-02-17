<#
.SYNOPSIS
    Bootstrap local development environment for market_app.
.DESCRIPTION
    Discovers/validates local paths, generates watchlist from OHLCV files,
    creates required directories, and prints actionable diagnostics.
#>
[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --- 1) Resolve repo root from script location ---
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Resolve-Path (Join-Path $scriptDir "..")
Write-Host "Repo root: $repoRoot" -ForegroundColor Cyan

# --- 2) Load optional .env.local ---
$envLocalPath = Join-Path $repoRoot ".env.local"
$envLocal = @{}
if (Test-Path $envLocalPath) {
    Write-Host "Loading overrides from .env.local" -ForegroundColor DarkGray
    foreach ($line in (Get-Content $envLocalPath)) {
        $line = $line.Trim()
        if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
            $parts = $line -split "=", 2
            $key   = $parts[0].Trim()
            $val   = $parts[1].Trim()
            if ($key -and $val) { $envLocal[$key] = $val }
        }
    }
}

# --- 3) Compute effective paths ---
function Resolve-EffectivePath {
    param(
        [string]$EnvVarName,
        [string]$EnvLocalKey,
        [string[]]$Probes
    )
    $val = [System.Environment]::GetEnvironmentVariable($EnvVarName)
    if ($val -and (Test-Path $val)) { return $val }

    if ($envLocal.ContainsKey($EnvLocalKey) -and (Test-Path $envLocal[$EnvLocalKey])) {
        return $envLocal[$EnvLocalKey]
    }

    foreach ($probe in $Probes) {
        $expanded = [System.Environment]::ExpandEnvironmentVariables($probe)
        if (Test-Path $expanded) { return $expanded }
    }
    return $null
}

$ohlcvDir = Resolve-EffectivePath `
    -EnvVarName  "MARKET_APP_OHLCV_DAILY_DIR" `
    -EnvLocalKey "MARKET_APP_OHLCV_DAILY_DIR" `
    -Probes @(
        "$env:USERPROFILE\OneDrive\Desktop\Market_Files\ohlcv_daily_csv",
        "C:\Users\micha\OneDrive\Desktop\Market_Files\ohlcv_daily_csv"
    )

$corpusDir = Resolve-EffectivePath `
    -EnvVarName  "MARKET_APP_CORPUS_DIR" `
    -EnvLocalKey "MARKET_APP_CORPUS_DIR" `
    -Probes @(
        "$env:USERPROFILE\OneDrive\Desktop\NLP Corpus"
    )

# --- 4) Validate OHLCV directory ---
if (-not $ohlcvDir) {
    Write-Host ""
    Write-Host "ERROR: OHLCV directory not found." -ForegroundColor Red
    Write-Host "Set MARKET_APP_OHLCV_DAILY_DIR env var or add it to .env.local" -ForegroundColor Yellow
    Write-Host "  Example: MARKET_APP_OHLCV_DAILY_DIR=C:\path\to\ohlcv_daily_csv" -ForegroundColor Yellow
    Write-Host "  See .env.local.example for template." -ForegroundColor Yellow
    exit 1
}

$csvCount = (Get-ChildItem -Path $ohlcvDir -Filter "*.csv" -File | Measure-Object).Count
if ($csvCount -lt 1) {
    Write-Host "ERROR: OHLCV directory exists but contains no CSV files: $ohlcvDir" -ForegroundColor Red
    exit 1
}
Write-Host "OHLCV dir: $ohlcvDir ($csvCount CSVs)" -ForegroundColor Green

# --- 5) Ensure watchlist exists ---
$watchlistDir  = Join-Path $repoRoot "market_app\config\watchlists"
$watchlistPath = Join-Path $watchlistDir "watchlist_core.csv"

if (-not (Test-Path $watchlistDir)) {
    New-Item -ItemType Directory -Path $watchlistDir -Force | Out-Null
}

$needGenerate = $false
if (-not (Test-Path $watchlistPath)) {
    $needGenerate = $true
} else {
    $content = (Get-Content $watchlistPath -Raw).Trim()
    if (-not $content) { $needGenerate = $true }
}

if ($needGenerate) {
    Write-Host "Generating watchlist from OHLCV directory..." -ForegroundColor Yellow
    $ohlcvHeader = @("date","open","high","low","close")
    $symbols = @()

    foreach ($csvFile in (Get-ChildItem -Path $ohlcvDir -Filter "*.csv" -File)) {
        $baseName = $csvFile.BaseName
        if ($baseName -ieq "conversion_errors") { continue }

        try {
            $firstLine = (Get-Content $csvFile.FullName -TotalCount 1).ToLower()
            $hasOhlcv = $true
            foreach ($col in $ohlcvHeader) {
                if ($firstLine -notmatch $col) { $hasOhlcv = $false; break }
            }
            if ($hasOhlcv) {
                $symbols += $baseName.ToUpper()
            }
        } catch {
            # skip unreadable files
        }
    }

    $symbols = $symbols | Sort-Object -Unique
    $lines = @("symbol,theme_bucket,asset_type")
    foreach ($sym in $symbols) {
        $lines += "$sym,,equity"
    }
    $lines | Out-File -FilePath $watchlistPath -Encoding utf8
    Write-Host "  Wrote $($symbols.Count) symbols to $watchlistPath" -ForegroundColor Green
} else {
    Write-Host "Watchlist: $watchlistPath (exists)" -ForegroundColor Green
}

# --- 6) Ensure out-dir parent exists ---
$outDir = Join-Path $repoRoot "outputs\runs"
if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
    Write-Host "Created output dir: $outDir" -ForegroundColor Green
} else {
    Write-Host "Output dir: $outDir (exists)" -ForegroundColor Green
}

# --- 7) Ensure exogenous directory exists (but do NOT create dummy CSVs) ---
$exogenousDir = Join-Path $repoRoot "market_app\config\data\exogenous\daily_features"
if (-not (Test-Path $exogenousDir)) {
    New-Item -ItemType Directory -Path $exogenousDir -Force | Out-Null
    Write-Host "Created exogenous dir: $exogenousDir (empty; exogenous is optional by default)" -ForegroundColor Green
} else {
    Write-Host "Exogenous dir: $exogenousDir (exists)" -ForegroundColor Green
}

# --- 8) Dotnet diagnostics ---
Write-Host ""
Write-Host "--- .NET Diagnostics ---" -ForegroundColor Cyan
$dotnetCmd = Get-Command dotnet -ErrorAction SilentlyContinue
if (-not $dotnetCmd) {
    Write-Host "WARNING: dotnet not found on PATH." -ForegroundColor Red
    Write-Host "  Install .NET SDK 8:" -ForegroundColor Yellow
    Write-Host "    winget install --id Microsoft.DotNet.SDK.8 --source winget" -ForegroundColor Yellow
    Write-Host "  Then install MAUI workload:" -ForegroundColor Yellow
    Write-Host "    dotnet workload install maui" -ForegroundColor Yellow
} else {
    $sdks = & dotnet --list-sdks 2>&1
    if (-not $sdks -or "$sdks" -match "No .NET SDKs") {
        Write-Host "WARNING: dotnet found but no SDKs installed." -ForegroundColor Red
        Write-Host "  Install .NET SDK 8:" -ForegroundColor Yellow
        Write-Host "    winget install --id Microsoft.DotNet.SDK.8 --source winget" -ForegroundColor Yellow
        Write-Host "    dotnet --list-sdks" -ForegroundColor Yellow
        Write-Host "    dotnet workload install maui" -ForegroundColor Yellow
    } else {
        Write-Host "dotnet SDKs: $sdks" -ForegroundColor Green
        $workloads = & dotnet workload list 2>&1
        if ("$workloads" -notmatch "maui") {
            Write-Host "WARNING: MAUI workload not installed." -ForegroundColor Yellow
            Write-Host "  Run: dotnet workload install maui" -ForegroundColor Yellow
        } else {
            Write-Host "MAUI workload: installed" -ForegroundColor Green
        }
    }
}

# --- 9) Print summary ---
Write-Host ""
Write-Host "==============================" -ForegroundColor Cyan
Write-Host " Bootstrap Complete" -ForegroundColor Green
Write-Host "==============================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Effective paths:" -ForegroundColor White
Write-Host "  OHLCV:     $ohlcvDir" -ForegroundColor White
if ($corpusDir) {
    Write-Host "  Corpus:    $corpusDir" -ForegroundColor White
} else {
    Write-Host "  Corpus:    (not found; optional)" -ForegroundColor DarkGray
}
Write-Host "  Watchlist: $watchlistPath" -ForegroundColor White
Write-Host "  Output:    $outDir" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "  # Launch GUI (requires .NET SDK 8 + MAUI workload):" -ForegroundColor White
Write-Host "  .\scripts\run_gui.ps1" -ForegroundColor Yellow
Write-Host ""
Write-Host "  # Run engine preflight:" -ForegroundColor White
Write-Host "  cd market_app" -ForegroundColor Yellow
Write-Host "  python -m market_monitor.cli preflight --config .\config\config.yaml" -ForegroundColor Yellow
Write-Host ""
Write-Host "  # Run engine pipeline:" -ForegroundColor White
Write-Host "  python -m market_monitor.cli run --config .\config\config.yaml --out-dir ..\outputs\runs\manual_run --offline --progress-jsonl" -ForegroundColor Yellow
