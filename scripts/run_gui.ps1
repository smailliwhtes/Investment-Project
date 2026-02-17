Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $repoRoot

# --- Preflight: check for dotnet ---
$dotnetCmd = Get-Command dotnet -ErrorAction SilentlyContinue
if (-not $dotnetCmd) {
    # Check common install paths
    $commonPaths = @(
        "$env:ProgramFiles\dotnet\dotnet.exe",
        "${env:ProgramFiles(x86)}\dotnet\dotnet.exe",
        "$env:USERPROFILE\.dotnet\dotnet.exe"
    )
    foreach ($p in $commonPaths) {
        if (Test-Path $p) { $dotnetCmd = $p; break }
    }
}
if (-not $dotnetCmd) {
    Write-Host "ERROR: dotnet executable not found." -ForegroundColor Red
    Write-Host "Install .NET SDK 8:" -ForegroundColor Yellow
    Write-Host "  winget install --id Microsoft.DotNet.SDK.8 --source winget" -ForegroundColor Yellow
    Write-Host "Then install MAUI workload:" -ForegroundColor Yellow
    Write-Host "  dotnet workload install maui" -ForegroundColor Yellow
    exit 1
}

$sdks = & dotnet --list-sdks 2>&1
if (-not $sdks -or "$sdks" -match "No .NET SDKs") {
    Write-Host "ERROR: dotnet found but no SDKs installed." -ForegroundColor Red
    Write-Host "Install .NET SDK 8:" -ForegroundColor Yellow
    Write-Host "  winget install --id Microsoft.DotNet.SDK.8 --source winget" -ForegroundColor Yellow
    Write-Host "  dotnet --list-sdks" -ForegroundColor Yellow
    Write-Host "Then install MAUI workload:" -ForegroundColor Yellow
    Write-Host "  dotnet workload install maui" -ForegroundColor Yellow
    exit 1
}

# Optional: check for MAUI workload
$workloads = & dotnet workload list 2>&1
if ("$workloads" -notmatch "maui") {
    Write-Host "WARNING: MAUI workload not detected. The build may fail." -ForegroundColor Yellow
    Write-Host "  Install with: dotnet workload install maui" -ForegroundColor Yellow
}

dotnet build "src/gui/MarketApp.Gui.sln" -c Release
dotnet run --project "src/gui/MarketApp.Gui/MarketApp.Gui.csproj" -c Release
