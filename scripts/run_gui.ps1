Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).ProviderPath
Set-Location $repoRoot

$dotnetCmd = Get-Command dotnet -ErrorAction SilentlyContinue
if (-not $dotnetCmd) {
    Write-Host "dotnet not found. Install .NET SDK 8.x. Then rerun." -ForegroundColor Red
    Write-Host "Install command: winget install --id Microsoft.DotNet.SDK.8 --source winget" -ForegroundColor Yellow
    exit 1
}

$sdks = & dotnet --list-sdks 2>&1
if ($LASTEXITCODE -ne 0 -or -not $sdks -or "$sdks" -match "No .NET SDKs" -or "$sdks" -notmatch "(?m)^8\.") {
    Write-Host "dotnet not found. Install .NET SDK 8.x. Then rerun." -ForegroundColor Red
    Write-Host "Install command: winget install --id Microsoft.DotNet.SDK.8 --source winget" -ForegroundColor Yellow
    Write-Host "Detected SDKs:" -ForegroundColor Yellow
    Write-Host "$sdks" -ForegroundColor Yellow
    exit 1
}

$workloads = & dotnet workload list 2>&1
if ($LASTEXITCODE -ne 0 -or "$workloads" -notmatch "(?im)^\s*maui\s") {
    Write-Host "MAUI workload missing. Run: dotnet workload install maui" -ForegroundColor Red
    exit 1
}

dotnet build "src/gui/MarketApp.Gui.sln" -c Release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

dotnet run --project "src/gui/MarketApp.Gui/MarketApp.Gui.csproj" -c Release
exit $LASTEXITCODE
