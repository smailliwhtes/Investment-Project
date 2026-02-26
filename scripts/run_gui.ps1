Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "Repo: $repoRoot"

# Prefer explicit MarketApp.Gui.csproj, else any csproj with 'Gui' in name/path
$gui = Get-ChildItem -Recurse -Filter "MarketApp.Gui.csproj" -ErrorAction SilentlyContinue |
  Select-Object -First 1 -ExpandProperty FullName

if (-not $gui) {
  $gui = Get-ChildItem -Recurse -Filter "*.csproj" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match 'MarketApp\.Gui' -or $_.Name -match 'MarketApp\.Gui' } |
    Select-Object -First 1 -ExpandProperty FullName
}

if (-not $gui) {
  $gui = Get-ChildItem -Recurse -Filter "*.csproj" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match '(?i)gui' } |
    Select-Object -First 1 -ExpandProperty FullName
}

if (-not $gui) { throw "GUI .csproj not found under $repoRoot" }

Write-Host "GUI project: $gui"

# Ensure MAUI workloads exist
dotnet workload install maui maui-windows | Out-Host

# Restore/build/run against the project (NOT the repo root)
dotnet workload restore --project "$gui" | Out-Host
dotnet restore "$gui" | Out-Host
dotnet run --project "$gui" -c Debug | Out-Host
