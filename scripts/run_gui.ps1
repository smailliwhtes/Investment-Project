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
# Keep workload install as best-effort in case components are already present.
try {
  dotnet workload install maui maui-windows | Out-Host
} catch {
  Write-Host "workload install warning: $($_.Exception.Message)"
}

# Restore/build with x64 so runtime layout matches verified smoke path.
dotnet workload restore --project "$gui" | Out-Host
dotnet restore "$gui" | Out-Host
dotnet build "$gui" -c Debug -p:Platform=x64 | Out-Host

$projectDir = Split-Path -Parent $gui
$exe = Get-ChildItem -Path (Join-Path $projectDir 'bin') -Recurse -Filter 'MarketApp.Gui.exe' -ErrorAction SilentlyContinue |
  Where-Object { $_.FullName -match '\\Debug\\' -and $_.FullName -match '\\win10-x64\\' } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

if (-not $exe) {
  throw "Built GUI executable not found under $projectDir/bin"
}

Write-Host "Launching GUI: $($exe.FullName)"
& $exe.FullName | Out-Host
