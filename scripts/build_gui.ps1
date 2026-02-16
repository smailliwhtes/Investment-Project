$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$solution = Join-Path $repoRoot 'src/gui/MarketApp.Gui/MarketApp.Gui.sln'

Write-Host "Building GUI solution: $solution"
dotnet workload restore $solution
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

dotnet build $solution -c Debug
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
