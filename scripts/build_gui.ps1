$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$project = Join-Path $repoRoot 'src/gui/MarketApp.Gui/MarketApp.Gui.csproj'

Write-Host "Building GUI project: $project"
dotnet workload restore $project
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

dotnet build $project -c Debug
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }