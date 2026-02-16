$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$project = Join-Path $repoRoot 'src/gui/MarketApp.Gui/MarketApp.Gui.csproj'

Write-Host "Running GUI project: $project"
dotnet build $project -t:Run -f net8.0-windows10.0.19041.0
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
