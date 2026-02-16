Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $repoRoot

dotnet build "src/gui/MarketApp.Gui.sln" -c Release
dotnet run --project "src/gui/MarketApp.Gui/MarketApp.Gui.csproj" -c Release
