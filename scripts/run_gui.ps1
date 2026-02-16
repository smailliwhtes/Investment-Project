Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

dotnet build "src/gui/MarketApp.Gui/MarketApp.Gui.csproj" -c Release
dotnet run --project "src/gui/MarketApp.Gui/MarketApp.Gui.csproj" -c Release
