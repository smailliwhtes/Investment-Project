Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Solution = "src/gui/MarketApp.Gui.sln"

dotnet restore $Solution
dotnet build $Solution -c Release
dotnet test "src/gui/MarketApp.Gui.Tests/MarketApp.Gui.Tests.csproj" -c Release
