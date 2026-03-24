Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Solution = "src/gui/MarketApp.Gui.sln"
$Project = "src/gui/MarketApp.ParquetConverter/MarketApp.ParquetConverter.csproj"
$Output = "artifacts/parquet-converter"

dotnet restore $Solution
dotnet publish $Project -c Release -p:WindowsPackageType=None -o $Output

Write-Output "Standalone converter published to $Output\MarketApp.ParquetConverter.exe"
