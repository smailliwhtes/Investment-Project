param(
  [string]$Config = ".\config.yaml",
  [string]$OutRoot = ".\outputs\acceptance"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

Write-Host "[info] acceptance_test.ps1 now delegates to scripts\\acceptance.ps1."
& .\scripts\acceptance.ps1 -Config $Config -OutRoot $OutRoot
exit $LASTEXITCODE
