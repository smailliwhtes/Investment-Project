param(
  [string]$Config = ".\config.yaml",
  [string]$OutRoot = ".\outputs\runs",
  [switch]$SkipInstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Get-PythonSpec {
  $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
  if ($pyLauncher) {
    try {
      & py -3.11 -c "import sys" | Out-Null
      if ($LASTEXITCODE -eq 0) { return @{ exe="py"; args=@("-3.11") } }
    } catch {}
  }
  $py = Get-Command python -ErrorAction SilentlyContinue
  if ($py) { return @{ exe="python"; args=@() } }
  throw "No Python found. Install Python 3.11 and reopen PowerShell."
}

Write-Host "[info] run_all.ps1 now delegates to scripts\\acceptance.ps1 (offline watchlist only)."
& .\scripts\acceptance.ps1 -Config $Config -OutRoot $OutRoot
exit $LASTEXITCODE
