param(
  [string]$CloneRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$SourceRoot = (Resolve-Path (Join-Path $PSScriptRoot ".."))
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
  throw "git is required for acceptance_gate.ps1"
}

if (-not $CloneRoot) {
  $CloneRoot = Join-Path $env:TEMP ("market_app_acceptance_{0}" -f ([guid]::NewGuid().ToString("N")))
}
New-Item -ItemType Directory -Force -Path $CloneRoot | Out-Null

$ClonePath = Join-Path $CloneRoot "repo"
Write-Host "[stage] cloning repo to $ClonePath"
git clone $SourceRoot $ClonePath | Out-Null
Set-Location $ClonePath

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
  throw "No Python found. Install Python 3.11+ and retry."
}

$PySpec = Get-PythonSpec
$VenvPath = Join-Path $ClonePath ".venv"
$VenvPy = Join-Path $VenvPath "Scripts\python.exe"

if (-not (Test-Path $VenvPy)) {
  Write-Host "[stage] creating venv"
  & $PySpec.exe @($PySpec.args) -m venv $VenvPath
}

Write-Host "[stage] python version"
& $VenvPy --version

Write-Host "[stage] installing dependencies"
if (Test-Path (Join-Path $ClonePath "requirements.txt")) {
  Write-Host "[info] using requirements.txt"
  & $VenvPy -m pip install --upgrade pip | Out-Null
  & $VenvPy -m pip install -r "requirements.txt" | Out-Null
} elseif (Test-Path (Join-Path $ClonePath "pyproject.toml")) {
  Write-Host "[info] using pyproject.toml"
  & $VenvPy -m pip install --upgrade pip | Out-Null
  & $VenvPy -m pip install . | Out-Null
} else {
  throw "No requirements.txt or pyproject.toml found."
}

Write-Host "[stage] running pytest"
& $VenvPy -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "pytest failed" }

Write-Host "[stage] running doctor"
& $VenvPy -m market_app.cli doctor --config configs\acceptance.yaml
if ($LASTEXITCODE -ne 0) { throw "doctor failed" }

$RunsDir = "runs_acceptance"
$Run1 = "acceptance_run_1"
$Run2 = "acceptance_run_2"

Write-Host "[stage] running pipeline (run 1)"
& $VenvPy -m market_app.cli run --config configs\acceptance.yaml --offline --runs-dir $RunsDir --run-id $Run1
if ($LASTEXITCODE -ne 0) { throw "run 1 failed" }

Write-Host "[stage] running pipeline (run 2)"
& $VenvPy -m market_app.cli run --config configs\acceptance.yaml --offline --runs-dir $RunsDir --run-id $Run2
if ($LASTEXITCODE -ne 0) { throw "run 2 failed" }

$ExpectedCore = @(
  "eligible.csv",
  "scored.csv",
  "features.csv",
  "classified.csv",
  "universe.csv"
)
$ExpectedMeta = @(
  "manifest.json",
  "digest.json",
  "run.log"
)

foreach ($runId in @($Run1, $Run2)) {
  $RunDir = Join-Path $ClonePath $RunsDir | Join-Path -ChildPath $runId
  if (-not (Test-Path $RunDir)) { throw "Missing run dir: $RunDir" }
  foreach ($name in $ExpectedCore) {
    $path = Join-Path $RunDir $name
    if (-not (Test-Path $path)) { throw "Missing output $name in $RunDir" }
  }
  foreach ($name in $ExpectedMeta) {
    $path = Join-Path $RunDir $name
    if (-not (Test-Path $path)) { throw "Missing output $name in $RunDir" }
  }
  $reportMd = Join-Path $RunDir "report.md"
  $reportHtml = Join-Path $RunDir "report.html"
  if (-not (Test-Path $reportMd) -and -not (Test-Path $reportHtml)) {
    throw "Missing report.md or report.html in $RunDir"
  }
}

$Digest1 = Join-Path $ClonePath $RunsDir | Join-Path -ChildPath $Run1 | Join-Path -ChildPath "digest.json"
$Digest2 = Join-Path $ClonePath $RunsDir | Join-Path -ChildPath $Run2 | Join-Path -ChildPath "digest.json"

$Hash1 = (Get-FileHash -Algorithm SHA256 $Digest1).Hash
$Hash2 = (Get-FileHash -Algorithm SHA256 $Digest2).Hash

if ($Hash1 -ne $Hash2) {
  Write-Host "[error] Determinism check failed."
  Write-Host "  run1 digest: $Digest1"
  Write-Host "  run2 digest: $Digest2"
  Write-Host "  hash1: $Hash1"
  Write-Host "  hash2: $Hash2"
  exit 1
}

Write-Host "[done] acceptance gate PASS"
