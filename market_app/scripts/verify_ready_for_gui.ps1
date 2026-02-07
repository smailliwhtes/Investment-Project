param(
  [string]$ConfigPath = ".\market_app\configs\acceptance.yaml",
  [string]$RunsDir = ".\runs_acceptance",
  [switch]$Offline = $true,
  [string]$PythonExe = "python",
  [string]$FixturesDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$AppRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$RepoRoot = Resolve-Path (Join-Path $AppRoot "..")

function Resolve-RepoPath {
  param([string]$PathValue)
  if ([System.IO.Path]::IsPathRooted($PathValue)) {
    return $PathValue
  }
  return (Join-Path $RepoRoot $PathValue)
}

function Add-Warning {
  param([string]$Message)
  Write-Host "[warn] $Message"
}

function Add-Failure {
  param([string]$Message)
  Write-Host "[fail] $Message"
  $script:Failed = $true
}

function Invoke-Checked {
  param(
    [string]$Label,
    [string[]]$Command
  )
  Write-Host "[step] $Label"
  & $Command
  if ($LASTEXITCODE -ne 0) {
    Add-Failure "$Label failed (exit code $LASTEXITCODE)."
    throw "$Label failed."
  }
}

function Find-FixturesDir {
  param([string]$Fallback)
  if ($Fallback -and (Test-Path $Fallback)) {
    return $Fallback
  }
  $candidates = Get-ChildItem -Path $RepoRoot -Recurse -Directory -Filter "ohlcv" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match "fixtures" }
  if ($candidates) {
    return $candidates[0].FullName
  }
  return $null
}

$Failed = $false
$LocationPushed = $false

try {
  $ResolvedConfigPath = Resolve-RepoPath $ConfigPath
  if (-not (Test-Path $ResolvedConfigPath)) {
    Add-Failure "Config not found: $ResolvedConfigPath"
    throw "Config missing."
  }

  $configText = Get-Content -Path $ResolvedConfigPath -Raw
  $absolutePathPattern = '(?m)^[^#]*(?:[A-Za-z]:[\\/]|/Users/|/home/|\\\\)'
  if ($configText -match $absolutePathPattern) {
    Add-Warning "Acceptance config contains absolute-looking paths. Prefer repo-local fixtures."
  }

  Push-Location $AppRoot
  $LocationPushed = $true
  Invoke-Checked "CLI help" @($PythonExe, "-m", "market_app.cli", "--help")

  $defaultFixtures = Resolve-RepoPath ".\market_app\tests\fixtures\ohlcv"
  if ($FixturesDir) {
    $ResolvedFixtures = Find-FixturesDir (Resolve-RepoPath $FixturesDir)
  } else {
    $ResolvedFixtures = Find-FixturesDir $defaultFixtures
  }
  if (-not $ResolvedFixtures) {
    Add-Failure "Fixture directory not found. Expected $defaultFixtures."
    throw "Fixtures missing."
  }
  if (-not (Test-Path $ResolvedFixtures)) {
    Add-Failure "Fixture directory missing: $ResolvedFixtures"
    throw "Fixtures missing."
  }
  Write-Host "[info] using fixtures: $ResolvedFixtures"

  $ResolvedRunsDir = Resolve-RepoPath $RunsDir
  if (-not (Test-Path $ResolvedRunsDir)) {
    New-Item -ItemType Directory -Force -Path $ResolvedRunsDir | Out-Null
  }

  $offlineArgs = @()
  if ($Offline) { $offlineArgs = @("--offline") }

  $doctorArgs = @($PythonExe, "-m", "market_app.cli", "doctor", "--config", $ResolvedConfigPath) + $offlineArgs
  Invoke-Checked "Doctor" $doctorArgs

  $run1 = "gui_ready_check_1"
  $run2 = "gui_ready_check_2"

  $run1Args = @(
    $PythonExe, "-m", "market_app.cli", "run",
    "--config", $ResolvedConfigPath,
    "--runs-dir", $ResolvedRunsDir,
    "--run-id", $run1
  ) + $offlineArgs
  Invoke-Checked "Run pipeline ($run1)" $run1Args

  $run2Args = @(
    $PythonExe, "-m", "market_app.cli", "run",
    "--config", $ResolvedConfigPath,
    "--runs-dir", $ResolvedRunsDir,
    "--run-id", $run2
  ) + $offlineArgs
  Invoke-Checked "Run pipeline ($run2)" $run2Args

  foreach ($runId in @($run1, $run2)) {
    $runDir = Join-Path $ResolvedRunsDir $runId
    if (-not (Test-Path $runDir)) {
      Add-Failure "Missing run dir: $runDir"
      throw "Run directory missing."
    }

    foreach ($required in @("manifest.json", "digest.json")) {
      $path = Join-Path $runDir $required
      if (-not (Test-Path $path)) {
        Add-Failure "Missing required artifact $required in $runDir"
        throw "Missing required artifact."
      }
    }

    $reportCandidates = Get-ChildItem -Path $runDir -Filter "report.*" -File -ErrorAction SilentlyContinue
    if (-not $reportCandidates) {
      Add-Warning "Report file missing in $runDir"
    }

    $logPath = Join-Path $runDir "run.log"
    if (-not (Test-Path $logPath)) {
      Add-Warning "run.log missing in $runDir"
    }
  }

  $digest1 = Join-Path $ResolvedRunsDir $run1 | Join-Path -ChildPath "digest.json"
  $digest2 = Join-Path $ResolvedRunsDir $run2 | Join-Path -ChildPath "digest.json"

  $digestText1 = Get-Content -Path $digest1 -Raw
  $digestText2 = Get-Content -Path $digest2 -Raw

  if ($digestText1 -ne $digestText2) {
    Add-Failure "Determinism check failed: digest.json differs between runs."
    $lines1 = $digestText1 -split "`n"
    $lines2 = $digestText2 -split "`n"
    $diffPreview = Compare-Object -ReferenceObject $lines1 -DifferenceObject $lines2 -SyncWindow 0 |
      Select-Object -First 10
    if ($diffPreview) {
      Write-Host "[diff] preview (first 10 lines):"
      $diffPreview | ForEach-Object { Write-Host ("  {0} {1}" -f $_.SideIndicator, $_.InputObject) }
    }
    throw "Digest mismatch."
  }

  Write-Host "[done] GUI readiness verification PASS"
} catch {
  Write-Host "[error] $($_.Exception.Message)"
} finally {
  if ($LocationPushed) {
    Pop-Location
  }
  if ($Failed) {
    Write-Host "[summary] FAIL"
    exit 1
  }
  Write-Host "[summary] PASS"
  exit 0
}
