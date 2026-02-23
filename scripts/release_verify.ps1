#!/usr/bin/env pwsh
param(
    [switch]$SkipDotnetTests,
    [switch]$SkipGuiSmoke,
    [switch]$SkipE2E,
    [switch]$SkipSbom,
    [switch]$SkipPipAudit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot

$auditRoot = Join-Path $repoRoot 'audit'
$logsRoot  = Join-Path $auditRoot 'logs'
$sbomRoot  = Join-Path $auditRoot 'sbom'
New-Item -ItemType Directory -Force -Path $auditRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logsRoot  | Out-Null
New-Item -ItemType Directory -Force -Path $sbomRoot  | Out-Null

$gitCommit = (git rev-parse --short HEAD).Trim()
$gitBranch = (git rev-parse --abbrev-ref HEAD).Trim()
$gitDirty  = [bool](git status --porcelain)
$runId     = "$(Get-Date -Format 'yyyyMMdd-HHmmss')-$gitCommit"

$report = [ordered]@{
    schema_version  = 2
    run_id          = $runId
    timestamp_utc   = (Get-Date).ToUniversalTime().ToString('o')
    git             = @{ branch = $gitBranch; commit = $gitCommit; dirty = $gitDirty }
    platform        = @{
        os     = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        pwsh   = $PSVersionTable.PSVersion.ToString()
        dotnet = $null
        python = $null
    }
    gates           = @()
    overall_status  = 'fail'
    artifacts_root  = 'audit'
}

function Save-Report {
    $reportPath = Join-Path $auditRoot 'verify_report.json'
    $report | ConvertTo-Json -Depth 10 | Set-Content -Path $reportPath -Encoding UTF8
}

function Add-GateResult {
    param([hashtable]$Gate)
    $script:report.gates += $Gate
}

function Invoke-LoggedCommand {
    param(
        [string]$Name,
        [string]$Command,
        [string]$WorkingDirectory = $repoRoot
    )
    $logPath = Join-Path $logsRoot "$Name.log"
    Push-Location -LiteralPath $WorkingDirectory
    try {
        $output = & pwsh -NoProfile -Command $Command 2>&1 | Out-String
        $output | Set-Content -Path $logPath -Encoding UTF8
        return @{ ExitCode = $LASTEXITCODE; Log = $logPath; Command = $Command }
    }
    finally {
        Pop-Location
    }
}

function Assert-RunStalenessContract {
    param([string]$RunDirectory)

    $check = @"
import csv
import pathlib
import sys

run_dir = pathlib.Path(r'''$RunDirectory''')
scored_path = run_dir / 'scored.csv'
dq_path = run_dir / 'data_quality.csv'
if not scored_path.exists():
    raise SystemExit(f"missing scored.csv at {scored_path}")
if not dq_path.exists():
    raise SystemExit(f"missing data_quality.csv at {dq_path}")

with scored_path.open('r', encoding='utf-8', newline='') as f:
    scored_rows = list(csv.DictReader(f))
with dq_path.open('r', encoding='utf-8', newline='') as f:
    dq_rows = list(csv.DictReader(f))

if not scored_rows:
    raise SystemExit('scored.csv has no rows')

scored_cols = set(scored_rows[0].keys())
missing_cols = {'last_date', 'lag_days'} - scored_cols
if missing_cols:
    raise SystemExit(f"scored.csv missing columns: {sorted(missing_cols)}")

dq_index = {row['symbol']: row for row in dq_rows}
for row in scored_rows:
    sym = row.get('symbol', '')
    if sym not in dq_index:
        raise SystemExit(f"data_quality.csv missing symbol from scored.csv: {sym}")
    dq = dq_index[sym]
    if str(row.get('last_date', '')) != str(dq.get('last_date', '')):
        raise SystemExit(f"last_date mismatch for {sym}: scored={row.get('last_date')} dq={dq.get('last_date')}")
    if str(row.get('lag_days', '')) != str(dq.get('lag_days', '')):
        raise SystemExit(f"lag_days mismatch for {sym}: scored={row.get('lag_days')} dq={dq.get('lag_days')}")

print(f"staleness contract OK for {len(scored_rows)} scored rows")
"@

    return (Invoke-LoggedCommand -Name 'contract_staleness' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command "python -c @'$check'@")
}

try {
    try {
        $dotnetVersion = (& dotnet --version 2>$null)
        if ($LASTEXITCODE -eq 0) { $report.platform.dotnet = $dotnetVersion.Trim() }
    } catch {}

    try {
        $pyVersion = (& python --version 2>&1)
        if ($LASTEXITCODE -eq 0) { $report.platform.python = $pyVersion.Trim() }
    } catch {}

    # Gate: tests_engine
    $pyResult = Invoke-LoggedCommand -Name 'pytest' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command 'python -m pytest -q'
    if ($pyResult.ExitCode -ne 0) {
        Add-GateResult @{ name='tests_engine'; status='fail'; details=@{ pytest='fail'; dotnet_test='skipped' } }
        throw "pytest failed (see $($pyResult.Log))"
    }

    $dotnetStatus = 'skipped'
    if (-not $SkipDotnetTests -and $IsWindows -and (Test-Path -LiteralPath (Join-Path $repoRoot 'src/gui/MarketApp.Gui.sln'))) {
        $dnResult = Invoke-LoggedCommand -Name 'dotnet_test' -Command 'dotnet test src/gui/MarketApp.Gui.Tests/MarketApp.Gui.Tests.csproj -c Release'
        if ($dnResult.ExitCode -ne 0) {
            Add-GateResult @{ name='tests_engine'; status='fail'; details=@{ pytest='pass'; dotnet_test='fail' } }
            throw "dotnet test failed (see $($dnResult.Log))"
        }
        $dotnetStatus = 'pass'
    }
    Add-GateResult @{ name='tests_engine'; status='pass'; details=@{ pytest='pass'; dotnet_test=$dotnetStatus } }

    # Gate: e2e_offline
    $e2eOut = $null
    if ($SkipE2E) {
        Add-GateResult @{ name='e2e_offline'; status='skipped'; details=@{ reason='skipped by flag' } }
    } else {
        $e2eOut = Join-Path $auditRoot ("runs/$runId")
        New-Item -ItemType Directory -Path $e2eOut -Force | Out-Null

        $e2eCmd = "python -m market_monitor.cli run --config config.yaml --out-dir '$e2eOut' --offline --progress-jsonl"
        $e2eResult = Invoke-LoggedCommand -Name 'e2e_offline' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command $e2eCmd
        if ($e2eResult.ExitCode -ne 0) {
            Add-GateResult @{ name='e2e_offline'; status='fail'; details=@{ command=$e2eCmd; outputs_dir=$e2eOut } }
            throw "Offline E2E failed (see $($e2eResult.Log))"
        }
        Add-GateResult @{ name='e2e_offline'; status='pass'; details=@{ command=$e2eCmd; outputs_dir=$e2eOut } }
    }

    # Gate: contract_scored_staleness
    if ($SkipE2E) {
        Add-GateResult @{ name='contract_scored_staleness'; status='skipped'; details=@{ reason='requires e2e_offline gate' } }
    } else {
        $contractRes = Assert-RunStalenessContract -RunDirectory $e2eOut
        if ($contractRes.ExitCode -ne 0) {
            Add-GateResult @{ name='contract_scored_staleness'; status='fail'; details=@{ run_dir=$e2eOut } }
            throw "Staleness contract failed (see $($contractRes.Log))"
        }
        Add-GateResult @{ name='contract_scored_staleness'; status='pass'; details=@{ run_dir=$e2eOut } }
    }

    # Gate: gui_smoke
    if ($SkipGuiSmoke -or -not $IsWindows -or $env:GITHUB_ACTIONS -eq 'true') {
        $skipReason = if ($env:GITHUB_ACTIONS -eq 'true') { 'skipped: MAUI WinUI requires desktop session' } elseif (-not $IsWindows) { 'skipped: non-Windows platform' } else { 'skipped by flag' }
        Add-GateResult @{ name='gui_smoke'; status='skipped'; details=@{ reason=$skipReason } }
    } else {
        $guiCmd = 'dotnet run --project src/gui/MarketApp.Gui/MarketApp.Gui.csproj --no-build -- --smoke'
        $guiRes = Invoke-LoggedCommand -Name 'gui_smoke' -Command $guiCmd
        if ($guiRes.ExitCode -ne 0) {
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ command=$guiCmd } }
            throw "GUI smoke failed (see $($guiRes.Log))"
        }
        Add-GateResult @{ name='gui_smoke'; status='pass'; details=@{ command=$guiCmd } }
    }

    # Gate: sbom
    if ($SkipSbom) {
        Add-GateResult @{ name='sbom'; status='skipped'; artifacts=@(); details=@{ reason='skipped by flag' } }
    } else {
        $sbomArtifacts = @()

        $pySbomCmd = "python -m pip install cyclonedx-bom && cyclonedx-py environment --output-format JSON --output-file '$sbomRoot/python.cdx.json'"
        $pySbomRes = Invoke-LoggedCommand -Name 'sbom_python' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command $pySbomCmd
        if ($pySbomRes.ExitCode -eq 0 -and (Test-Path -LiteralPath (Join-Path $sbomRoot 'python.cdx.json'))) {
            $sbomArtifacts += 'audit/sbom/python.cdx.json'
        }

        $dotnetSbomCmd = "dotnet tool install --global CycloneDX --ignore-failed-sources; dotnet-CycloneDX src/gui/MarketApp.Gui.sln -o '$sbomRoot' -j"
        $dotnetSbomRes = Invoke-LoggedCommand -Name 'sbom_dotnet' -Command $dotnetSbomCmd
        foreach ($candidate in @('bom.json','sbom.cdx.json')) {
            $candidatePath = Join-Path $sbomRoot $candidate
            if (Test-Path -LiteralPath $candidatePath) {
                Copy-Item -LiteralPath $candidatePath -Destination (Join-Path $sbomRoot 'dotnet.cdx.json') -Force
                $sbomArtifacts += 'audit/sbom/dotnet.cdx.json'
                break
            }
        }

        if ($sbomArtifacts.Count -eq 2) {
            Add-GateResult @{ name='sbom'; status='pass'; artifacts=$sbomArtifacts; details=@{ format='CycloneDX' } }
        } else {
            Add-GateResult @{ name='sbom'; status='fail'; artifacts=$sbomArtifacts; details=@{ reason='missing python and/or dotnet sbom output' } }
            throw 'SBOM generation failed. See audit/logs/sbom_python.log and audit/logs/sbom_dotnet.log.'
        }
    }

    # Gate: pip_audit
    if ($SkipPipAudit) {
        Add-GateResult @{ name='pip_audit'; status='skipped'; details=@{ reason='skipped by flag' } }
    } else {
        $pipAuditCmd = 'python -m pip install pip-audit && pip-audit --progress-spinner off --strict'
        $auditRes = Invoke-LoggedCommand -Name 'pip_audit' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command $pipAuditCmd
        if ($auditRes.ExitCode -ne 0) {
            Add-GateResult @{ name='pip_audit'; status='fail'; details=@{} }
            throw "pip-audit failed (see $($auditRes.Log))"
        }
        Add-GateResult @{ name='pip_audit'; status='pass'; details=@{} }
    }

    $report.overall_status = 'pass'
    Save-Report
    Write-Host "release_verify completed: PASS"
    exit 0
}
catch {
    $report.overall_status = 'fail'
    Save-Report
    Write-Error $_
    Write-Host "release_verify completed: FAIL (see audit/verify_report.json and audit/logs/)"
    exit 1
}
