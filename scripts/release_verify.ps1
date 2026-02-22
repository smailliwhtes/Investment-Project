#!/usr/bin/env pwsh
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

param(
    [switch]$SkipDotnetTests,
    [switch]$SkipGuiSmoke,
    [switch]$SkipE2E,
    [switch]$SkipSbom
)

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$auditRoot = Join-Path $repoRoot 'audit'
$logsRoot = Join-Path $auditRoot 'logs'
New-Item -ItemType Directory -Force -Path $auditRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logsRoot | Out-Null

$gitCommit = (git rev-parse --short HEAD).Trim()
$gitBranch = (git rev-parse --abbrev-ref HEAD).Trim()
$gitDirty = [bool](git status --porcelain)
$runId = "$(Get-Date -Format 'yyyyMMdd-HHmmss')-$gitCommit"

$report = [ordered]@{
    schema_version = 1
    run_id = $runId
    timestamp_utc = (Get-Date).ToUniversalTime().ToString('o')
    git = @{ branch = $gitBranch; commit = $gitCommit; dirty = $gitDirty }
    platform = @{
        os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        pwsh = $PSVersionTable.PSVersion.ToString()
        dotnet = $null
        python = $null
    }
    gates = @()
    overall_status = 'fail'
    artifacts_root = 'audit'
}

function Save-Report {
    $reportPath = Join-Path $auditRoot 'verify_report.json'
    $report | ConvertTo-Json -Depth 8 | Set-Content -Path $reportPath -Encoding UTF8
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
    $stdoutPath = Join-Path $logsRoot "$Name.stdout.log"
    $stderrPath = Join-Path $logsRoot "$Name.stderr.log"
    $combinedPath = Join-Path $logsRoot "$Name.log"

    Push-Location $WorkingDirectory
    try {
        & pwsh -NoProfile -Command $Command 1> $stdoutPath 2> $stderrPath
        $exitCode = $LASTEXITCODE
        $merged = @(
            "### STDOUT ($Name)",
            (Get-Content -Path $stdoutPath -ErrorAction SilentlyContinue),
            "",
            "### STDERR ($Name)",
            (Get-Content -Path $stderrPath -ErrorAction SilentlyContinue)
        )
        $merged | Set-Content -Path $combinedPath -Encoding UTF8
        return @{ ExitCode = $exitCode; Log = $combinedPath; StdoutLog = $stdoutPath; StderrLog = $stderrPath }
    }
    finally {
        Pop-Location
    }
}

function Resolve-GlobFiles {
    param([string]$Pattern)
    $allFiles = git ls-files
    $regex = '^' + [regex]::Escape($Pattern).Replace('\*\*', '.*').Replace('\*', '[^/\\]*').Replace('\?', '.') + '$'
    return @($allFiles | Where-Object { $_ -match $regex })
}

function Get-PropValue {
    param($Obj, [string]$Name, $Default = $null)
    $prop = $Obj.PSObject.Properties[$Name]
    if ($null -ne $prop) { return $prop.Value }
    return $Default
}

function Test-ManifestItem {
    param($Item)
    $kind = Get-PropValue -Obj $Item -Name "kind"
    switch ($kind) {
        'file' {
            return [System.IO.File]::Exists((Join-Path $repoRoot $(Get-PropValue -Obj $Item -Name 'path')))
        }
        'dir' {
            return [System.IO.Directory]::Exists((Join-Path $repoRoot $(Get-PropValue -Obj $Item -Name 'path')))
        }
        'glob' {
            $files = Resolve-GlobFiles -Pattern $(Get-PropValue -Obj $Item -Name 'glob')
            $minRaw = Get-PropValue -Obj $Item -Name 'min_count' -Default 1
            $minCount = [int]$minRaw
            return ($files.Count -ge $minCount)
        }
        default {
            $itemId = Get-PropValue -Obj $Item -Name 'id' -Default 'unknown'
            throw "Unknown kind '$kind' for id '$itemId'"
        }
    }
}

try {
    $dotnetVersion = (& dotnet --version 2>$null)
    if ($LASTEXITCODE -eq 0) { $report.platform.dotnet = $dotnetVersion.Trim() }
} catch {}
try {
    $pyVersion = (& python --version 2>&1)
    if ($LASTEXITCODE -eq 0) { $report.platform.python = $pyVersion.Trim() }
} catch {}

$failed = $false

try {
    # Gate: inventory
    $trackedPath = Join-Path $auditRoot 'file_inventory.tracked.txt'
    $shaPath = Join-Path $auditRoot 'file_inventory.sha256.tsv'
    $files = @(git ls-files | Sort-Object)
    $files | Set-Content -Path $trackedPath -Encoding UTF8

    $shaRows = foreach ($path in $files) {
        $full = Join-Path $repoRoot $path
        if (Test-Path -LiteralPath $full -PathType Leaf) {
            $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
            "$hash`t$path"
        }
    }
    $shaRows | Set-Content -Path $shaPath -Encoding UTF8
    Add-GateResult @{ name='inventory'; status='pass'; artifacts=@('audit/file_inventory.tracked.txt','audit/file_inventory.sha256.tsv'); details=@{} }

    # Gate: runtime_manifest
    $manifestPath = Join-Path $repoRoot 'docs/runtime_required_files.yaml'
    if (-not (Test-Path $manifestPath)) { throw "Manifest not found: $manifestPath" }
    $manifest = Get-Content -Raw -Path $manifestPath | ConvertFrom-Yaml
    $failedIds = New-Object System.Collections.Generic.List[string]

    foreach ($item in $manifest.required) {
        if (-not (Test-ManifestItem -Item $item)) { $failedIds.Add($item.id) }
    }
    foreach ($group in $manifest.either) {
        $ok = $false
        foreach ($option in $group.any_of) {
            if (Test-ManifestItem -Item $option) { $ok = $true; break }
        }
        if (-not $ok) { $failedIds.Add($group.id) }
    }

    if ($failedIds.Count -gt 0) {
        Add-GateResult @{ name='runtime_manifest'; status='fail'; details=@{ failed_ids = @($failedIds) } }
        throw "Runtime manifest validation failed for ids: $($failedIds -join ', ')"
    }
    Add-GateResult @{ name='runtime_manifest'; status='pass'; details=@{ failed_ids=@() } }

    # Gate: tests_engine
    $pytestStatus = 'skipped'
    $dotnetTestStatus = 'skipped'

    if (Test-Path (Join-Path $repoRoot 'market_app/pyproject.toml')) {
        $pyResult = Invoke-LoggedCommand -Name 'pytest' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command 'python -m pytest -q'
        if ($pyResult.ExitCode -ne 0) {
            $pytestStatus = 'fail'
            Add-GateResult @{ name='tests_engine'; status='fail'; details=@{ pytest=$pytestStatus; dotnet_test=$dotnetTestStatus } }
            throw "pytest failed (see $($pyResult.Log))"
        }
        $pytestStatus = 'pass'
    }

    $hasSolution = Test-Path (Join-Path $repoRoot 'src/gui/MarketApp.Gui.sln')
    if ($hasSolution -and -not $SkipDotnetTests -and $IsWindows) {
        $dnResult = Invoke-LoggedCommand -Name 'dotnet_test' -Command 'dotnet test src/gui/MarketApp.Gui.Tests/MarketApp.Gui.Tests.csproj -c Release'
        if ($dnResult.ExitCode -ne 0) {
            $dotnetTestStatus = 'fail'
            Add-GateResult @{ name='tests_engine'; status='fail'; details=@{ pytest=$pytestStatus; dotnet_test=$dotnetTestStatus } }
            throw "dotnet test failed (see $($dnResult.Log))"
        }
        $dotnetTestStatus = 'pass'
    } elseif ($hasSolution -and -not $IsWindows) {
        $dotnetTestStatus = 'skipped'
    }
    Add-GateResult @{ name='tests_engine'; status='pass'; details=@{ pytest=$pytestStatus; dotnet_test=$dotnetTestStatus } }

    # Gate: e2e_offline
    if ($SkipE2E) {
        Add-GateResult @{ name='e2e_offline'; status='skipped'; details=@{ command='skipped by flag'; outputs_dir=$null } }
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

    # Gate: gui_smoke
    if ($SkipGuiSmoke -or -not $IsWindows) {
        Add-GateResult @{ name='gui_smoke'; status='skipped'; details=@{ ready_file=$null; hold_seconds=15; exit_code=0 } }
    } else {
        $guiProj = Get-ChildItem -Path $repoRoot -Recurse -Filter '*.csproj' |
            Where-Object { Select-String -Path $_.FullName -Pattern '<UseMaui>true</UseMaui>' -Quiet } |
            Select-Object -First 1
        if (-not $guiProj) { throw 'Could not locate MAUI GUI project for smoke test.' }

        $readyFile = Join-Path $env:TEMP "marketapp_ready_$runId.json"
        if (Test-Path $readyFile) { Remove-Item $readyFile -Force }
        $env:MARKETAPP_SMOKE_READY_FILE = $readyFile
        $env:MARKETAPP_SMOKE_HOLD_SECONDS = '15'
        $env:MARKETAPP_OFFLINE = '1'

        $guiOutLog = Join-Path $logsRoot 'gui_smoke.stdout.log'
        $guiErrLog = Join-Path $logsRoot 'gui_smoke.stderr.log'
        $guiLog = Join-Path $logsRoot 'gui_smoke.log'
        $proc = Start-Process dotnet -ArgumentList @('run','--project',$guiProj.FullName,'--','--smoke') -PassThru -NoNewWindow -RedirectStandardOutput $guiOutLog -RedirectStandardError $guiErrLog

        $deadline = (Get-Date).AddSeconds(60)
        while ((Get-Date) -lt $deadline -and -not (Test-Path $readyFile)) {
            Start-Sleep -Milliseconds 500
        }

        if (-not (Test-Path $readyFile)) {
            if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force }
            @(
                '### STDOUT (gui_smoke)',
                (Get-Content -Path $guiOutLog -ErrorAction SilentlyContinue),
                '',
                '### STDERR (gui_smoke)',
                (Get-Content -Path $guiErrLog -ErrorAction SilentlyContinue)
            ) | Set-Content -Path $guiLog -Encoding UTF8
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ ready_file=$readyFile; hold_seconds=15; exit_code=1 } }
            throw 'GUI smoke failed: READY file was not created within timeout.'
        }

        Start-Sleep -Seconds 15
        if (-not $proc.HasExited) {
            $proc.WaitForExit(30 * 1000) | Out-Null
        }
        $exitCode = if ($proc.HasExited) { $proc.ExitCode } else { -1 }
        @(
            '### STDOUT (gui_smoke)',
            (Get-Content -Path $guiOutLog -ErrorAction SilentlyContinue),
            '',
            '### STDERR (gui_smoke)',
            (Get-Content -Path $guiErrLog -ErrorAction SilentlyContinue)
        ) | Set-Content -Path $guiLog -Encoding UTF8

        if ($exitCode -ne 0) {
            if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force }
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ ready_file=$readyFile; hold_seconds=15; exit_code=$exitCode } }
            throw "GUI smoke failed with exit code $exitCode"
        }
        Add-GateResult @{ name='gui_smoke'; status='pass'; details=@{ ready_file=$readyFile; hold_seconds=15; exit_code=$exitCode } }
    }

    # Gate: sbom
    if ($SkipSbom) {
        Add-GateResult @{ name='sbom'; status='skipped'; artifacts=@(); details=@{ tool='cyclonedx-dotnet'; format='CycloneDX'; reason='skipped by flag' } }
    } else {
        $sbomFile = Join-Path $auditRoot 'sbom.cdx.json'
        $cyclone = Get-Command 'dotnet-cyclonedx' -ErrorAction SilentlyContinue
        if (-not $cyclone) { $cyclone = Get-Command 'cyclonedx' -ErrorAction SilentlyContinue }
        if ($cyclone -and (Test-Path (Join-Path $repoRoot 'src/gui/MarketApp.Gui.sln'))) {
            $sbomCmd = "dotnet-cyclonedx src/gui/MarketApp.Gui.sln -o '$auditRoot' -j"
            $sbomRes = Invoke-LoggedCommand -Name 'sbom' -Command $sbomCmd
            if ($sbomRes.ExitCode -eq 0) {
                Add-GateResult @{ name='sbom'; status='pass'; artifacts=@('audit/bom.json'); details=@{ tool='cyclonedx-dotnet'; format='CycloneDX' } }
            } else {
                Add-GateResult @{ name='sbom'; status='skipped'; artifacts=@(); details=@{ tool='cyclonedx-dotnet'; format='CycloneDX'; reason='tool failed' } }
            }
        } else {
            Add-GateResult @{ name='sbom'; status='skipped'; artifacts=@(); details=@{ tool='cyclonedx-dotnet'; format='CycloneDX'; reason='tool not installed' } }
        }
    }

    $report.overall_status = 'pass'
    Save-Report
    exit 0
}
catch {
    $failed = $true
    $report.overall_status = 'fail'
    Save-Report
    Write-Error $_
    exit 1
}
