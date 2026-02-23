#!/usr/bin/env pwsh
param(
    [switch]$SkipDotnetTests,
    [switch]$SkipGuiSmoke,
    [switch]$SkipE2E,
    [switch]$SkipSbom
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot

$auditRoot = Join-Path $repoRoot 'audit'
$logsRoot  = Join-Path $auditRoot 'logs'
New-Item -ItemType Directory -Force -Path $auditRoot | Out-Null
New-Item -ItemType Directory -Force -Path $logsRoot  | Out-Null

$gitCommit = (git rev-parse --short HEAD).Trim()
$gitBranch = (git rev-parse --abbrev-ref HEAD).Trim()
$gitDirty  = [bool](git status --porcelain)
$runId     = "$(Get-Date -Format 'yyyyMMdd-HHmmss')-$gitCommit"

$report = [ordered]@{
    schema_version  = 1
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

function Resolve-GlobFiles {
    param([string]$Pattern)
    $allFiles = git ls-files
    $regex = '^' + [regex]::Escape($Pattern).
        Replace('\*\*', '.*').
        Replace('\*', '[^/\\]*').
        Replace('\?', '.') + '$'
    return @($allFiles | Where-Object { $_ -match $regex })
}

function Get-PropValue {
    param($Obj, [string]$Name, $Default = $null)
    $prop = $Obj.PSObject.Properties[$Name]
    if ($null -ne $prop) { return $prop.Value }
    return $Default
}

function ConvertTo-PSObject {
    param($Obj)
    if ($Obj -is [hashtable]) {
        $h = @{}
        foreach ($k in $Obj.Keys) { $h[$k] = ConvertTo-PSObject $Obj[$k] }
        return [pscustomobject]$h
    }
    if ($Obj -is [System.Collections.IEnumerable] -and -not ($Obj -is [string])) {
        $arr = @()
        foreach ($x in $Obj) { $arr += ,(ConvertTo-PSObject $x) }
        return $arr
    }
    return $Obj
}

function Parse-SimpleYamlScalar {
    param([string]$Text)
    $t = $Text.Trim()

    if ($t -match '^"(.*)"$') { return $Matches[1] }
    if ($t -match "^'(.*)'$") { return $Matches[1] }

    if ($t -eq 'null' -or $t -eq '~') { return $null }
    if ($t -eq 'true')  { return $true }
    if ($t -eq 'false') { return $false }

    if ($t -match '^-?\d+$') { return [int]$t }

    return $t
}

function Get-NextMeaningfulLine {
    param([string[]]$Lines, [int]$StartIndex)
    for ($i = $StartIndex; $i -lt $Lines.Count; $i++) {
        $l = $Lines[$i].TrimEnd()
        $trim = $l.Trim()
        if ($trim.Length -eq 0) { continue }
        if ($trim.StartsWith('#')) { continue }
        return @{ Index = $i; Line = $l }
    }
    return $null
}

function ConvertFrom-SimpleYaml {
    param([string]$YamlText)

    $lines = ($YamlText -replace "`r", '') -split "`n"

    $root = @{}
    $stack = New-Object System.Collections.Generic.List[object]
    $stack.Add([pscustomobject]@{ Indent = 0; Kind = 'map'; Obj = $root })

    for ($i = 0; $i -lt $lines.Count; $i++) {
        $raw = $lines[$i].TrimEnd()
        $trim = $raw.Trim()
        if ($trim.Length -eq 0) { continue }
        if ($trim.StartsWith('#')) { continue }

        $indent = ($raw -match '^(\s*)') ? $Matches[1].Length : 0
        if (($indent % 2) -ne 0) { throw "Unsupported YAML indentation (must be multiples of 2): line $($i+1)" }

        while ($stack.Count -gt 0 -and $indent -lt $stack[$stack.Count-1].Indent) {
            $stack.RemoveAt($stack.Count-1)
        }
        if ($stack.Count -eq 0) { throw "YAML parse error near line $($i+1)" }

        $cur = $stack[$stack.Count-1]

        # List item
        if ($raw.TrimStart().StartsWith('- ')) {
            if ($cur.Kind -ne 'list' -or $indent -ne $cur.Indent) {
                throw "YAML parse error: list item at unexpected indent near line $($i+1)"
            }

            $rest = $raw.TrimStart().Substring(2).Trim()

            # If list item starts a map inline: "- key: value"
            if ($rest -match '^([A-Za-z0-9_\-]+):\s*(.*)$') {
                $itemMap = @{}
                $k = $Matches[1]
                $v = $Matches[2]

                if ($v -eq '') {
                    $next = Get-NextMeaningfulLine -Lines $lines -StartIndex ($i + 1)
                    $newKind = if ($null -ne $next -and $next.Line.TrimStart().StartsWith('-')) { 'list' } else { 'map' }
                    $newObj  = if ($newKind -eq 'list') { @() } else { @{} }
                    $itemMap[$k] = $newObj
                } else {
                    $itemMap[$k] = Parse-SimpleYamlScalar $v
                }

                $cur.Obj += ,$itemMap

                # Allow subsequent indented lines to add more keys to this map
                $stack.Add([pscustomobject]@{ Indent = $indent + 2; Kind = 'map'; Obj = $itemMap })
                continue
            }

            # Scalar list item
            $cur.Obj += ,(Parse-SimpleYamlScalar $rest)
            continue
        }

        # Key/value in map
        if ($cur.Kind -ne 'map' -or $indent -ne $cur.Indent) {
            throw "YAML parse error: mapping at unexpected indent near line $($i+1)"
        }

        if ($raw -notmatch '^\s*([A-Za-z0-9_\-]+):\s*(.*)$') {
            throw "Unsupported YAML line near $($i+1): $raw"
        }

        $key = $Matches[1]
        $val = $Matches[2]

        if ($val -eq '') {
            $next = Get-NextMeaningfulLine -Lines $lines -StartIndex ($i + 1)
            $newKind = if ($null -ne $next -and $next.Line.TrimStart().StartsWith('-')) { 'list' } else { 'map' }
            $newObj  = if ($newKind -eq 'list') { @() } else { @{} }

            $cur.Obj[$key] = $newObj
            $stack.Add([pscustomobject]@{ Indent = $indent + 2; Kind = $newKind; Obj = $newObj })
        }
        else {
            $cur.Obj[$key] = Parse-SimpleYamlScalar $val
        }
    }

    return (ConvertTo-PSObject $root)
}

function Load-Manifest {
    param([string]$Path)

    $raw = Get-Content -Raw -Path $Path

    # Prefer ConvertFrom-Yaml if present, but it is not native in many environments.
    if (Get-Command ConvertFrom-Yaml -ErrorAction SilentlyContinue) {
        return ($raw | ConvertFrom-Yaml)
    }

    # Fallback: internal minimal YAML parser sufficient for docs/runtime_required_files.yaml schema.
    return (ConvertFrom-SimpleYaml $raw)
}

function Test-ManifestItem {
    param($Item)

    $kind = Get-PropValue -Obj $Item -Name 'kind'
    switch ($kind) {
        'file' {
            return [System.IO.File]::Exists((Join-Path $repoRoot (Get-PropValue -Obj $Item -Name 'path')))
        }
        'dir' {
            return [System.IO.Directory]::Exists((Join-Path $repoRoot (Get-PropValue -Obj $Item -Name 'path')))
        }
        'glob' {
            $files = Resolve-GlobFiles -Pattern (Get-PropValue -Obj $Item -Name 'glob')
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

try {
    # Gate: inventory
    $trackedPath = Join-Path $auditRoot 'file_inventory.tracked.txt'
    $shaPath     = Join-Path $auditRoot 'file_inventory.sha256.tsv'
    $files       = @(git ls-files | Sort-Object)
    $files | Set-Content -Path $trackedPath -Encoding UTF8

    $shaRows = foreach ($path in $files) {
        $full = Join-Path $repoRoot $path
        if (Test-Path -LiteralPath $full -PathType Leaf) {
            $hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $full).Hash.ToLowerInvariant()
            "$hash`t$path"
        }
    }
    $shaRows | Set-Content -Path $shaPath -Encoding UTF8
    Add-GateResult @{
        name      = 'inventory'
        status    = 'pass'
        artifacts = @('audit/file_inventory.tracked.txt','audit/file_inventory.sha256.tsv')
        details   = @{}
    }

    # Gate: runtime_manifest
    $manifestPath = Join-Path $repoRoot 'docs/runtime_required_files.yaml'
    if (-not (Test-Path -LiteralPath $manifestPath)) { throw "Manifest not found: $manifestPath" }

    $manifest = Load-Manifest -Path $manifestPath

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
    $pytestStatus     = 'skipped'
    $dotnetTestStatus = 'skipped'

    if (Test-Path -LiteralPath (Join-Path $repoRoot 'market_app/pyproject.toml')) {
        $pyResult = Invoke-LoggedCommand -Name 'pytest' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command 'python -m pytest -q'
        if ($pyResult.ExitCode -ne 0) {
            $pytestStatus = 'fail'
            Add-GateResult @{ name='tests_engine'; status='fail'; details=@{ pytest=$pytestStatus; dotnet_test=$dotnetTestStatus } }
            throw "pytest failed (see $($pyResult.Log))"
        }
        $pytestStatus = 'pass'
    }

    $hasSolution = Test-Path -LiteralPath (Join-Path $repoRoot 'src/gui/MarketApp.Gui.sln')
    if ($hasSolution -and -not $SkipDotnetTests -and $IsWindows) {
        $dnResult = Invoke-LoggedCommand -Name 'dotnet_test' -Command 'dotnet test src/gui/MarketApp.Gui.Tests/MarketApp.Gui.Tests.csproj -c Release'
        if ($dnResult.ExitCode -ne 0) {
            $dotnetTestStatus = 'fail'
            Add-GateResult @{ name='tests_engine'; status='fail'; details=@{ pytest=$pytestStatus; dotnet_test=$dotnetTestStatus } }
            throw "dotnet test failed (see $($dnResult.Log))"
        }
        $dotnetTestStatus = 'pass'
    }
    $testsEngineStatus = if ($pytestStatus -eq 'skipped' -and $dotnetTestStatus -eq 'skipped') { 'skipped' } else { 'pass' }
    Add-GateResult @{ name='tests_engine'; status=$testsEngineStatus; details=@{ pytest=$pytestStatus; dotnet_test=$dotnetTestStatus } }

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
        if (Test-Path -LiteralPath $readyFile) { Remove-Item -LiteralPath $readyFile -Force }

        $env:MARKETAPP_SMOKE_READY_FILE   = $readyFile
        $env:MARKETAPP_SMOKE_HOLD_SECONDS = '15'
        $env:MARKETAPP_OFFLINE            = '1'

        # IMPORTANT: Start-Process errors if stdout/stderr are redirected to the same file.
        $guiOutLog = Join-Path $logsRoot 'gui_smoke.out.log'
        $guiErrLog = Join-Path $logsRoot 'gui_smoke.err.log'

        $proc = Start-Process dotnet -ArgumentList @('run','--project',$guiProj.FullName,'--','--smoke') `
            -PassThru -NoNewWindow `
            -RedirectStandardOutput $guiOutLog `
            -RedirectStandardError  $guiErrLog

        $deadline = (Get-Date).AddSeconds(60)
        while ((Get-Date) -lt $deadline -and -not (Test-Path -LiteralPath $readyFile)) {
            Start-Sleep -Milliseconds 500
        }

        if (-not (Test-Path -LiteralPath $readyFile)) {
            if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force }
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ ready_file=$readyFile; hold_seconds=15; exit_code=1; stdout_log=$guiOutLog; stderr_log=$guiErrLog } }
            throw 'GUI smoke failed: READY file was not created within timeout.'
        }

        Start-Sleep -Seconds 15
        if (-not $proc.HasExited) {
            $proc.WaitForExit(30 * 1000) | Out-Null
        }

        $exitCode = if ($proc.HasExited) { $proc.ExitCode } else { -1 }

        if ($exitCode -ne 0) {
            if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force }
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ ready_file=$readyFile; hold_seconds=15; exit_code=$exitCode; stdout_log=$guiOutLog; stderr_log=$guiErrLog } }
            throw "GUI smoke failed with exit code $exitCode"
        }

        Add-GateResult @{ name='gui_smoke'; status='pass'; details=@{ ready_file=$readyFile; hold_seconds=15; exit_code=$exitCode; stdout_log=$guiOutLog; stderr_log=$guiErrLog } }
    }

    # Gate: sbom
    if ($SkipSbom) {
        Add-GateResult @{ name='sbom'; status='skipped'; artifacts=@(); details=@{ tool='cyclonedx-dotnet'; format='CycloneDX'; reason='skipped by flag' } }
    } else {
        $cyclone = Get-Command 'dotnet-cyclonedx' -ErrorAction SilentlyContinue
        if (-not $cyclone) { $cyclone = Get-Command 'cyclonedx' -ErrorAction SilentlyContinue }

        if ($cyclone -and (Test-Path -LiteralPath (Join-Path $repoRoot 'src/gui/MarketApp.Gui.sln'))) {
            $sbomCmd = "dotnet-cyclonedx src/gui/MarketApp.Gui.sln -o '$auditRoot' -j"
            $sbomRes = Invoke-LoggedCommand -Name 'sbom' -Command $sbomCmd
            if ($sbomRes.ExitCode -eq 0) {
                # dotnet-cyclonedx default output name may be bom.json or sbom.cdx.json; discover it
                $sbomFile = $null
                foreach ($candidate in @('sbom.cdx.json','bom.json')) {
                    $candidatePath = Join-Path $auditRoot $candidate
                    if (Test-Path -LiteralPath $candidatePath) { $sbomFile = $candidatePath; break }
                }
                if ($sbomFile) {
                    $sbomArtifactPath = [System.IO.Path]::GetRelativePath($repoRoot, $sbomFile) -replace '\\','/'
                    Add-GateResult @{ name='sbom'; status='pass'; artifacts=@($sbomArtifactPath); details=@{ tool='cyclonedx-dotnet'; format='CycloneDX' } }
                } else {
                    Add-GateResult @{ name='sbom'; status='skipped'; artifacts=@(); details=@{ tool='cyclonedx-dotnet'; format='CycloneDX'; reason='sbom file missing' } }
                }
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
    $report.overall_status = 'fail'
    Save-Report
    Write-Error $_
    exit 1
}
