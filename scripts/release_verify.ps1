#!/usr/bin/env pwsh
param(
    [switch]$SkipDotnetTests,
    [switch]$SkipGuiSmoke,
    [switch]$SkipE2E,
    [switch]$SkipSbom,
    [switch]$SkipPipAudit
)

if ($PSVersionTable.PSEdition -ne 'Core') {
    $pwsh = Get-Command pwsh -ErrorAction SilentlyContinue
    if ($null -eq $pwsh) {
        throw 'PowerShell 7 (pwsh) is required to run scripts/release_verify.ps1.'
    }

    & $pwsh.Source -NoProfile -ExecutionPolicy Bypass -File $PSCommandPath @args
    exit $LASTEXITCODE
}
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$isWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)

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

function Get-MissingSbomArtifacts {
    param(
        [string]$PythonSbomPath,
        [string]$DotnetSbomPath
    )

    $missing = @()
    if (-not (Test-Path -LiteralPath $PythonSbomPath)) { $missing += 'audit/sbom/python.cdx.json' }
    if (-not (Test-Path -LiteralPath $DotnetSbomPath)) { $missing += 'audit/sbom/dotnet.cdx.json' }
    return $missing
}

function Resolve-PythonCommand {
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($null -ne $python) {
        try {
            & python --version *> $null
            if ($LASTEXITCODE -eq 0) { return 'python' }
        } catch {}
    }

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($null -ne $pyLauncher) {
        try {
            & py -3 --version *> $null
            if ($LASTEXITCODE -eq 0) { return 'py -3' }
        } catch {}
    }

    $knownPython = Join-Path $env:LOCALAPPDATA 'Programs\Python\Python312\python.exe'
    if (Test-Path -LiteralPath $knownPython) { return "& '$knownPython'" }

    throw 'Python interpreter not found. Install Python 3.x or add python/py to PATH.'
}
function Assert-RunStalenessContract {
    param([string]$RunDirectory)

    $cmd = "$script:pythonCmd ../scripts/check_staleness_contract.py --run-dir '$RunDirectory'"
    return (Invoke-LoggedCommand -Name 'contract_staleness' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command $cmd)
}

$pythonCmd = Resolve-PythonCommand

try {
    try {
        $dotnetVersion = (& dotnet --version 2>$null)
        if ($LASTEXITCODE -eq 0) { $report.platform.dotnet = $dotnetVersion.Trim() }
    } catch {}

    try {
        $pyVersionRes = Invoke-LoggedCommand -Name 'python_version' -Command "$pythonCmd --version"
        if ($pyVersionRes.ExitCode -eq 0 -and (Test-Path -LiteralPath $pyVersionRes.Log)) {
            $pyVersionLine = (Get-Content -Path $pyVersionRes.Log -TotalCount 1).Trim()
            if ($pyVersionLine) { $report.platform.python = $pyVersionLine }
        }
    } catch {}

    # Gate: tests_engine
    $pyResult = Invoke-LoggedCommand -Name 'pytest' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command "$pythonCmd -m pytest -q"
    if ($pyResult.ExitCode -ne 0) {
        Add-GateResult @{ name='tests_engine'; status='fail'; details=@{ pytest='fail'; dotnet_test='skipped' } }
        throw "pytest failed (see $($pyResult.Log))"
    }

    $dotnetStatus = 'skipped'
    if (-not $SkipDotnetTests -and $isWindowsHost -and (Test-Path -LiteralPath (Join-Path $repoRoot 'src/gui/MarketApp.Gui.sln'))) {
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
        $runsRoot = Join-Path $auditRoot 'runs'
        $e2eOut = Join-Path $runsRoot $runId
        New-Item -ItemType Directory -Path $e2eOut -Force | Out-Null

        $e2eCmd = "$pythonCmd -m market_app.cli run --config tests/data/mini_dataset/config.yaml --output-dir '$runsRoot' --run-id '$runId' --offline --as-of-date 2025-01-31"
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
            if (Test-Path -LiteralPath $contractRes.Log) {
                Write-Host "--- contract_staleness.log (first 200 lines) ---"
                Get-Content -Path $contractRes.Log -TotalCount 200 | ForEach-Object { Write-Host $_ }
                Write-Host "--- end contract_staleness.log ---"
            }
            throw "Staleness contract failed (see $($contractRes.Log))"
        }
        Add-GateResult @{ name='contract_scored_staleness'; status='pass'; details=@{ run_dir=$e2eOut } }
    }

    # Gate: gui_smoke
    if ($SkipGuiSmoke -or -not $isWindowsHost -or $env:GITHUB_ACTIONS -eq 'true') {
        $skipReason = if ($env:GITHUB_ACTIONS -eq 'true') { 'skipped: MAUI WinUI requires desktop session' } elseif (-not $isWindowsHost) { 'skipped: non-Windows platform' } else { 'skipped by flag' }
        Add-GateResult @{ name='gui_smoke'; status='skipped'; details=@{ reason=$skipReason } }
    } else {
        $guiBuildCmd = 'dotnet build src/gui/MarketApp.Gui/MarketApp.Gui.csproj -c Release -p:Platform=x64 --no-restore'
        $guiBuildRes = Invoke-LoggedCommand -Name 'gui_smoke_build' -Command $guiBuildCmd
        if ($guiBuildRes.ExitCode -ne 0) {
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ step='build'; command=$guiBuildCmd } }
            throw "GUI smoke build failed (see $($guiBuildRes.Log))"
        }

        $guiExeCandidates = @(
            (Join-Path $repoRoot 'src/gui/MarketApp.Gui/bin/x64/Release/net8.0-windows10.0.19041.0/win10-x64/MarketApp.Gui.exe'),
            (Join-Path $repoRoot 'src/gui/MarketApp.Gui/bin/Release/net8.0-windows10.0.19041.0/win10-x64/MarketApp.Gui.exe')
        )
        $guiExe = $guiExeCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
        if (-not $guiExe) {
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ step='locate_exe'; candidates=$guiExeCandidates } }
            throw 'GUI smoke failed: built executable not found in expected output paths.'
        }

        $guiCmd = @"
`$proc = Start-Process -FilePath '$guiExe' -PassThru
Start-Sleep -Seconds 5
`$proc.Refresh()
if (`$proc.HasExited) {
    Write-Output "GuiExitedEarly=`$(`$proc.ExitCode)"
    exit 1
}
Stop-Process -Id `$proc.Id -Force
Write-Output 'GuiLaunchAliveThenStopped'
exit 0
"@
        $guiRes = Invoke-LoggedCommand -Name 'gui_smoke' -Command $guiCmd
        if ($guiRes.ExitCode -ne 0) {
            Add-GateResult @{ name='gui_smoke'; status='fail'; details=@{ step='run'; command=$guiCmd; exe=$guiExe } }
            throw "GUI smoke failed (see $($guiRes.Log))"
        }
        Add-GateResult @{ name='gui_smoke'; status='pass'; details=@{ command='Start-Process liveness check (5s)'; exe=$guiExe } }
    }
    # Gate: sbom
    if ($SkipSbom) {
        Add-GateResult @{ name='sbom'; status='skipped'; artifacts=@(); details=@{ reason='skipped by flag' } }
    } else {
        $sbomArtifacts = @()
        $pythonSbomPath = Join-Path $sbomRoot 'python.cdx.json'
        $dotnetSbomPath = Join-Path $sbomRoot 'dotnet.cdx.json'
        if (Test-Path -LiteralPath $pythonSbomPath) { Remove-Item -LiteralPath $pythonSbomPath -Force }
        if (Test-Path -LiteralPath $dotnetSbomPath) { Remove-Item -LiteralPath $dotnetSbomPath -Force }

        $pySbomCmd = @"
$pythonCmd -m pip install cyclonedx-bom
$pythonCmd -c "import pathlib, tomllib; p=pathlib.Path('pyproject.toml'); deps=tomllib.loads(p.read_text(encoding='utf-8')).get('project', {}).get('dependencies', []); pathlib.Path('.sbom-requirements.txt').write_text('\\n'.join(deps)+'\\n', encoding='utf-8')"
cyclonedx-py requirements --output-format JSON --output-file '$pythonSbomPath' '.sbom-requirements.txt'
"@
$pySbomCmd = $pySbomCmd + " 2>&1 | Tee-Object -FilePath `"$logsRoot/sbom_python.log`""
        $pySbomRes = Invoke-LoggedCommand -Name 'sbom_python' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command $pySbomCmd
        $pySbomCleanup = Invoke-LoggedCommand -Name 'sbom_python_cleanup' -WorkingDirectory (Join-Path $repoRoot 'market_app') -Command "if (Test-Path -LiteralPath '.sbom-requirements.txt') { Remove-Item -LiteralPath '.sbom-requirements.txt' -Force }"
        if ($pySbomRes.ExitCode -eq 0 -and (Test-Path -LiteralPath $pythonSbomPath)) {
            $sbomArtifacts += 'audit/sbom/python.cdx.json'
        }

        $dotnetSbomCmd = @"
if (-not (Test-Path -LiteralPath '.config/dotnet-tools.json')) {
    dotnet new tool-manifest
}
dotnet tool restore --tool-manifest "$repoRoot/.config/dotnet-tools.json"
dotnet tool run dotnet-CycloneDX src/gui/MarketApp.Gui.sln -o '$sbomRoot' -j
if (Test-Path -LiteralPath '$sbomRoot/bom.json') {
    Copy-Item -LiteralPath '$sbomRoot/bom.json' -Destination '$dotnetSbomPath' -Force
} elseif (Test-Path -LiteralPath '$sbomRoot/sbom.cdx.json') {
    Copy-Item -LiteralPath '$sbomRoot/sbom.cdx.json' -Destination '$dotnetSbomPath' -Force
}
"@
$dotnetSbomCmd = $dotnetSbomCmd + " 2>&1 | Tee-Object -FilePath `"$logsRoot/sbom_dotnet.log`""
        $dotnetSbomRes = Invoke-LoggedCommand -Name 'sbom_dotnet' -Command $dotnetSbomCmd
        if ($dotnetSbomRes.ExitCode -eq 0 -and (Test-Path -LiteralPath $dotnetSbomPath)) {
            $sbomArtifacts += 'audit/sbom/dotnet.cdx.json'
        }

        $missingSboms = @(Get-MissingSbomArtifacts -PythonSbomPath $pythonSbomPath -DotnetSbomPath $dotnetSbomPath)
if (@($missingSboms).Count -eq 0) {
            Add-GateResult @{ name='sbom'; status='pass'; artifacts=$sbomArtifacts; details=@{ format='CycloneDX' } }
        } else {
            $missingList = ($missingSboms -join ', ')
            Add-GateResult @{ name='sbom'; status='fail'; artifacts=$sbomArtifacts; details=@{ missing=$missingSboms; python_log='audit/logs/sbom_python.log'; dotnet_log='audit/logs/sbom_dotnet.log' } }
            throw "SBOM generation failed. Missing output(s): $missingList. See audit/logs/sbom_python.log and audit/logs/sbom_dotnet.log."
        }
    }

    # Gate: pip_audit
    if ($SkipPipAudit) {
        Add-GateResult @{ name='pip_audit'; status='skipped'; details=@{ reason='skipped by flag' } }
    } else {
        $pipAuditCmd = "$pythonCmd -m pip install pip-audit && pip-audit --progress-spinner off -r requirements.txt"
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
    Write-Host $_
    Write-Host "release_verify completed: FAIL (see audit/verify_report.json and audit/logs/)"
    exit 1
}







