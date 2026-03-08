param(
  [switch]$Rebuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "Repo: $repoRoot"

# Prefer explicit MarketApp.Gui.csproj, else any csproj with 'Gui' in name/path
$gui = Get-ChildItem -Recurse -Filter "MarketApp.Gui.csproj" -ErrorAction SilentlyContinue |
  Select-Object -First 1 -ExpandProperty FullName

if (-not $gui) {
  $gui = Get-ChildItem -Recurse -Filter "*.csproj" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match 'MarketApp\.Gui' -or $_.Name -match 'MarketApp\.Gui' } |
    Select-Object -First 1 -ExpandProperty FullName
}

if (-not $gui) {
  $gui = Get-ChildItem -Recurse -Filter "*.csproj" -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -match '(?i)gui' } |
    Select-Object -First 1 -ExpandProperty FullName
}

if (-not $gui) { throw "GUI .csproj not found under $repoRoot" }

Write-Host "GUI project: $gui"

$configuration = "Release"
$platform = "x64"
$buildSucceeded = $false

if ($Rebuild) {
  try {
    dotnet workload restore --project "$gui" | Out-Host
    dotnet restore "$gui" | Out-Host
    dotnet build "$gui" -c $configuration -p:Platform=$platform | Out-Host
    $buildSucceeded = $true
  } catch {
    Write-Host "build warning: $($_.Exception.Message)"
    Write-Host "Continuing with most recent existing GUI executable..."
  }
} else {
  Write-Host "Skipping build (use -Rebuild to restore/build before launch)."
}

$projectDir = Split-Path -Parent $gui
$exeCandidates = Get-ChildItem -Path (Join-Path $projectDir 'bin') -Recurse -Filter 'MarketApp.Gui.exe' -ErrorAction SilentlyContinue |
  Where-Object { $_.FullName -like '*\win10-x64\*' }

if ($buildSucceeded) {
  $exe = $exeCandidates |
    Where-Object { $_.FullName -like "*\$configuration\*" -and $_.FullName -like "*\$platform\*" } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
} else {
  $exe = $exeCandidates |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
}

if (-not $exe) {
  throw "GUI executable not found under $projectDir/bin. Build once with 'dotnet build src/gui/MarketApp.Gui/MarketApp.Gui.csproj -c Release -p:Platform=x64' then rerun this script."
}

Write-Host "Launching GUI: $($exe.FullName)"

# Ensure local shell variables from smoke checks do not force an immediate auto-exit.
Remove-Item Env:MARKETAPP_SMOKE_MODE -ErrorAction SilentlyContinue
Remove-Item Env:MARKETAPP_SMOKE_READY_FILE -ErrorAction SilentlyContinue
Remove-Item Env:MARKETAPP_SMOKE_HOLD_SECONDS -ErrorAction SilentlyContinue

$proc = Start-Process -FilePath $exe.FullName -PassThru
Start-Sleep -Seconds 3
$proc.Refresh()
if ($proc.HasExited) {
  Write-Error "GUI exited during startup (exit code: $($proc.ExitCode))."

  try {
    $since = (Get-Date).AddMinutes(-2)
    $events = Get-WinEvent -FilterHashtable @{ LogName = 'Application'; StartTime = $since } |
      Where-Object { $_.Message -like '*MarketApp.Gui.exe*' } |
      Sort-Object TimeCreated -Descending |
      Select-Object -First 3

    if ($events) {
      Write-Host "Recent Application log events:"
      $events | ForEach-Object {
        Write-Host "[$($_.TimeCreated)] Provider=$($_.ProviderName) Id=$($_.Id)"
      }
    }
  } catch {
    Write-Host "Unable to read Application event logs in this session."
  }

  $winuiLog = Join-Path $env:TEMP 'marketapp_winui_startup_error.log'
  if (Test-Path -LiteralPath $winuiLog) {
    Write-Host "Recent WinUI startup errors (tail):"
    Get-Content -Path $winuiLog -Tail 30 | ForEach-Object { Write-Host $_ }
  }

  exit 1
}

# Catch the specific WinUI startup failure state where process survives but app window is broken.
$procDetails = Get-Process -Id $proc.Id -ErrorAction SilentlyContinue
if ($procDetails -and $procDetails.MainWindowTitle -like '*could not be started*') {
  Write-Error "GUI process started but WinUI failed to initialize (window title: '$($procDetails.MainWindowTitle)')."
  exit 1
}

Write-Host "GUI launched (PID $($proc.Id)). This shell is free to use while the app remains open."

