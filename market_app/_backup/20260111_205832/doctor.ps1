Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) { throw "Missing venv python: $py" }

& $py -c "import sys; print('python:', sys.version)"
if ($LASTEXITCODE -ne 0) { throw "Python version check failed." }

& $py -m pip --version
if ($LASTEXITCODE -ne 0) { throw "pip check failed." }

# Core imports used by the app. pandas.to_markdown() requires tabulate. :contentReference[oaicite:1]{index=1}
& $py -c "import pandas, numpy, sklearn, statsmodels, requests, tabulate; print('imports: OK')"
if ($LASTEXITCODE -ne 0) { throw "Core imports check failed." }

if (-not (Test-Path ".\config.json")) { throw "config.json missing" }
$cfg = Get-Content .\config.json -Raw | ConvertFrom-Json
if (-not $cfg.finnhub.api_key -or $cfg.finnhub.api_key -match 'PUT_YOUR_KEY_HERE') {
  Write-Host "NOTE: Finnhub key is placeholder; run will still work, but live price will be NaN."
}

Write-Host "doctor: OK"
