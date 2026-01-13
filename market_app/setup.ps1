param(
  [string]$PythonExe = "py",
  [string[]]$PythonArgs = @("-3")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path ".\requirements.txt")) {
  throw "requirements.txt not found. Create it first."
}

# Create / refresh venv
& $PythonExe @PythonArgs -m venv .venv

$pyexe = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $pyexe)) { throw "Venv python not found at $pyexe" }

& $pyexe -m pip install --upgrade pip
& $pyexe -m pip install -r .\requirements.txt

Write-Host "OK: venv created and requirements installed."
