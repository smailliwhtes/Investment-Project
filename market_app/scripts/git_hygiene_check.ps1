param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
  Write-Error "Git is not available on PATH. Install Git to run the hygiene guard."
  exit 2
}

$paths = @(
  ".venv",
  "outputs",
  "data/raw",
  "data/processed"
)

$tracked = @()
foreach ($path in $paths) {
  $items = & git ls-files -- $path
  if ($LASTEXITCODE -ne 0) {
    Write-Error "Git hygiene check failed while scanning '$path'."
    exit $LASTEXITCODE
  }
  if ($items) {
    $tracked += $path
  }
}

if ($tracked.Count -gt 0) {
  $list = $tracked -join ", "
  Write-Error (
    "Forbidden paths are tracked ($list). " +
    "This can cause Codex diff size limit failures. " +
    "Fix: 'git rm --cached -r <path>' and add them to .gitignore if needed."
  )
  exit 1
}

Write-Host "OK"
