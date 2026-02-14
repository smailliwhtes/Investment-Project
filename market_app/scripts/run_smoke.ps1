$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..')
$appRoot = Join-Path $repoRoot 'market_app'

Set-Location $appRoot
python -m market_app.cli run --config tests\data\mini_dataset\config.yaml --offline --as-of-date 2025-01-31 --run-id smoke
exit $LASTEXITCODE
