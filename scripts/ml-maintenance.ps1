#Requires -Version 7
[CmdletBinding()]
param([ValidateSet('status','cycle')][string]$Operation='status')
$ErrorActionPreference='Stop'
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
if ($Operation -eq 'status') {
    & docker compose exec -T trainer bybit-flow ml status
    if ($LASTEXITCODE -ne 0) { throw 'ML status failed' }
    return
}
& docker compose --profile ml stop trainer
if ($LASTEXITCODE -ne 0) { throw 'Cannot serialize trainer' }
try {
    & docker compose --profile ml run --rm --no-deps trainer bybit-flow ml cycle
    if ($LASTEXITCODE -ne 0) { throw 'ML cycle failed or abstained; inspect readiness' }
} finally {
    & docker compose --profile ml up -d --no-build trainer
    if ($LASTEXITCODE -ne 0) { throw 'Cannot resume trainer' }
}
