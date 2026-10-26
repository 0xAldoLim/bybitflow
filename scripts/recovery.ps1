#Requires -Version 7
[CmdletBinding()]
param([ValidateSet('binance','bybit','okx')][string]$Source='bybit',
      [ValidateRange(1,1000)][int]$Limit=50,[switch]$Apply)
$ErrorActionPreference='Stop'
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
$argsList=@('compose','--profile','ml','run','--rm','--no-deps','trainer','bybit-flow','ml','bootstrap-labels','--source',$Source,'--limit',"$Limit",'--resume')
if (!$Apply) { $argsList += '--dry-run' }
if ($Apply) {
    & docker compose --profile ml stop trainer
    if ($LASTEXITCODE -ne 0) { throw 'Cannot serialize recovery against worker' }
}
try {
    & docker @argsList
    if ($LASTEXITCODE -ne 0) { throw 'Recovery failed or deferred' }
} finally {
    if ($Apply) { & docker compose --profile ml up -d --no-build trainer }
}
