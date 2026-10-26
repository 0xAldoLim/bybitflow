#Requires -Version 7
[CmdletBinding()]
param([Parameter(Mandatory)][string]$ImageRecord)
$ErrorActionPreference='Stop'
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
$images=Get-Content -LiteralPath $ImageRecord -Raw | ConvertFrom-Json -AsHashtable
if (!$images.desk) { throw 'A captured known-good desk image is required' }
. (Join-Path $PSScriptRoot 'continuity.ps1')
$before=Read-Continuity
$services=@{}
foreach ($name in @('desk','trainer')) {
    if ($images[$name]) {
        if ($images[$name] -notmatch '^sha256:[a-f0-9]{64}$') { throw 'Expected captured immutable image id' }
        & docker image inspect $images[$name] --format '{{.Id}}'
        if ($LASTEXITCODE -ne 0) { throw 'Known-good rollback image unavailable; do not restore an older database' }
        $services[$name]=@{image=$images[$name]}
    }
}
$override=Join-Path ([IO.Path]::GetTempPath()) ('bybitflow-rollback-'+[guid]::NewGuid().ToString('N')+'.json')
try {
    @{services=$services} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $override
    & docker compose -f compose.yaml -f $override --profile ml up -d --no-build --pull never --wait --wait-timeout 180
    if ($LASTEXITCODE -ne 0) { throw 'Rollback health check failed' }
    $after=Read-Continuity -Ids ($before.active.Keys -join ',')
    Assert-Continuity -Before $before -After $after
    & docker compose exec -T desk bybit-flow version
    if ($LASTEXITCODE -ne 0) { throw 'Rollback runtime verification failed' }
} finally {
    Remove-Item -LiteralPath $override -ErrorAction SilentlyContinue
}
