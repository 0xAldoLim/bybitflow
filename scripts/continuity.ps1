function Read-Continuity {
    param([string]$Ids='')
    $output=Get-Content -LiteralPath (Join-Path $PSScriptRoot 'runtime_continuity.py') -Raw | & docker compose exec -T desk python - $Ids
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read continuity snapshot; no deployment should proceed' }
    return ($output -join "`n" | ConvertFrom-Json -AsHashtable)
}
function Assert-Continuity {
    param($Before,$After)
    foreach ($id in $Before.active.Keys) {
        if (!$After.active.ContainsKey($id) -or (($Before.active[$id] | ConvertTo-Json -Compress) -ne ($After.active[$id] | ConvertTo-Json -Compress))) { throw "Original plan changed or disappeared: $id" }
    }
    $receipts=@($After.sent_initial | ForEach-Object { $_ | ConvertTo-Json -Compress })
    foreach ($receipt in $Before.sent_initial) {
        if (($receipt | ConvertTo-Json -Compress) -notin $receipts) { throw 'Sent initial receipt changed or disappeared' }
    }
    foreach ($table in $Before.counts.Keys) {
        if ($After.counts[$table] -lt $Before.counts[$table]) { throw "Permanent evidence decreased: $table" }
    }
}
