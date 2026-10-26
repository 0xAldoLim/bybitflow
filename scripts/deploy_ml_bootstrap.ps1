#Requires -Version 7
[CmdletBinding()]
param()
Write-Warning 'This legacy command now deploys only. Use ml-maintenance.ps1 or recovery.ps1 separately.'
& (Join-Path $PSScriptRoot 'deploy.ps1')
if ($LASTEXITCODE -ne 0) { throw 'Deployment failed' }
