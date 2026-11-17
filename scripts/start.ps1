#Requires -Version 7
[CmdletBinding()]
param([switch]$WithTrainer, [switch]$InstallLoginTask)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)
if ($InstallLoginTask) {
    $user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    $engine = (Get-Command pwsh).Source
    $arguments = '-NoProfile -NonInteractive -File "' + $PSCommandPath + '"'
    if ($WithTrainer) { $arguments += ' -WithTrainer' }
    $action = New-ScheduledTaskAction -Execute $engine -Argument $arguments -WorkingDirectory (Get-Location).Path
    $principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
    $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Minutes 8) -MultipleInstances IgnoreNew -StartWhenAvailable
    Register-ScheduledTask -TaskName 'BybitFlow-StartExisting' -Action $action -Principal $principal -Trigger $trigger -Settings $settings -Force | Out-Null
    Write-Host 'Registered login startup for the existing installation. No build, deployment or data replacement.'
    exit 0
}
& docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    $desktop = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
    if (!(Test-Path -LiteralPath $desktop)) { throw 'Docker engine unavailable and Docker Desktop was not found' }
    Start-Process -FilePath $desktop -WindowStyle Hidden
    $deadline = (Get-Date).AddMinutes(3)
    do {
        Start-Sleep -Seconds 3
        & docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) { break }
    } while ((Get-Date) -lt $deadline)
    if ($LASTEXITCODE -ne 0) { throw 'Docker engine did not become available; no reset or recreation attempted' }
}
$services = @('desk','maintenance')
$configured = @(& docker compose --profile ml config --services)
if ($LASTEXITCODE -ne 0) { throw 'Cannot read Compose services' }
if ($configured -contains 'dns') { $services = @('dns') + $services }
if ($WithTrainer) { $services += 'trainer' }
& docker compose --profile ml start @services
if ($LASTEXITCODE -ne 0) { throw 'Cannot start existing containers; use the verified deployment workflow if containers are missing' }
& docker compose --profile ml ps
if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect started services' }
