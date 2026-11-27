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
# After an unclean shutdown Docker Desktop can leave AF_UNIX socket files that
# Windows refuses to open or delete ("The file cannot be accessed by the system").
# Docker Desktop then crashes on every start until the folder is moved aside.
# These folders hold runtime sockets only: no images, volumes or settings.
function Repair-DockerSockets {
    $moved = 0
    foreach ($dir in @((Join-Path $env:LOCALAPPDATA 'Docker\run'), (Join-Path $env:LOCALAPPDATA 'docker-secrets-engine'))) {
        if (!(Test-Path -LiteralPath $dir)) { continue }
        $stale = @(Get-ChildItem -LiteralPath $dir -Force | Where-Object { $_.Attributes -band [IO.FileAttributes]::ReparsePoint })
        if (!$stale) { continue }
        $target = '{0}.stale-{1}' -f (Split-Path -Leaf $dir), (Get-Date -Format 'yyyyMMdd-HHmmss')
        Rename-Item -LiteralPath $dir -NewName $target
        New-Item -ItemType Directory -Path $dir | Out-Null
        Write-Host "Moved stale Docker socket folder aside: $target"
        $moved++
    }
    return $moved
}
function Stop-DockerDesktop {
    Get-Process -Name 'Docker Desktop','com.docker.backend','com.docker.build' -ErrorAction SilentlyContinue |
        Stop-Process -Force -Confirm:$false -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3
}
function Test-DockerEngine {
    & docker info --format '{{.ServerVersion}}' 2>$null | Out-Null
    return $LASTEXITCODE -eq 0
}
if (!(Test-DockerEngine)) {
    $desktop = Join-Path $env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'
    if (!(Test-Path -LiteralPath $desktop)) { throw 'Docker engine unavailable and Docker Desktop was not found' }
    $ready = $false
    # Each crash can leave a new stale socket, so allow a few repair rounds.
    foreach ($attempt in 1..4) {
        if (!(Get-Process -Name 'com.docker.backend' -ErrorAction SilentlyContinue)) { Repair-DockerSockets | Out-Null }
        Start-Process -FilePath $desktop -WindowStyle Hidden
        $deadline = (Get-Date).AddSeconds(90)
        do {
            Start-Sleep -Seconds 3
            if (Test-DockerEngine) { $ready = $true; break }
        } while ((Get-Date) -lt $deadline)
        if ($ready) { break }
        Stop-DockerDesktop
        if (!(Repair-DockerSockets)) { break }
    }
    if (!$ready) { throw 'Docker engine did not become available after socket repair; no reset or recreation attempted' }
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
