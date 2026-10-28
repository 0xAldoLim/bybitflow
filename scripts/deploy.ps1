#Requires -Version 7
[CmdletBinding()]
param([switch]$WithTrainer)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $repoRoot

function Invoke-Docker {
    param([string[]]$DockerArgs)
    & docker @DockerArgs
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed: $($DockerArgs[0])" }
}

$commit = (& git rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot determine repository commit' }
if ((& git branch --show-current).Trim() -ne 'main') { throw 'Deployment requires main' }
if (& git status --porcelain --untracked-files=no) { throw 'Tracked changes must be committed before deployment' }
Invoke-Docker -DockerArgs @('info', '--format', '{{.ServerVersion}}')

# Require exact pushed main and all latest Python/container verification jobs.
$apiRoot = 'https://api.github.com/repos/0xAldoLim/bybitflow'
$headers = @{ Accept='application/vnd.github+json'; 'User-Agent'='BybitFlow-Deployment' }
$remote = Invoke-RestMethod -Uri "$apiRoot/git/ref/heads/main" -Headers $headers
if ($remote.object.sha -ne $commit) { throw "Local main must match pushed remote main" }
$runs = Invoke-RestMethod -Uri "$apiRoot/actions/runs?head_sha=$commit&event=push" -Headers $headers
$run = $runs.workflow_runs | Where-Object { $_.name -eq 'Offline verification' } | Sort-Object run_number -Descending | Select-Object -First 1
if (!$run -or $run.head_sha -ne $commit -or $run.conclusion -ne 'success') { throw 'The exact commit has not passed GitHub verification; deploy after CI succeeds' }
$jobs = Invoke-RestMethod -Uri "$apiRoot/actions/runs/$($run.id)/jobs" -Headers $headers
$required = @('test (3.12)', 'test (3.13)', 'container-smoke')
foreach ($name in $required) {
    if (!($jobs.jobs | Where-Object { $_.name -eq $name -and $_.conclusion -eq 'success' })) { throw "Required CI job missing or failed: $name" }
}
$security = $runs.workflow_runs | Where-Object { $_.name -eq 'Security analysis' } | Sort-Object run_number -Descending | Select-Object -First 1
if (!$security -or $security.head_sha -ne $commit -or $security.conclusion -ne 'success') { throw 'The exact commit has not passed security analysis' }
$securityJobs = Invoke-RestMethod -Uri "$apiRoot/actions/runs/$($security.id)/jobs" -Headers $headers
if (!($securityJobs.jobs | Where-Object { $_.name -eq 'codeql' -and $_.conclusion -eq 'success' })) { throw 'Required CodeQL check missing or failed' }
Write-Host "Verified commit $commit · $($run.html_url)"

$auditCode = @'
import hashlib,json,os,sqlite3
from pathlib import Path
from bybit_flow.config import Settings
from bybit_flow.storage import directory_bytes
import sys
settings=Settings()
root=settings.data_dir
ids=[x for x in sys.argv[1].split(',') if x] if len(sys.argv)>1 else []
db=sqlite3.connect(f'file:{root}/research.sqlite?mode=ro',uri=True,timeout=15)
fields=('id','symbol','source','family','direction','created_ms','entry','zone','stop','tp1','tp2','expires_ms','holding_deadline_ms','horizon_profile')
where="state NOT IN ('EXPIRED','INVALIDATED','RESOLVED')"
if ids: where+=' OR id IN ('+','.join('?' for _ in ids)+')'
active={}
for payload, in db.execute('SELECT payload FROM signals WHERE '+where,ids).fetchall():
    s=json.loads(payload)
    active[s['id']]={k:s.get(k) for k in fields}
sent=[list(row) for row in db.execute("SELECT key,message_id FROM outbox WHERE status='sent' AND key LIKE '%:initial'").fetchall()]
counts={table:db.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in ('ml_snapshots','ml_labels','ml_models')}
print(json.dumps(dict(active=active,sent_initial=sent,counts=counts,used_bytes=directory_bytes(root),budget_bytes=settings.max_storage_gb*1e9,files={p.name:p.stat().st_size for p in root.iterdir() if p.is_file()})))
'@

function Read-Audit {
    param([string]$Ids = '')
    $output = $auditCode | & docker compose exec -T desk python - $Ids
    if ($LASTEXITCODE -ne 0) { throw 'Runtime audit failed' }
    return ($output -join "`n" | ConvertFrom-Json -AsHashtable)
}

$before = Read-Audit
$reportRoot = Join-Path $repoRoot 'data\deployment-audits'
New-Item -ItemType Directory -Force -Path $reportRoot | Out-Null
$reportPath = Join-Path $reportRoot ($commit + '.json')
$before | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath ($reportPath + '.before')
. (Join-Path $PSScriptRoot 'docker-state.ps1')
$images = Read-RunningImages
$images | ConvertTo-Json | Set-Content -LiteralPath ($reportPath + '.rollback-images.json')
$env:FLOW_CODE_COMMIT = $commit
# Preserve an intentionally disabled trainer unless explicitly requested.
$services=@('desk','maintenance')
if ($WithTrainer -or $images.ContainsKey('trainer')) { $services += 'trainer' }
Invoke-Docker -DockerArgs (@('compose','--profile','ml','build') + $services)
Invoke-Docker -DockerArgs (@('compose','--profile','ml','up','-d','--no-build','--wait','--wait-timeout','180') + $services)
$runtimeVersion = (& docker compose exec -T desk bybit-flow version | ConvertFrom-Json)
if ($runtimeVersion.deployment.git_commit -ne $commit) { throw 'Runtime commit differs from approved source' }
$containerId = (& docker compose ps -q desk).Trim()
$imageId = (& docker inspect --format '{{.Image}}' $containerId).Trim()
$recordCode = "from bybit_flow.config import Settings; from bybit_flow.storage import Store,now_ms; from bybit_flow.provenance import record; import os,sys; os.environ['FLOW_IMAGE_ID']=sys.argv[1]; settings=Settings(); store=Store(settings.data_dir); record(store,settings,now_ms()); store.close()"
$recordCode | & docker compose exec -T desk python - $imageId
if ($LASTEXITCODE -ne 0) { throw 'Deployment provenance not acknowledged' }

$after = Read-Audit -Ids ($before.active.Keys -join ',')
foreach ($id in $before.active.Keys) {
    if (!$after.active.ContainsKey($id) -or (($before.active[$id] | ConvertTo-Json -Compress) -ne ($after.active[$id] | ConvertTo-Json -Compress))) {
        throw "Original setup plan changed or disappeared: $id"
    }
}
foreach ($table in $before.counts.Keys) {
    if ($after.counts[$table] -lt $before.counts[$table]) { throw "Permanent records decreased: $table" }
}
$afterSent = @($after.sent_initial | ForEach-Object { $_ | ConvertTo-Json -Compress })
foreach ($sent in $before.sent_initial) {
    if (($sent | ConvertTo-Json -Compress) -notin $afterSent) { throw 'An original sent delivery receipt disappeared' }
}
@{ commit=$commit; ci_url=$run.html_url; rollback_images=$images; before=$before; after=$after; continuity='Original plans and delivery receipts retained' } | ConvertTo-Json -Depth 15 | Set-Content -LiteralPath $reportPath
Invoke-Docker -DockerArgs @('compose','exec','-T','desk','bybit-flow','doctor','--json')
Invoke-Docker -DockerArgs @('compose','exec','-T','desk','bybit-flow','signals','status')
Write-Host "Audit saved to $reportPath"
