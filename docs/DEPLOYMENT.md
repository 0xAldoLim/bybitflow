# Deployment, maintenance and rollback

Run commands from any clone's repository root. Keep `.env` private and retain the
same Compose project and `research-data` volume. Deployment interrupts collection
briefly; schedule it separately from routine operation. A source commit alone
does not deploy or restart the running recorder.

## Deploy an approved revision

On Windows, install PowerShell 7. From CMD:

```bat
pwsh -NoProfile -File scripts\deploy.ps1
```

The command requires main, clean tracked files, exact pushed remote main, and green
Python 3.12/3.13 plus container-smoke CI for that exact revision. It captures active
plans, sent initial receipts, permanent evidence counts and previous immutable image
IDs. It builds/recreates services, waits for health, verifies the runtime commit,
checks continuity including previously active setups that became terminal, and
records version/config/lock/image/schema/time provenance. Audits are local under
`data/deployment-audits`, excluded from Git. It does not run a manual ML cycle,
bootstrap labeling, promotion or maturity reset. An already-running trainer resumes
its existing schedule. An intentionally stopped trainer stays stopped unless
deployment is requested with `-WithTrainer`; resource/evidence gates still apply.
The legacy `deploy_ml_bootstrap.ps1` now calls deployment only and warns explicitly.

`docker compose exec desk bybit-flow version` reports semantic version and source
commit, lock digest, database level, image ID when recorded, deployment time and an
allowlisted effective configuration digest. Secrets, account values, private URLs
and local paths are excluded. Unknown image/resource values are shown as UNKNOWN.
The redacted snapshot is reproducible operational context, not a backup of `.env`.

Linux operators use their normal exact-revision CI gate and Compose workflow;
verify the same continuity conditions before and after deployment. Never replace
an existing volume with a fresh one just to make startup succeed.

## ML maintenance and recovery are separate

```bat
pwsh -NoProfile -File scripts\ml-maintenance.ps1
pwsh -NoProfile -File scripts\ml-maintenance.ps1 -Operation cycle
pwsh -NoProfile -File scripts\recovery.ps1 -Source bybit -Limit 50
pwsh -NoProfile -File scripts\recovery.ps1 -Source bybit -Limit 50 -Apply
```

Status is read-only. A manual cycle serializes against the trainer worker and
resumes it afterward. Recovery defaults to the existing bootstrap dry run;
`-Apply` requests bounded recovery under existing storage/recorder protections.
These commands do not restart the desk, deploy source or promote models. Other
engine-specific recovery commands remain documented in [ML research](ML_RESEARCH.md).
A dry run may update operational audit/checkpoint metadata; it does not train models.

## Roll back code while retaining current evidence

Retain the previous images listed by the deployment audit. Do not prune those
images until the new deployment has passed operational review.

```bat
pwsh -NoProfile -File scripts\rollback.ps1 -ImageRecord data\deployment-audits\REVISION.json.rollback-images.json
```

This verifies captured immutable image IDs exist locally, captures continuity,
starts those images with `--no-build --pull never`, waits for health and verifies
original plans, sent receipts and evidence counts. It uses the current Compose
configuration and current research-data volume. Newly recorded evidence stays.
The maintenance migration is additive; a future incompatible migration must ship
an explicit rollback-compatibility plan. Rollback cannot undo data corruption.

Do not use `git reset --hard`, `down -v`, volume deletion or restoration of an older
database for an ordinary code rollback. Restoring an old database can erase newer
evidence and delivery receipts and belongs to a separately approved disaster-recovery
procedure. Inspect failures rather than repeatedly recreating containers.

## Basic troubleshooting

- Clock warning: inspect the measured drift versus request uncertainty. Enable
  automatic time in Windows and click Sync now only when drift is established.
  A stale/failed exchange check can be DNS/network related instead.
- Storage pressure: inspect `storage status`, category growth and protected ranges.
  Preview existing safe cleanup with `storage cleanup --dry-run`; never manually
  delete databases, WALs, active evidence or model files. Docker image/cache usage
  is separate from the configured research-data budget.
- No signals: inspect `signals status`, feed/history freshness, exact gate reasons
  and delivery receipts. Millions of events are not millions of independent setups.
  Do not loosen strategy gates merely to generate a notification.
- Database contention/loop lag: review measured diagnostics and deferred background
  state. Critical writes fail closed. Investigate concurrent writers and slow disk;
  do not force a live WAL truncation.
- Recovery backlog: inspect each engine's source/history/gap reason and next retry.
  Missing causal proof remains incomplete; successful recovery never relabels old
  immutable evidence as if it had been continuously recorded.

See the [operator runbook](USER_RUNBOOK.md) for detailed host/network guidance.

## Storage upkeep when training is paused

`maintenance` starts with the default stack and uses the same persistent volume.
It processes existing local outcomes and safe retention without starting the
trainer or making exchange/Discord requests. Inspect `docker compose logs --tail
30 maintenance` for progress, busy deferrals or capacity blockers. Keep it running
when stopping `trainer` to reduce memory. Never remove protected evidence to make
a stalled recorder appear healthy. Permanently full protected storage requires a
capacity decision; the application reports that state explicitly.
