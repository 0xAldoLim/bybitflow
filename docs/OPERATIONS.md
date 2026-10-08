# Operations, deployment, and data lifecycle

Start with the [README](../README.md) and [operator guide](USER_RUNBOOK.md) for
commands, configuration, and troubleshooting. This reference describes operational
constraints. Repository verification records are dated evidence, not live health.

## Deployment and resources

Run one collector and at most one ML worker per persistent data directory. Compose
uses Linux containers, a 4 GB/two-CPU desk limit, and a 2 GB/one-CPU trainer limit.
The trainer is enabled by `--profile ml`. Both services use `restart: unless-stopped`.
Measure memory, disk, and throughput before increasing deep subscriptions from the
default eight toward the supported maximum of 30. A small VPS is not a measured
capacity guarantee for that workload.

Compose binds `127.0.0.1:8000` and requires `FLOW_ADMIN_TOKEN`. The HTTP Basic user
is `research`. Prefer an SSH tunnel for remote access; use TLS if authentication
travels beyond localhost. Do not expose unauthenticated APIs or start multiple
Uvicorn workers against the collection state.

Settings are loaded at startup. Apply `.env` changes with `docker compose --profile
ml up -d`; restarting existing containers does not change their environment.
Keep the existing Compose project name and `research-data` volume when updating.
An ordinary stop preserves data; `down -v` deletes volumes. Host sleep, an unavailable
Docker engine, or shutdown interrupts collection regardless of restart policy.

Webhook/password values are excluded from public settings responses and the build
context. Keep `.env` private and avoid verbose HTTP logging with webhook credentials.
The optional DNS relay affects project containers only; see
[encrypted DNS](USER_RUNBOOK.md#optional-encrypted-dns).

## Data health and capacity

`/healthz` describes process/scanner/recorder state, not guaranteed freshness for
every selected market. `doctor --json` exposes primary stream health, active-plan
required feeds, recorder queue/drops, probes, storage, and worker state. Check
observation times and required coverage, not just container healthcheck success.

The recorder has a bounded queue and a separate writer process. New segments rotate
at 15 seconds or 10,000 rows by default, with pressure flushing. Trade tapes,
WebSocket buffers, and replay work are bounded. Missing flow or an overflow is
recorded as a coverage gap and cannot become complete confirmation/ML evidence.
Clean shutdown drains the queue; a hard crash can lose queued observations.

The default application-data budget is 10 decimal GB. This does not cap Docker
images, build cache, or virtual-disk allocation. Maturity status is healthy below
80%, warning at 80–90%, pressure at 90–95%, and backpressured at 95% or above.
Independent maintenance begins packing at 70% and safe pruning at 85%; training
and bootstrap downloads defer at 95%. These thresholds are unchanged by ML models.

Routine retention can release finalized, unprotected raw evidence while keeping
snapshots, labels, datasets, models, plans, manifests, and audit hashes. Active
setups, unresolved primary outcomes, durable checkpoints, and reader leases protect
their required ranges. Raw data cannot be replayed after deletion. If protected or
permanent data alone fills the budget, recording can stop explicitly. Do not erase
evidence or repeatedly raise the cap to hide a retention failure. An external
quota can enforce a stricter physical ceiling than the application's budget.

SQLite and file writes are not one atomic transaction. A crash can leave orphan
files; unmanifested or unverified files are not research evidence. Inspect integrity
failures before replay. See [managed storage](USER_RUNBOOK.md#managed-storage) for
preview, cleanup, and compaction commands.

## Backups and migrations

`bybit-flow backup NEW_PATH.sqlite` uses SQLite's online backup API for consistent
metadata and refuses to overwrite an existing backup. It does not copy recordings,
packs, datasets, or model files. Archive the stopped volume for a full backup, and
protect credentials separately. Verify hashes and restore into a new volume with
scanning/notifications disabled before replacing the live installation.

Schema creation and additive migrations preserve historical records. Original
plans and immutable model/label provenance must survive upgrades. The
CI-gated [deployment script](ML_RESEARCH.md#storage-and-replay-checkpoints) records
active-plan and delivery continuity and creates the maturity epoch only if absent.
Later deployments and worker cycles preserve that marker. PostgreSQL migration
is not implemented.

## Discord delivery semantics

Research delivery needs `FLOW_RESEARCH_WEBHOOK` plus explicit alert opt-in. The
broader `FLOW_RESEARCH_ALERTS=true` accepts all grades subject to confirmation,
entry, liquidity, risk, macro/alignment, and applicable production gates. An
economic-plan fingerprint and durable outbox suppress repeated notifications.
Delivery does not mean account execution.

Every user-facing lifecycle update requires the exact setup's durable initial
outbox record to be `sent` with a message ID. A known initial webhook must match
the current signals destination. Suppressed, never-attempted, dry-run, disabled,
rejected, or uncertain initials do not establish visibility. Those setups retain
independent internal monitoring; updates return `blocked:no-visible-initial`.
A child mentioned in another setup's card is not independently delivered.

Terminal events are persisted before an independent worker edits the original
signals card. Editing a known message can be retried without creating another
initial alert. Monitoring pause/resume cards use `FLOW_MONITORING_WEBHOOK` and
deduplicate by setup ID and outage start. Catch-up that finds a terminal result
does not send a resume first. Notification clustering never merges or terminates
the underlying plans. Switching webhooks does not republish old initials.

Initial HTTP timeouts can remain `uncertain`: exactly-once external delivery cannot
be guaranteed. Check the Discord channel by setup ID before reconciling ambiguity.
Initial delivery rechecks the stored lifecycle state and entry deadline after
claiming and immediately before HTTP dispatch. At least the existing 15-second
request budget must remain; otherwise it returns `blocked:entry-window-too-short`
or `blocked:entry-window-expired` without posting. The initial request also has
a total 15-second timeout across connection, write and response phases. This
does not extend an entry window or change qualification. A timeout after dispatch
can still be ambiguous, so it never causes a blind retry.
Do not clear the outbox to force a resend. Missing/deleted original messages and
non-retryable terminal edit failures remain diagnostic results. Synthetic and
connection tests are excluded from genuine signal/ML metrics.

Validated public probability requires registry approval and current cohort evidence.
Current assumed-cost print labels cannot meet verified-cost promotion requirements.
Bootstrap OHLC-proxy models can never become champions or filter delivery. There
is no automatic model promotion, strategy tuning, or threshold optimization.

### Scanner running but no new trading cards

Use `bybit-flow signals status` in the desk container to inspect recent candidate,
confirmation, rejection, V8 and delivery counts. Repeated confirmation windows
are attempts, not independent setups; rejection counts can overlap. Check
`bybit-flow doctor` for the actual selected-market feed state. A healthy recorder
or REST probe does not establish a healthy trade or order-book WebSocket.

Candidate refresh checks readiness after candle I/O, and each candidate's
confirmation checks use its current evaluation time. Live receipts can advance
while earlier candidates await analysis; they must not be compared against an
obsolete batch timestamp and misclassified as future or stale data. The normal
book/trade stale limits and all strategy gates still apply.

When confirmed, alert-claimed and HTTP-attempt counts are all zero, qualification
has stopped before delivery. An empty primary ML cohort does not block the
deterministic production policy: its ML output is advisory. When HTTP attempts
exist, inspect durable outbox statuses and message IDs before retrying anything.
Do not lower strategy gates, fabricate a signal or clear delivery history to
test connectivity; use the separately labeled connection-test command.

## Dependencies and licenses

Application source is MIT. Direct runtime libraries include FastAPI, Uvicorn, HTTPX,
websockets, Pydantic, NumPy, PyArrow, and DuckDB. The trainer adds scikit-learn,
LightGBM, and CPU PyTorch. Tests use pytest, pytest-asyncio, and Ruff. Pinned
versions are recorded in the lock files; dependency licenses remain with their
projects. Exchange data is governed by source terms, not the repository license.
