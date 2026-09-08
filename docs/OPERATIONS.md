# Operations, deployment and data care

## Local and VPS

Use one process per SQLite/data directory. A 2-core / 2–4GB VPS is a starting resource budget,
not a measured capacity promise. Deep subscriptions default to eight eligible instruments,
50 book levels. Increase only after measuring throughput, memory and disk. Scanner REST
budget is three requests/second; wide scans may outlast a 15-minute cadence, and stale
contexts then abstain. No live credentials or trade permissions exist in this application.

Compose binds loopback and requires an admin password even though its host port is local.
Use `research` as the HTTP Basic username. Prefer an SSH tunnel; HTTP Basic requires TLS
if transported beyond localhost. Do not use multiple Uvicorn workers, public unauthenticated
ports, or a reverse proxy that exposes `/api` without the same authentication.

Configuration is read at startup from `FLOW_*` environment variables and `.env`; restart after
changes. Webhook secrets and the password are excluded from API settings responses, `.gitignore`,
Docker build context and application logs. HTTP client request logging is suppressed in the CLI.
Do not enable verbose HTTP transport logging with real webhook tokens.

## Data health and capacity

`/healthz` reports process/scanner/recorder health, not a guarantee all symbols are fresh.
`/api/overview` exposes queue occupancy, recording status, latest error and stream health.
The dashboard distinguishes unavailable feeds from empty eligible lists.

Raw and Parquet segments flush at 100 envelopes or roughly one second. The queue is bounded
by count and a 32MB serialized-payload budget; WebSocket receive buffers and symbol tapes are
also bounded. Each tape retains at most 50,000 trades by default. A busy symbol that cannot
retain its complete 15M window abstains; increasing this bound requires measured memory headroom.

Default local storage budget: 10GB. On budget exhaustion, write failure or queue overflow,
the recording circuit opens, feed coverage is invalidated, and new confirmations stop.
Restart only after resolving the issue. Segment writes are not an atomic transaction across
both files and SQLite: a crash may leave an orphan raw/Parquet segment. Treat unmanifested
files as unverified and inspect them before research. In-memory queued observations can be
lost on a hard crash. Clean shutdown stops feeds and drains the bounded queue.

Do not delete active market data automatically. `bybit-flow retention-plan` lists manifested
segments older than the default 14-day review age; archive them with hashes first, then remove
only reviewed explicit paths. Retention age is a review policy, not an automatic data eraser.
An external disk quota is recommended for a strict hard ceiling because a final batch/SQLite
growth can exceed the soft application budget. Data versioning must accompany archival.

## Backups and migrations

```sh
.venv/bin/bybit-flow backup /absolute/new/path/research-backup.sqlite
```

The SQLite online backup API creates a consistent metadata snapshot and refuses to overwrite
an existing backup. Back up immutable segments, experiment JSON and `.env` separately; encrypt
secret backups. Restore into a NEW directory, verify segment SHA-256, start with scanning and
notifications disabled, inspect state, then enable collection. No destructive restore script
is supplied.

Schema v1 creates versioned metadata tables idempotently. A future migration must back up,
check `schema_version`, use a transaction and retain the original database until verification.
No v2 migration is claimed. PostgreSQL migration would move metadata tables and the alert
attempt ledger, while retaining Parquet/object files and their hashes; it is not implemented.

## Discord delivery semantics

No notifications are sent until a research webhook and explicit research-alert toggle are
configured. Initial cards are delivered only after required coverage and risk gates. Signal
IDs derive from symbol/direction/family/setup close/version. The durable outbox deduplicates
that ID and applies a symbol cooldown. Successfully delivered signals transition to ALERTED;
this means a card was delivered, not a trade executed. INVALIDATED/EXPIRED updates use
separate deduplication keys. Paper RESOLVED requires manual journal input.

If Discord times out after accepting a request, there is no safe exactly-once replay guarantee.
Rows left `sending` or `uncertain` must be reconciled against the channel using the signal ID.
HTTP 429/rejections are retained as `rate-limited`/`rejected`; they are not blindly retried.
Do not clear the outbox to force a resend. A missed research card is preferable to duplicated
or stale cards. Public alert delivery remains locked irrespective of webhook configuration.

## Dependencies and licenses

Application source: MIT. Direct libraries include FastAPI, Uvicorn, HTTPX, websockets,
Pydantic/pydantic-settings, NumPy, PyArrow and DuckDB. Tests use pytest, pytest-asyncio and
Ruff. Dependency licenses remain with their projects; exact installed versions are captured
in `requirements.lock`. No commercial data subscription is required. Exchange data is governed
by the source's terms and is not covered by this repository's MIT license.
