# BybitFlow

BybitFlow monitors public crypto perpetual-market data and sends confirmed research
setups to Discord. Cards include LONG or SHORT, quality grade, entry zone, stop
loss, TP1, TP2, entry deadline, and holding horizon. It does not place orders or
read trading-account fills. Quality scores and ML rankings are not validated win
probabilities.

The application includes a local dashboard, recorded order-flow evidence,
independent setup monitoring, managed recording retention, and a background ML
worker. Binance, Bybit, and OKX REST and WebSocket adapters are implemented.
Automatic source selection uses one working primary venue; ML outcomes remain
separate by source.

## Quick navigation

- [First-time setup](#first-time-setup)
- [Start, stop, and check status](#start-stop-and-check-status)
- [Understand Discord cards](#understand-discord-cards)
- [ML collection and training](#ml-collection-and-training)
- [Storage and cleanup](#storage-and-cleanup)
- [Troubleshooting](#troubleshooting)
- [Updates and backups](#updates-and-backups)
- [Reference guides](#reference-guides)

## First-time setup

Install Git and Docker Desktop on Windows, or Docker Engine with the Compose plugin
on Linux. Docker Desktop must use Linux containers. Keep the host awake and allow
access to public exchange endpoints. No exchange trading keys or paid chart
subscription are required. Discord delivery needs two channel webhooks.

In Windows **Command Prompt (CMD)**, clone into a folder of your choice:

```bat
git clone https://github.com/0xAldoLim/bybitflow.git
cd bybitflow
if not exist .env copy .env.example .env
notepad .env
```

For an existing checkout, enter its folder instead:

```bat
cd /d "C:\path\to\bybitflow"
```

Replace the example path with your own repository location. On Linux or macOS, use
`cd /path/to/bybitflow` and copy `.env.example` to `.env` only if `.env` does not exist.
The Docker commands below work on either platform.

Set a long private password and full Discord webhook URLs in `.env`:

```dotenv
FLOW_ADMIN_TOKEN=replace-with-a-long-random-password
FLOW_RESEARCH_WEBHOOK=https://discord.com/api/webhooks/REPLACE_WITH_SIGNALS_WEBHOOK
FLOW_MONITORING_WEBHOOK=https://discord.com/api/webhooks/REPLACE_WITH_MONITORING_WEBHOOK
FLOW_SCAN_ENABLED=true
FLOW_RESEARCH_ALERTS=true
FLOW_EXECUTION_WINDOW_SECONDS=60
FLOW_SPREAD_BUCKET_SECONDS=60
FLOW_SPREAD_WINDOW_MINUTES=30
FLOW_ML_ENABLED=true
FLOW_ML_FILTER_RESEARCH=false
FLOW_RECORDING_RETENTION_ENABLED=true
FLOW_MAX_STORAGE_GB=10
```

The webhook values are placeholders. These settings enable all-grade confirmed
research alerts and minute-by-minute flow confirmation. The shipped example keeps
research delivery disabled and uses a 900-second confirmation window until you
change it. Two-stage ML defaults to `FLOW_ML_TWO_STAGE=false` and is optional.

`FLOW_DEEP_SYMBOLS` defaults to 8 and supports up to 30 deep subscriptions. The
[top-30 configuration example](examples/ml-top30.env) contains a dated watchlist;
unsupported or ineligible pairs are not guaranteed a subscription. Size storage
for available disk and collection volume. The 10 GB example limits application
data, not total Docker disk usage.

Keep `.env` private and preserve existing credentials on upgrades. The older
`FLOW_DISCORD_WEBHOOK` is not the destination for setup delivery.

Build and start:

```bat
docker compose --profile ml up -d --build
```

The first build downloads application and training dependencies, including CPU-only
PyTorch in the trainer image. Open [the dashboard](http://127.0.0.1:8000), sign in
as `research`, and use your `FLOW_ADMIN_TOKEN` as the password.

## Start, stop, and check status

Run from the repository folder with Docker running:

| Action | Command |
|---|---|
| Start or resume scanner and ML worker | `docker compose --profile ml up -d` |
| Stop services and preserve data | `docker compose --profile ml stop` |
| Check containers | `docker compose --profile ml ps` |
| Check feeds, recorder, and runtime diagnostics | `docker compose exec desk bybit-flow doctor` |
| Inspect signal admission and delivery counters | `docker compose exec desk bybit-flow signals status` |
| Inspect ML readiness, models, and heartbeat | `docker compose exec trainer bybit-flow ml status` |
| Inspect storage and retention | `docker compose exec desk bybit-flow storage status` |
| View recent logs | `docker compose --profile ml logs --tail 100` |

For JSON diagnostics, use `bybit-flow doctor --json`. Closing CMD or the browser
does not stop containers. Docker and the host must stay running. Containers use
`restart: unless-stopped`; this does not prevent host sleep or ensure Docker
Desktop starts after a reboot.

Configuration is read at startup. Apply `.env` changes with
`docker compose --profile ml up -d`; a plain `restart` does not load changed
container environment values. Repeated restarts reset live flow continuity and
can prolong warm-up.

## Understand Discord cards

| Destination | Messages |
|---|---|
| `FLOW_RESEARCH_WEBHOOK` | New setups, related/conflicting setup context, entry expiry, withdrawal, targets, and other setup lifecycle results |
| `FLOW_MONITORING_WEBHOOK` | Monitoring paused and monitoring resumed notices |

Terminal results edit the original delivered setup card, retaining entry, stop,
targets, and first-posted time. Check earlier cards for updated status. Monitoring
notices are deduplicated by outage episode. An internal setup cannot send any
user-facing lifecycle update without its own successful initial delivery and
message ID in the durable outbox. Mentioning a child setup inside another card
does not establish its initial delivery.

Temporary feed loss pauses monitoring rather than proving a stop or target.
Recovery requires historical catch-up and fresh live coverage. Each setup keeps
its original plan and lifecycle even when related notifications are grouped or
suppressed.

| Horizon | Expected holding | Delivery |
|---|---|---|
| Short intraday | 15 minutes–2 hours | Confirmed research setups |
| Core intraday | 1–4 hours | Confirmed research setups |
| Swing | 4–48 hours | Confirmed research setups |
| Extended swing | 2–7 days | Shadow research; no initial Discord setup |

The entry deadline is separate from the holding deadline. A 60-second confirmation
window does not mean a one-minute holding period. Implemented grade bands are
**SSS 95+, SS 90+, S 85+, A 75+, B 65+, C 50+, D 35+, E 20+, F below 20**.
Every grade must still pass structure, flow, liquidity, freshness, entry, risk,
and applicable production gates.

At a tracking deadline, a reliable original-venue price and observed entry can
support a paper profit/loss estimate before TP1. Missing evidence leaves the result
unknown. Late research observations never turn an expired setup into a historical
win. The Paper journal records an account close separately.

There is no guaranteed alert interval. Minute-spread qualification usually needs
12–15 minutes of uninterrupted quotes after subscription; discovery, cold candle
loading, and feed gaps can add time. A healthy scanner can reject every current
setup. See the [horizon and lifecycle guide](docs/USER_RUNBOOK.md#multi-horizon-research).

## ML collection and training

| Track | Evidence | Authority |
|---|---|---|
| Primary | Current `candidate-v10` decisions with complete `prints-v1` outcomes from recorded trades | Preferred compatible model; promotion requires independent admission checks |
| Bootstrap | `bootstrap-core-v1` features with `ohlc-path-v1` historical one-minute candle outcomes | Advisory OHLC-proxy challenger; promotion and production filtering disabled |

Each track needs **500 complete unique outcomes per source**. Millions of exchange
events, repeated evaluations, incomplete paths, and manual journal entries do not
meet that requirement. A positive label means simulated net R above zero after
assumed costs, not a verified account win.

The baseline compares Logistic Regression and LightGBM. With
`FLOW_ML_TWO_STAGE=true`, the primary sequence pipeline can compare LightGBM,
Random Forest, and LSTM, followed by Logistic Regression, SVM, and Random Forest
probabilities. It needs 500 complete outcomes with 16 compatible prior observations
each. Below that threshold it attempts the tabular baseline. XGBoost is not installed.

Training preserves chronological partitions, a four-hour embargo, at least
200 training / 100 calibration / 100 validation / 100 untouched holdout outcomes,
and both classes in training and calibration. `adaptive-causal-v2` refines only
pathological V1 allocations using timing and counts, with a V1 fallback. Previously
consumed holdouts cannot become unseen evidence again.

Readiness refreshes every 15 minutes. The first eligible fit is dispatched on the
next worker loop; later fits require 50 new usable outcomes, seven days, or a manual
cycle, and must still satisfy readiness. Status reports partition/class blockers,
compatibility abstentions, models, and heartbeat. Collection does not guarantee
immediate fitting or a prediction on every setup. With `FLOW_ML_FILTER_RESEARCH=false`,
ML abstention does not block otherwise qualified alerts; bootstrap can never filter them.

The verified deployment workflow creates an immutable maturity epoch. Keep feature
definitions, gates, strategy rules, score weights, labels, partition policy, and
ML minimums fixed while outcomes mature, except for correctness, safety, exchange
API, or severe runtime/storage repairs. There is no automatic strategy tuning or
model promotion. See [ML research](docs/ML_RESEARCH.md).

## Storage and cleanup

Docker stores recordings, SQLite history, snapshots, labels, datasets, and models
in the persistent `research-data` volume. `FLOW_MAX_STORAGE_GB` defaults to
**10 decimal GB** and may differ in an existing installation. It excludes Docker
images, build cache, and Docker/WSL virtual-disk allocation.

Retention preserves permanent ML records and protected evidence. Routine worker
cleanup releases finalized, unprotected raw data after its retention floor; it
does not wait for every future model fit. Active plans, unresolved primary outcomes,
checkpoints, and replay leases prevent unsafe deletion. Raw cleanup keeps learned
model artifacts, but deleted raw history cannot be replayed again.

Inspect the plan before manual cleanup:

```bat
docker compose exec desk bybit-flow storage status
docker compose exec desk bybit-flow storage cleanup --dry-run
docker compose exec desk bybit-flow storage cleanup
docker compose exec desk bybit-flow storage compact
```

Manual cleanup can report no deletion below the pressure threshold. Maturity health
shows healthy below 80%, warning at 80–90%, pressure at 90–95%, and training/backfill
backpressure at 95% or above. Maintenance separately starts packing at 70% and safe
pressure pruning at 85%. If protected or permanent data fills the budget, recording
can stop rather than delete it. See [managed storage](docs/USER_RUNBOOK.md#managed-storage).

## Troubleshooting

Start with `docker compose --profile ml ps`, `doctor`, and recent logs. A running
container does not prove every selected market has fresh evidence.

| Symptom | First action |
|---|---|
| Clock skew, future events, or timestamp errors | Open Windows **Settings → Time & language → Date & time**, enable automatic time, choose the correct time zone, and select **Sync now**. Run `doctor` again. |
| Dashboard unavailable | Start Docker Desktop, run `docker compose --profile ml up -d`, and inspect `ps` and logs. Use `http://127.0.0.1:8000`; sign in as `research` with `FLOW_ADMIN_TOKEN`. |
| Docker engine or socket initialization error | Confirm Linux-container mode. Restart Docker Desktop from its Troubleshoot menu and check `docker info` before starting Compose again. Avoid factory reset or volume deletion as ordinary recovery. |
| Disk full or recording-budget error | Check Windows free space, `docker system df`, and application `storage status`. Preview managed cleanup; preserve the database, models, protected recordings, and volume. |
| No Discord setups | Check alert settings, `signals status`, feed freshness, warm-up, gate rejections, and webhook results. There is no fixed signal schedule. |
| Pending confirmation | Required executed-flow/entry checks have not passed. Missing or interrupted flow cannot be replaced by a higher score. |
| Monitoring paused | Restore source/recording coverage and let catch-up finish. The original plan remains monitored internally. |
| ML collecting or abstaining | Inspect unique outcomes, class counts, unseen holdout shortage, compatibility, storage backpressure, and heartbeat in `ml status`. Do not weaken checks to force fitting. |
| Browser exchange access works, but feeds fail | Test REST and WebSocket access inside the container; browser DNS/proxy behavior can differ. Keep certificate verification enabled. |
| Downtime after sleep or reboot | Keep the host awake, start Docker Desktop, and resume Compose. Closing the browser is harmless; sleep interrupts collection. |

Test actual connections:

```bat
docker compose exec desk bybit-flow test-market --exchange binance
docker compose exec desk bybit-flow test-market --exchange bybit
docker compose exec desk bybit-flow test-market --exchange okx
docker compose exec desk bybit-flow test-discord
```

`test-discord` posts **BYBITFLOW CONNECTION TEST / NOT A TRADE SIGNAL** to the
signals webhook. It does not verify the separate monitoring destination or qualify
a market setup. Detailed [troubleshooting](docs/USER_RUNBOOK.md#troubleshooting)
includes administrator CMD clock commands, cache cleanup, sleep recovery, and
project-local encrypted DNS. See [Microsoft's clock settings guide](https://support.microsoft.com/en-us/windows/experience/personalization/set-time-date-and-time-zone-settings-in-windows)
and [Docker's troubleshooting guide](https://docs.docker.com/desktop/troubleshoot-and-support/troubleshoot/).

## Updates and backups

Create a uniquely named metadata backup before updating:

```bat
docker compose exec desk bybit-flow backup /app/data/research-backup-YYYYMMDD-HHMM.sqlite
docker compose cp desk:/app/data/research-backup-YYYYMMDD-HHMM.sqlite ./research-backup-YYYYMMDD-HHMM.sqlite
```

Replace `YYYYMMDD-HHMM` with the actual backup time. This backs up SQLite metadata,
not recordings or model files. A complete archive needs the entire stopped volume
and a separately protected copy of `.env`; see the [backup guide](docs/USER_RUNBOOK.md#backups).

For an ordinary update:

```bat
docker compose --profile ml stop
git pull --ff-only origin main
docker compose --profile ml up -d --build
docker compose exec desk bybit-flow doctor
docker compose exec trainer bybit-flow ml status
```

If Git reports local changes or a failed pull, resolve that before rebuilding.
For CI-gated ML deployment and continuity auditing, use the
[deployment workflow](docs/ML_RESEARCH.md#storage-and-replay-checkpoints).
Stop/update commands preserve the volume and original active plans. Do not use
`docker compose down -v`: it removes the saved data volume.

## Reference guides

- [Operator guide](docs/USER_RUNBOOK.md): configuration, horizons, troubleshooting, and backups.
- [ML research](docs/ML_RESEARCH.md): labels, algorithms, partitions, readiness, and maturity policy.
- [Primary completeness](docs/PRIMARY_COMPLETENESS.md): coverage forensics, eligibility, and completion rates.
- [Scoring](docs/SCORING.md): grade bands and evidence rubric.
- [Asset facts](docs/ASSET_FACTS.md): sourced research and instrument selection.
- [Operations](docs/OPERATIONS.md): deployment, retention, and delivery semantics.
- [Architecture](docs/SELF_HOSTED_ORDERFLOW.md) and [exchange adapters](docs/MULTI_EXCHANGE.md).
- [Contributing](CONTRIBUTING.md): development setup and verification.
- [Verification records](docs/AUTONOMY_VERIFICATION.md): dated test evidence and limitations; use live diagnostics for current runtime state.
