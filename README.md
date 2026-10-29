# BybitFlow

**Crypto perpetual-market research with a local dashboard and Discord alerts.**

BybitFlow monitors public Binance, Bybit and OKX market data. Confirmed LONG
and SHORT setup cards include an entry zone, stop loss, target, an entry
deadline and a holding horizon. The application records market evidence,
monitors published setups and supports optional machine-learning research.

Version 2.2 adds one Setup Score, price-response diagnostics, horizon-based
target reachability, stop viability and a fresh execution check before Discord
delivery. Cards distinguish the execution target from the structural objective.
Ranking and shared market exposure are informational; every distinct valid
opportunity may be sent, subject to notification deduplication. A delivered active
setup blocks opposite-direction alerts for the same symbol and holding horizon;
internal monitoring continues independently. Other opposing alerts are blocked
unless a shorter-horizon counter-move has fresh strong structure/flow confirmation,
a distinct frozen level, and a complete plan contained between the original stop
and target. The original must be freshly monitored and stable. Approved exceptions
explain their evidence on the card; a different horizon or higher score alone is
insufficient. There is no daily signal quota or top-N alert cap.

Observed TP1 and stop results edit the original Discord card and send one result
notification. Result posts require a successful initial card in the current
signals destination. Ambiguous delivery is not automatically resent. Price-path
results describe the published plan; they do not verify account fills or profit.
See the [v2.2 assumptions and limitations](docs/V22_SETUP_QUALITY.md).
The [fixed retrospective comparison](docs/V22_VERIFICATION.md) does not establish
improved profitability; full target-construction performance remains unmeasured.

BybitFlow does not place orders or read trading-account fills. Quality scores
and model rankings are research measures, not validated win probabilities.

[Get started](#first-time-setup) | [Start and stop](#start-stop-and-check-status) |
[Troubleshooting](#troubleshooting) | [Release notes](docs/RELEASE_NOTES.md)

Version **2.2.1 — Reliability & Evidence Loop** adds prospective shadow-policy
reports, ordered migrations, bounded operational telemetry, storage growth
forecasts and separate deployment/maintenance workflows. It preserves the v2.2
trading rules. See [architecture and evaluation](docs/RELIABILITY_EVIDENCE_LOOP.md)
and [deployment/rollback](docs/DEPLOYMENT.md).

## Features

- Public REST and WebSocket adapters with automatic primary-venue selection
  or a fixed venue.
- Intraday and swing setup research with executed-flow confirmation.
- Bounded quote-event OFI and price-response assessment, with explicit unknown states.
- Versioned execution-target selection and immutable delivery snapshots.
- Separate Discord destinations for setup cards and monitoring notices.
- An authenticated local dashboard with feed, clock, storage and delivery diagnostics.
- Bounded recording and managed retention that preserve protected evidence,
  setup plans, training labels and model artifacts.
- Optional source-specific ML collection, outcome recovery and automatic training
  when data requirements are met.

Extended-swing setups and some measurements remain research-only. See
[feature status](docs/STATUS.md) for supported behavior and limitations.

## Requirements

Install Git and Docker with the Compose plugin. Use Linux containers through
Docker Desktop, or Docker Engine with Compose on a Linux host. Public exchange
REST and WebSocket endpoints must be reachable from the containers.

Discord delivery requires permission to create two channel webhooks. No exchange
trading keys or paid chart subscription are required.

The default limits are 4 GB/two CPUs for the desk, 512 MB/one-quarter CPU for
local storage upkeep, and 2 GB/one CPU for the optional trainer. Docker and the operating system need additional resources. Begin with
the default eight deep subscriptions and measure resource use before increasing
them. Continuous collection requires an awake host and a running Docker engine.
The [memory audit](docs/MEMORY_AUDIT.md) explains application memory, file cache
and the limits of the controlled 30-market benchmark.

## First-time setup

### 1. Clone and configure

Choose any working directory. No particular parent folder or development tool
is required.

**Windows Command Prompt (CMD):**

```bat
git clone https://github.com/0xAldoLim/bybitflow.git
cd bybitflow
if not exist .env copy .env.example .env
notepad .env
```

**Windows PowerShell:**

```powershell
git clone https://github.com/0xAldoLim/bybitflow.git
Set-Location bybitflow
if (-not (Test-Path -LiteralPath .env)) { Copy-Item .env.example .env }
notepad .env
```

**Linux or macOS shell:**

```sh
git clone https://github.com/0xAldoLim/bybitflow.git
cd bybitflow
test -e .env || cp .env.example .env
```

Open `.env` in a text editor. For an existing checkout, enter its directory and
preserve the existing configuration instead of replacing it with the example.
Run subsequent Compose commands from the repository directory.

### 2. Set dashboard access and Discord destinations

Create two Discord webhooks: one for signals and setup lifecycle results, and
one for monitoring paused/resumed notices. Set these values in `.env`:

```dotenv
FLOW_ADMIN_TOKEN=replace-with-a-long-random-password
FLOW_RESEARCH_WEBHOOK=https://discord.com/api/webhooks/SIGNALS_ID/SIGNALS_TOKEN
FLOW_MONITORING_WEBHOOK=https://discord.com/api/webhooks/MONITORING_ID/MONITORING_TOKEN
FLOW_SCAN_ENABLED=true
FLOW_RESEARCH_ALERTS=true
```

Replace every placeholder with the actual password or full webhook URL. Keep
`.env` private; webhook URLs are credentials. The dashboard username is `research`.

The shipped `.env.example` enables scanning and ML but leaves Discord alerts
disabled until configured. It uses a 900-second executed-flow confirmation window
and minute-spread observations over a rolling 30-minute window.
`FLOW_EXECUTION_WINDOW_SECONDS=60` is an optional setting for minute-by-minute
confirmation; other qualification requirements still apply.

The application-data budget defaults to `FLOW_MAX_STORAGE_GB=10`, in decimal GB.
Choose a budget that fits available disk and collection volume. It does not cap
Docker images, build cache or Docker/WSL virtual-disk allocation.
See the [configuration reference](docs/USER_RUNBOOK.md#configuration) for other settings.

### 3. Build and start

For the dashboard, scanner and ML worker:

```sh
docker compose --profile ml up -d --build
docker compose --profile ml ps
```

The first build downloads application and training dependencies, including
CPU-only PyTorch for the trainer image. ML is optional: use
`docker compose up -d --build` to start only the default services.

Open [the local dashboard](http://127.0.0.1:8000), sign in as `research` and use
`FLOW_ADMIN_TOKEN` as the password. The listener is local to the host by default.
For remote deployment and capacity planning, see [operations](docs/OPERATIONS.md).

### 4. Verify the installation

```sh
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow signals status
docker compose exec trainer bybit-flow ml status
```

The last command requires the ML worker. Check feed coverage, recording health
and observation times; a running container does not establish fresh evidence
for every selected market.

To test the signals webhook:

```sh
docker compose exec desk bybit-flow test-discord
```

This posts **BYBITFLOW CONNECTION TEST / NOT A TRADE SIGNAL**. It does not test the
separate monitoring webhook or generate a real setup.

Discord endpoint connectivity is checked automatically using read-only requests,
usually every five minutes. Connection failures retry with backoff; Discord rate
limits are respected. These checks do not send cards or change delivery receipts.
An earlier unconfirmed delivery remains in history even after connectivity returns.
A reachable endpoint does not prove that the earlier card arrived or that writes
are permitted; rejected and rate-limited writes remain visible until a later
confirmed write succeeds.

Storage warnings distinguish usage near the configured budget from recording
being stopped. Automatic safe retention preserves the database, models and
protected evidence. A failed maintenance cycle is reported separately and retries
automatically. Retention cannot guarantee indefinite recording when permanent or
protected data fills the configured budget; the limit is never expanded silently.

## Start, stop, and check status

Run these commands from the repository directory with Docker running. They work
in CMD, PowerShell and Linux/macOS shells.

| Action | Command |
|---|---|
| Start or resume scanner and dashboard | `docker compose up -d` |
| Start or resume with the ML worker | `docker compose --profile ml up -d` |
| Stop services and preserve data | `docker compose --profile ml stop` |
| Check containers | `docker compose --profile ml ps` |
| Follow recent logs; Ctrl+C exits log viewing | `docker compose --profile ml logs --tail 100 -f` |
| Check version and deployment metadata | `docker compose exec desk bybit-flow version` |
| Check feeds, recording and diagnostics | `docker compose exec desk bybit-flow doctor` |
| Inspect signal qualification and delivery | `docker compose exec desk bybit-flow signals status` |
| Inspect prospective policy observations | `docker compose exec desk bybit-flow forward-report` |
| Inspect ML readiness and worker status | `docker compose exec trainer bybit-flow ml status` |
| Inspect storage and retention | `docker compose exec desk bybit-flow storage status` |

Closing the terminal or browser does not stop containers. Host sleep, shutdown
and an unavailable Docker engine interrupt collection. The restart policy resumes
eligible containers when Docker starts; it does not prevent sleep or restart
services deliberately stopped with Compose.

Apply `.env` changes with `docker compose --profile ml up -d`; omit
`--profile ml` when operating without the trainer. A plain container
restart does not reload changed environment values. Repeated restarts can prolong
flow warm-up.

## Understand Discord cards

| Destination | Messages |
|---|---|
| `FLOW_RESEARCH_WEBHOOK` | New setups, related setup context, entry expiry, withdrawal, targets and other setup lifecycle results |
| `FLOW_MONITORING_WEBHOOK` | Monitoring paused and monitoring resumed notices |

Terminal results edit the original setup card, preserving its plan and first-posted
time. Each setup needs its own successfully delivered initial card before it can
produce lifecycle updates. Grouped or suppressed notifications do not merge the
underlying setups or change their independent monitoring.

Temporary feed loss pauses monitoring; it does not establish a stop or target.
Monitoring resumes after historical catch-up and fresh coverage are available.

| Horizon | Expected holding period | Publication |
|---|---|---|
| Short intraday | 15 minutes-2 hours | Confirmed research setup |
| Core intraday | 1-4 hours | Confirmed research setup |
| Swing | 4-48 hours | Confirmed research setup |
| Extended swing | 2-7 days | Shadow research; no initial Discord setup |

The entry deadline is separate from the holding deadline. A short confirmation
window does not imply a short holding period. Grades range from SSS to F; every
grade must pass the applicable structure, flow, liquidity, freshness, entry and
risk checks. See [scoring](docs/SCORING.md) and the
[horizon guide](docs/USER_RUNBOOK.md#multi-horizon-research).

There is no fixed alert schedule. With the example minute-spread configuration,
initial qualification needs at least 12 valid minute observations and 80% coverage.
Clock corrections, missing quotes, discovery and candle-history loading can extend
warm-up. A healthy scanner can also reject all current opportunities.

## ML collection and training

ML is optional research functionality. It does not automatically change strategy
rules, lower signal thresholds or promote models. The example keeps
`FLOW_ML_FILTER_RESEARCH=false`, so ML abstention does not block otherwise
qualified alerts.

| Track | Evidence | Use |
|---|---|---|
| Primary | Current `candidate-v20` decisions and complete local or verified historical trade paths | Preferred compatible model, subject to admission and promotion checks |
| Monitored | Current decision features and original-venue one-minute candle outcomes | Advisory research with separate OHLC-proxy provenance |
| Bootstrap | Supported historical feature projection and original-venue candle outcomes | Advisory research with separate schema and OHLC-proxy provenance |

Downtime does not erase saved plans or learning records. The trainer can recover
eligible candle outcomes after services resume. For Binance, it can also recover
consecutive aggregate trades and replay the same primary entry, participation,
cost and exit rules. Existing complete labels and decision-time flow features remain
unchanged. Recovery saves progress and durable retry times, so an unavailable old
path cannot block newer setups or longer horizons. Missing history is retried;
invalid frozen plans, ambiguous ordering and unverifiable outcomes remain excluded.

Primary historical trade replay currently supports Binance only and uses a
bounded 128 MB archive cache. Bybit/OKX primary trade replay and larger archives
are not supported by that recovery path. Candle recovery remains a separate track.

Each track needs **500 complete unique outcomes per source**, plus compatible
features, valid chronological partitions and adequate class coverage. Millions
of recorded events are not millions of training examples. Readiness refreshes
every 15 minutes; eligible training starts automatically on a worker loop.

The baseline compares Logistic Regression and LightGBM. Optional two-stage research
uses LightGBM, Random Forest and LSTM, followed by Logistic Regression, SVM and
Random Forest probabilities. Model compatibility, holdout and promotion checks
remain in force. See [ML research](docs/ML_RESEARCH.md) for algorithms, limitations
and recovery status fields.

## Storage and cleanup

The default `maintenance` service processes existing recorded outcomes and runs
safe retention independently of ML training. It has no network access and does
not train or promote models. Pausing `trainer` therefore does not pause required
storage upkeep. It retries busy database work and processes bounded chronological
batches, retaining unresolved evidence and the existing completeness rules.

```sh
docker compose ps maintenance
docker compose logs --tail 30 maintenance
```


Application data lives in the persistent `research-data` Docker volume. Stopping
services preserves it. Managed retention releases eligible raw recordings while
keeping setup plans, snapshots, labels, datasets, model artifacts and delivery
receipts. Active setups, unresolved outcomes and replay leases protect required
evidence. Older horizon-research exports are compressed losslessly in bounded
background batches; the current export and model artifacts remain protected.

Preview cleanup before running it manually:

```sh
docker compose exec desk bybit-flow storage status
docker compose exec desk bybit-flow storage cleanup --dry-run
docker compose exec desk bybit-flow storage cleanup
```

Cleanup may find no eligible data to remove. Permanent records continue to grow
and require capacity planning. If protected data fills the budget, recording can
pause rather than delete it. Deleted raw history cannot be replayed again.

Do not use `docker compose down -v` to stop the application: it deletes the saved
volume. See [managed storage](docs/USER_RUNBOOK.md#managed-storage) for retention,
compaction and Docker disk-space guidance.

## Troubleshooting

Start with container status, `doctor` and recent logs.

| Symptom | What to check |
|---|---|
| Measured clock drift or future timestamps | Synchronize the host clock. On Windows, use **Settings > Time & language > Date & time > Sync now**, then run `doctor` again. |
| Clock check delayed, unavailable or interrupted | Check exchange connectivity and latency. These messages do not establish drift; repeated synchronization does not repair a network timeout. |
| Dashboard unavailable | Confirm Docker is running, resume Compose, check logs and open `http://127.0.0.1:8000`. |
| Dashboard sign-in fails | Use username `research` and the configured `FLOW_ADMIN_TOKEN`. Recreate services after environment changes. |
| Docker engine or socket error | Run `docker info` first. See the [socket-error guide](docs/USER_RUNBOOK.md#docker-engine-unavailable-or-socket-initialization-failure) for the specific Windows runtime-socket error. |
| Disk full or recording-budget warning | Check host free space, `docker system df` and application storage status. Preview managed cleanup and preserve protected evidence. |
| No new Discord setups | Check alert configuration, warm-up, feed coverage, qualification reasons and delivery status. A connection test does not qualify a trade. |
| Spread history rebuilding | Allow valid quotes to accumulate. The example configuration requires 12 minute observations and 80% coverage. |
| Pending confirmation | Required flow or entry evidence has not passed; a higher score cannot replace it. |
| Monitoring paused | Restore source/recording coverage and allow catch-up. The saved plan remains internally monitored. |
| ML collecting or abstaining | Inspect outcomes, schema/source compatibility, partitions, unseen holdouts, storage and heartbeat in `ml status`. |
| Exchange website works but feeds fail | Test container access; browser DNS/proxy behavior can differ. Keep TLS verification enabled. |
| Downtime after sleep or reboot | Start Docker, resume Compose and check recovery. Continuous monitoring requires an awake host. |

Test public exchange connections with:

```sh
docker compose exec desk bybit-flow test-market --exchange binance
docker compose exec desk bybit-flow test-market --exchange bybit
docker compose exec desk bybit-flow test-market --exchange okx
```

See [operator troubleshooting](docs/USER_RUNBOOK.md#troubleshooting) for clock
synchronization, free-space checks, DNS and Docker recovery. For a bug report,
include the application version, redacted diagnostics and relevant log timestamps.
Never include `.env`, passwords or webhook tokens.

## Updates and backups

Back up before updating. The `backup` command copies SQLite metadata, not the
entire volume, and requires enough free space for another database copy. A full
backup must include recordings and model files, with credentials protected
separately. Follow the [backup guide](docs/USER_RUNBOOK.md#backups).

After a verified backup, update from the repository directory:

```sh
git pull --ff-only origin main
docker compose --profile ml up -d --build
docker compose exec desk bybit-flow doctor
docker compose exec trainer bybit-flow ml status
```

Resolve a failed pull before rebuilding. Updating briefly interrupts changed
services but preserves the volume and original active plans. Keep the Compose
project name and volume mapping consistent. The ML status command requires the
trainer; omit `--profile ml` from the update command when operating without it.
See [build provenance](docs/USER_RUNBOOK.md#record-the-build-commit) and the
[advanced deployment guide](docs/ML_RESEARCH.md#storage-and-replay-checkpoints).

## Documentation and contributing

- [Operator guide](docs/USER_RUNBOOK.md): configuration, recovery and troubleshooting.
- [ML research](docs/ML_RESEARCH.md): labeling, training and evidence requirements.
- [Scoring](docs/SCORING.md): grade bands and evidence interpretation.
- [Asset facts](docs/ASSET_FACTS.md): sourced research and instrument selection.
- [Operations](docs/OPERATIONS.md): deployment, delivery and data lifecycle.
- [Architecture](docs/SELF_HOSTED_ORDERFLOW.md) and [exchange adapters](docs/MULTI_EXCHANGE.md).
- [Primary completeness](docs/PRIMARY_COMPLETENESS.md): coverage forensics.
- [Release notes](docs/RELEASE_NOTES.md): implementation history and research limits.
- [Contributing](CONTRIBUTING.md): development setup and verification.
- [Verification records](docs/AUTONOMY_VERIFICATION.md): dated evidence, not current deployment health.

BybitFlow is distributed under the [MIT license](LICENSE). Exchange data and
third-party dependencies remain subject to their respective terms and licenses.
