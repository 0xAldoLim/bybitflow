# Operator guide

Use the [README](../README.md) for installation and everyday CMD commands. This
guide explains configuration, recovery, and the limits of the recorded evidence.
Commands assume the terminal is in the repository folder, with Docker running.

## Configuration

Copy `.env.example` only for a new installation. Existing installations should
edit selected settings without replacing credentials or their data budget.

| Setting | Implemented behavior |
|---|---|
| `FLOW_ADMIN_TOKEN` | Required private dashboard password; HTTP Basic username `research` |
| `FLOW_RESEARCH_WEBHOOK` | Signals and setup lifecycle destination |
| `FLOW_MONITORING_WEBHOOK` | Monitoring pause/resume destination |
| `FLOW_RESEARCH_ALERTS=true` | Enable confirmed SSS–F research cards; mandatory gates still apply |
| `FLOW_SSS_RESEARCH=true` | SSS-only opt-in when broader research alerts are disabled |
| `FLOW_SCAN_ENABLED=true` | Enable scanning and collection |
| `FLOW_MARKET_SOURCE=auto` | Select a working primary from Binance, Bybit, and OKX |
| `FLOW_DEEP_SYMBOLS` | Deep subscription capacity; default 8, maximum 30 |
| `FLOW_EXECUTION_WINDOW_SECONDS` | Complete flow window; default 900, set 60 for minute confirmation |
| `FLOW_SPREAD_BUCKET_SECONDS=60` / `FLOW_SPREAD_WINDOW_MINUTES=30` | Minute-spread baseline; included in `.env.example` |
| `FLOW_ML_ENABLED=true` | Enable labeling, training, and compatible inference in the ML worker/desk |
| `FLOW_ML_TWO_STAGE=true` | Optional sequence collection and two-stage research; default false |
| `FLOW_ML_FILTER_RESEARCH=false` | Keep ML abstention/acceptance from filtering research signals |
| `FLOW_MAX_STORAGE_GB` | Application-data budget in decimal GB; default 10 |
| `FLOW_RECORDING_RETENTION_ENABLED=true` | Enable protected-range-aware raw retention |
| `FLOW_ML_RAW_RETENTION_MINUTES` | Routine worker retention floor; default 60 minutes |

Apply changed environment settings with `docker compose --profile ml up -d`.
A plain container restart retains the old environment. Do not print `.env` or
paste webhook URLs into public troubleshooting reports.

The default Compose limits are 4 GB/two CPUs for the desk and 2 GB/one CPU for
the trainer. These are container limits, not a promise of total host/WSL usage.
The dashboard binds to `127.0.0.1:8000`. Use one collector and at most one ML
worker per data directory.

## Startup and verification

```bat
docker compose --profile ml up -d --build
docker compose --profile ml ps
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow signals status
docker compose exec trainer bybit-flow ml status
```

Use `doctor --json` to save structured diagnostics. `doctor` reports probes and
cached runtime state; a healthy probe alone does not establish continuity for
every selected symbol. Check required active feeds, recorder health, heartbeat,
storage, and observation times.

Laptop downtime does not discard saved setups. After `docker compose --profile ml
up -d`, lifecycle monitoring checks the missing interval against the original
exchange's historical candles. The trainer independently records eligible
`monitored-ohlc-v1` outcomes without requiring continuous local trade recording.
Check `monitored_backfill` and `monitored_trainability` in ML status. Missing
history is retried, and ambiguous paths stay excluded. Notifications are sent
after the application returns; nothing runs or sends alerts while the laptop
is shut down. See [monitored learning](ML_RESEARCH.md#monitored-setups-and-laptop-downtime).

```bat
docker compose exec desk bybit-flow test-market --exchange binance
docker compose exec desk bybit-flow test-market --exchange bybit
docker compose exec desk bybit-flow test-market --exchange okx
docker compose exec desk bybit-flow test-discord
```

`test-market` checks public REST and real trade WebSocket reception. `test-discord`
posts **BYBITFLOW CONNECTION TEST / NOT A TRADE SIGNAL** to the signals webhook.
It does not test the monitoring webhook. The optional `test-signal` command sends
an explicitly synthetic pipeline/format test. Neither test is a real opportunity
or an ML outcome; neither proves profitable performance.

## Multi-horizon research

| Profile | Context / setup / execution bars | Holding period | Publication |
|---|---|---|---|
| SHORT_INTRADAY | 1H / 15M / 5M | 15 minutes–2 hours | Confirmed research setups |
| CORE_INTRADAY | 4H / 1H / 15M | 1–4 hours | Confirmed research setups |
| SWING | 1D / 4H / 1H | 4–48 hours | Confirmed research setups |
| EXTENDED_SWING | 1D / 4H / 1H | 2–7 days | Shadow research; no initial Discord alert |
| LEGACY | Original frozen configuration | Original deadline | Original lifecycle |

The executed-flow confirmation window is distinct from these candle timeframes
and holding periods. Entry expiry does not extend when research observation
continues after the operational deadline. Each setup retains its frozen plan,
score, horizon, source, and policy through restarts and notification clustering.
Duplicate or related setups remain independently monitored internally.

Grades are absolute: SSS 95+, SS 90+, S 85+, A 75+, B 65+, C 50+, D 35+,
E 20+, F below 20. Mandatory rejections apply to every grade. The [scoring guide](SCORING.md)
describes the seven evidence categories. Missing observations earn no invented
credit; the quality score is separate from opportunity priority and ML ranking.

`FLOW_HORIZON_PROFILES` accepts a JSON list. `FLOW_POST_TERMINAL_ENABLED` enables
lightweight late-outcome research. `FLOW_POST_TERMINAL_CHECKPOINTS` accepts increasing
checkpoint minutes keyed by profile. These research checkpoints do not extend
operational deadlines, reactivate expired entries, or change primary ML labels.

Late closed-candle observations can describe favorable/adverse moves after an
expiry. They do not retroactively make the original trade a win. At the holding
deadline, an observed entry and reliable original-venue price can support a paper
net-R estimate even without TP1. Missing price/path evidence leaves the result
unknown. A manual Paper journal close remains separate from simulated outcomes.

## Collection and Discord delivery

Contracts must pass age, turnover, spread, liquidity, and history checks. The
minute-spread configuration needs at least 12 distinct valid minute observations
and 80% coverage; repeated quotes in one bucket count once. Warm-up usually takes
12–15 uninterrupted minutes after subscription, with additional discovery/candle
loading time possible. Omitting those settings uses the legacy five-minute,
six-hour spread history.

With `FLOW_EXECUTION_WINDOW_SECONDS=60`, closed minutes create independent
confirmation opportunities while the price setup remains valid. A complete
executed-flow window, fresh book, family-specific trigger, entry, and risk checks
still apply. Missing flow cannot qualify. Broad scans use a configured delay after
completion; they are separate from selected-symbol live candidate refresh.

New confirmations also face applicable market alignment, macro, and V8 gates.
High-impact USD calendar pauses apply around scheduled events in the DST-aware
New York session. Active monitoring continues. Provider failure with no fresh
calendar cache is visibly degraded and fail-open. Optional missing V8 features
fall back to prior qualification rather than supplying fabricated evidence.
No automatic threshold tuning is implemented.

The durable outbox must show `sent` with the exact setup's initial message ID
before any lifecycle Discord update is allowed. Known initial destinations must
match the current signals webhook. Suppressed, uncertain, dry-run, disabled, or
never-delivered setups keep internal monitoring but return
`blocked:no-visible-initial` for user-facing updates. A mention inside another
card does not establish delivery for a child setup.

Terminal updates edit the original signals card. Monitoring pause/resume notices
use the monitoring webhook, keyed by setup and outage episode. Catch-up must
finish before resumption; a terminal result found during catch-up does not first
send a resume notice. Do not clear delivery records to force resends. See
[delivery semantics](OPERATIONS.md#discord-delivery-semantics).

## Managed storage

The `research-data` volume contains application data, including metadata backups.
Its Compose name normally has a project prefix. Reusing the checkout/project name
preserves the intended volume. Docker images, build cache, and virtual-disk size
are outside `FLOW_MAX_STORAGE_GB`.

| Mechanism | Threshold / action |
|---|---|
| Maturity health display | Healthy below 80%; warning 80–90%; pressure 90–95%; backpressure at 95% |
| Independent maintenance | Packing at 70%; safe pressure pruning at 85%; emergency maintenance at 95% |
| Worker retention | Release eligible finalized raw data in bounded batches; retain at least 60 recent minutes by default |
| Pressure retention | At 85%, the recent raw floor may shorten to 15 minutes; protected ranges remain |
| Training/bootstrap guard | At 95%, defer fitting and historical bootstrap downloads |
| Recorder budget guard | Try safe cleanup, then stop recording if required evidence cannot fit |

These health thresholds and maintenance triggers serve different purposes. Packing
preserves verified bytes/hashes inside archives. Pruning releases only eligible
evidence. Active plans, unresolved primary labels, checkpoints, and reader leases
protect their required ranges. Snapshots, labels, datasets, models, and audit
provenance remain permanent.

For active setups, already-processed raw history can be released after durable
monitoring progress is saved. Cleanup keeps 15 minutes of overlap and all
unprocessed history. Missing or invalid progress retains the original interval.
Unresolved ML outcomes and reader leases can still protect older recordings.
This preserves each original plan and its restart/reconciliation path.

```bat
docker compose exec desk bybit-flow storage status
docker compose exec desk bybit-flow storage cleanup --dry-run
docker compose exec desk bybit-flow storage cleanup
docker compose exec desk bybit-flow storage compact
```

Manual cleanup normally reports no deletion below 85% unless resuming a prior
prune. Its ordinary recent floor is six hours; pressure cleanup at 85% can use
the 15-minute floor. Worker cleanup below pressure uses the configured routine
floor. Retention opt-out is respected.

Incomplete ML outcomes are excluded from fitting and can release raw evidence
once no operational setup or reader needs it. Clean pending outcomes retain
protection. Deleting consumed raw recordings does not delete learned models,
but those recordings cannot be replayed again. Permanent records themselves can
eventually require capacity planning; retention is not unlimited storage.

## Troubleshooting

### Clock skew or future timestamps

1. Open Windows **Settings → Time & language → Date & time**.
2. Enable **Set time automatically**, select the correct time zone, and click
   **Sync now** under clock synchronization.
3. Run `docker compose exec desk bybit-flow doctor` again and inspect exchange
   `clock_skew_ms` and live-feed freshness.

For CMD recovery, open **Command Prompt as administrator**:

```bat
net start w32time
w32tm /resync
w32tm /query /status
```

An already-running service message is harmless. If no time source is available,
check network/time-service policy rather than repeatedly restarting BybitFlow.
If Windows is synchronized but container time remains wrong, restart Docker
Desktop once, resume Compose, and recheck. A corrected clock does not repair
previously recorded clock-damaged evidence; affected flow must warm up again.
See [Microsoft's Windows Time documentation](https://learn.microsoft.com/en-us/windows-server/networking/windows-time-service/windows-time-service-tools-and-settings).

### Disk full or application storage limit

Check both physical free space and the application budget:

```bat
docker system df
docker compose exec desk bybit-flow storage status
docker compose exec desk bybit-flow storage cleanup --dry-run
```

Free unrelated downloads/temporary files through Windows **Settings → System →
Storage**. Review Docker's usage before removing anything. To reclaim dangling
build cache, run the following and review Docker's confirmation prompt:

```bat
docker builder prune
```

This removes build cache, not BybitFlow's data volume, and future builds may need
downloads again. Review and run managed `storage cleanup`/`storage compact` for
eligible application evidence. If cleanup reports protected history, let the
worker finish outcomes or investigate its blocker. Do not delete SQLite, model
files, Docker/WSL disks, active recordings, or the volume to force headroom.
Do not use `down -v`, `volume prune`, or factory reset as a space-recovery shortcut.
Docker virtual-disk allocation may not shrink immediately when internal files are
removed. See [Docker disk usage](https://docs.docker.com/reference/cli/docker/system/df/)
and [build-cache cleanup](https://docs.docker.com/reference/cli/docker/builder/prune/).

### Docker engine unavailable or socket initialization failure

This includes Docker Desktop startup errors involving `dockerInference` or
`docker-secrets-engine/engine.sock`; the application cannot start until the engine
works. Confirm Linux-container mode, restart Docker Desktop from its Troubleshoot
menu, then run:

```bat
docker info
docker compose --profile ml up -d
docker compose --profile ml ps
```

If the engine still fails, gather Docker Desktop diagnostics and consult its
[troubleshooting guide](https://docs.docker.com/desktop/troubleshoot-and-support/troubleshoot/).
Avoid deleting socket folders, factory-resetting, or changing unrelated Windows
services without diagnosing the engine error. Restarting Docker Desktop interrupts
other containers too.

### Dashboard unavailable or authentication fails

Check `ps`, desk logs, Docker availability, and port 8000. Open
`http://127.0.0.1:8000`, not a remote/public URL. Use username `research` and the
current `.env` password. A missing `FLOW_ADMIN_TOKEN` blocks Compose startup.
After changing it, recreate services with `up -d` before signing in again.

### No signals, pending confirmation, or monitoring paused

```bat
docker compose exec desk bybit-flow signals status
docker compose exec desk bybit-flow doctor
docker compose --profile ml logs --tail 100 desk
```

Inspect warm-up, required stream freshness, recorder gaps, confirmation checks,
gate rejection reasons, and actual Discord attempts. `FLOW_RESEARCH_ALERTS=true`
and a valid `FLOW_RESEARCH_WEBHOOK` are needed for broader research delivery.
`test-discord` confirms signals-webhook access only. A connection test cannot
force a real setup. Terminal results may be edits of an older card.

Pending confirmation means required evidence has not passed. A pause after an
alert retains the plan and requires historical catch-up plus fresh data. Do not
clear an outbox, fabricate evidence, or weaken risk checks to generate a message.

### Exchange access, DNS, or certificate errors

Run `test-market --exchange binance`, `bybit`, and `okx` separately. Browser access
does not prove container REST/WebSocket access. Check host time, DNS, Docker proxy
settings, firewall/network filtering, and provider availability. Keep TLS
verification enabled and do not pin exchange IPs. If one primary fails, auto mode
can select another working venue; active plans still use their original source.

An optional project-local DNS-over-HTTPS relay is described below. It can help
incorrect DNS resolution; it does not guarantee access through filtering or fix
exchange outages. Normal scan failures retry after the attempt completes; recovery
may require fresh spread/flow history.

### ML remains collecting, not ready, or abstaining

Inspect `docker compose exec trainer bybit-flow ml status` and trainer logs.
Millions of events do not replace complete independent outcomes. Check source,
schema, class counts, causal partitions, unseen holdout shortage, compatibility,
storage guard, and worker state. A heartbeat alone does not mean ML is enabled:
`FLOW_ML_ENABLED=false` leaves the worker idle. Restarting or manually fitting
cannot create missing evidence. See [ML readiness](ML_RESEARCH.md#training).

### Downtime after sleep or reboot

Keep Docker running and the host awake while monitoring. In Windows power settings,
choose an appropriate plugged-in sleep policy; turning the screen off is separate
from sleep. Configure Docker Desktop startup if automatic recovery after sign-in
is desired, and verify services afterward. `restart: unless-stopped` cannot prevent
host sleep or resume a service deliberately stopped with Compose.

After downtime, resume with `docker compose --profile ml up -d`. Original setups
are reconciled against closed one-minute candles from their original venue.
Missing history keeps monitoring paused; same-candle ambiguity is conservative.
No downtime recovery establishes an actual account fill.

## Optional encrypted DNS

The relay in `compose.doh.yaml` uses verified HTTPS to Google Public DNS and does
not fall back to unencrypted upstream DNS. It affects these project containers,
not Windows/browser DNS. Google receives their queries. It reserves
`172.30.53.0/24`; use an unused subnet if that conflicts with another Docker network.

For a checkout without an existing `compose.override.yaml`, run in CMD:

```bat
docker compose -p bybitflow build desk
copy compose.doh.yaml compose.override.yaml
docker compose --profile ml up -d --no-build
```

The build command supplies the `bybitflow-desk:latest` tag used by the relay; it
does not create containers or replace volumes. If an override already exists,
merge the DNS configuration rather than overwriting it. The local override is
ignored by Git. Check the `dns` service with `docker compose ps`.

To disable a DNS-only override, provided the destination filename does not exist:

```bat
ren compose.override.yaml compose.override.yaml.disabled
docker compose --profile ml up -d --no-build --remove-orphans
```

For a merged override, remove only its DNS sections instead. Review other orphan
services before using `--remove-orphans`. See [Google's DoH protocol](https://developers.google.com/speed/public-dns/docs/doh).

## Stop, restart, and update

```bat
docker compose --profile ml stop
docker compose --profile ml up -d
docker compose --profile ml logs --tail 100
```

The first command stops services without deleting data; the second resumes them.
Closing the dashboard or terminal does not stop them. For an update, back up,
stop, run `git pull --ff-only origin main`, then rebuild with
`docker compose --profile ml up -d --build`. Resolve a failed pull before rebuilding.
Run `doctor` and `ml status` afterward. Preserve the Compose project name/volume;
do not run `docker compose down -v`.

The CI-gated PowerShell deployment script also audits existing active plans and
delivery receipts and preserves the maturity epoch. See [ML deployment](ML_RESEARCH.md#storage-and-replay-checkpoints).

## Backups

For an online metadata backup, choose a new filename:

```bat
docker compose exec desk bybit-flow backup /app/data/research-backup-YYYYMMDD-HHMM.sqlite
docker compose cp desk:/app/data/research-backup-YYYYMMDD-HHMM.sqlite ./research-backup-YYYYMMDD-HHMM.sqlite
```

Replace the timestamp placeholder. The backup command refuses to overwrite an
existing destination. A metadata backup excludes raw recordings, packs, datasets,
and model files. Stop both writers for a complete volume archive and protect
`.env` separately. Verify hashes and restore into a separate volume before
replacing an installation. Keep outbox and manifests with their artifacts.
See [operations](OPERATIONS.md#backups-and-migrations).
