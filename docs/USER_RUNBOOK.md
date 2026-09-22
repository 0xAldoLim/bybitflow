# Operator guide

For everyday Windows start/stop commands, begin with the [README](../README.md).
This guide covers configuration and recovery in more detail.

## Requirements

- Docker Desktop on Windows, or Docker Engine with Compose on Linux.
- A Discord webhook for the research channel.
- Network access to the selected exchange's public REST and WebSocket endpoints.
- Persistent storage and an awake host during collection.

The dashboard binds to `127.0.0.1:8000`. The default Compose limits are 2 GB per
service, two CPUs for the desk, and one CPU for the trainer.

## Configuration

Copy `.env.example` to `.env`. Existing installations should update selected
settings without replacing their credentials.

| Setting | Purpose |
|---|---|
| `FLOW_ADMIN_TOKEN` | Required dashboard password; username is `research` |
| `FLOW_RESEARCH_WEBHOOK` | Discord research-channel webhook |
| `FLOW_RESEARCH_ALERTS=true` | Enable confirmed SSS–F research cards; mandatory rejection still blocks delivery |
| `FLOW_SSS_RESEARCH=true` | Enable SSS-only cards when broader research alerts are disabled |
| `FLOW_SCAN_ENABLED=true` | Enable public-data collection and scanning |
| `FLOW_MARKET_SOURCE=auto` | Probe Binance, Bybit, then OKX and retain a working primary |
| `FLOW_ML_ENABLED=true` | Enable decision-time ML inference when a compatible model exists |
| `FLOW_ML_TWO_STAGE=true` | Collect LSTM sequences and compare the two-stage research models |
| `FLOW_ML_FILTER_RESEARCH=false` | Keep ML acceptance from filtering research alerts |
| `FLOW_DEEP_SYMBOLS` | Concurrent deep subscription capacity; default 8, maximum 30 |
| `FLOW_EXECUTION_WINDOW_SECONDS` | Required complete executed-flow window; default 900, set 60 for minute-by-minute confirmation |

`examples/ml-top30.env` provides non-secret settings for broader coverage.
[Asset facts](ASSET_FACTS.md) describes the dated research pack and symbol selection.

Settings are read at startup. Apply changes with:

```sh
docker compose --profile ml up -d
```

## Startup and verification

```sh
docker compose --profile ml up -d --build
docker compose --profile ml ps
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow test-market
docker compose exec desk bybit-flow test-discord
docker compose exec desk bybit-flow test-signal
```

Open [the dashboard](http://127.0.0.1:8000). The Discord test is labelled as a
connection test and does not represent a trade setup.

`doctor` reports configuration, storage, database, source probes, and recorded
runtime health. `test-market` verifies REST and actual trade WebSocket reception.
A configured webhook or passing offline test does not establish external delivery.

## Multi-horizon research

| Profile | Context / setup / execution | Expected holding | Publication |
|---|---|---|---|
| SHORT_INTRADAY | 1H / 15M / 5M | 15 minutes–2 hours | Confirmed research alerts |
| CORE_INTRADAY | 4H / 1H / 15M | 1–4 hours | Original strategy remains supported |
| SWING | 1D / 4H / 1H | 4–48 hours | Confirmed research alerts |
| EXTENDED_SWING | 1D / 4H / 1H | 2–7 days | Shadow research initially; no automatic public SSS |
| LEGACY | Original frozen configuration | Original deadline | Original lifecycle continues |

The scanner evaluates each configured profile with its corresponding closed bars,
using the same public streams and candle cache. A minute of executed flow remains
the confirmation requirement when configured; it does not set the holding period.
Each horizon receives its own immutable candidate and one combined quality score.
Related plans have a thesis ID. Near-identical plans are suppressed at delivery;
materially different entry/stop/target plans may receive separate alerts. The
cooldown suppresses substantially repeated plans; legacy signals retain their
original symbol cooldown. Longer holding periods reserve more funding.
Extended-swing observations do not bypass validation by receiving a high score.

`FLOW_HORIZON_PROFILES` accepts a JSON list of enabled profiles. Core intraday
remains the baseline strategy. `FLOW_POST_TERMINAL_ENABLED` controls the lightweight
worker. `FLOW_POST_TERMINAL_CHECKPOINTS` can override research checkpoint minutes
from creation, for example `{"CORE_INTRADAY":[480,720,1440,2880]}`. Checkpoints are
research policies, not optimized holding-time promises.

Default research checkpoints are 4/8/24 hours for short intraday; 8/12/24/48 hours
for core intraday; 72 hours/5 days/7 days for swing; 10/14 days for extended swing.
The original operational deadline is stored separately and never extended by a
research checkpoint.

### Existing signal continuity

Before the horizon schema is created, the application makes an SQLite backup.
Original serialized signals and their checksums are preserved in `signal_origins`.
Historical terminal payloads are not rewritten. Older active records load as
LEGACY, retain their plan and score, and continue through their original lifecycle.
Restart is not an invalidation event, and the persistent Discord outbox prevents
resending the original entry alert. No account fills are inferred from public data.

### WHAT IF A SIGNAL EXPIRES BUT PRICE LATER REACHES THE TARGET?

The original trade remains expired. Operational status and research observation
status are separate: an EXPIRED setup can be FOLLOWING_LATE_OUTCOME, then COMPLETE.
Compact closed one-minute candles record later favorable/adverse excursions,
target/stop visits, elapsed times, sessions and checkpoints. They do not reactivate
the trade or send a late move as an ordinary target-hit alert.

The extended same-rules policy ignores the original time deadline while retaining
the original stop and target. Stop before target means STOP, even if the target
is reached later. If a candle touches both, stop wins conservatively. Missing
coverage cannot establish an extended winner. Directional afterlife separately
records favorable movement even after a stop. Price observations are bounds,
not verified account executions.

Primary model labels remain separate from `research_labels`. Late results are
labels available only after observation, never inputs at the original decision.
The pooled horizon research summary initially abstains below 100 completed,
coverage-qualified family observations; it does not change production horizons or
old signals. Timing classifications use deterministic rules and retain UNCLEAR
when the data cannot distinguish poor entry from an overly tight stop.

### Sessions and one combined score

UTC timestamps are classified through IANA zones: Asia/Singapore 08:00–16:00,
Europe/London 08:00–16:00, America/New_York 08:00–17:00. London and New York DST
changes are handled by timezone data; overlap and off-session periods are explicit.
Moving into another session does not invalidate an active thesis.

The original seven score categories remain. Location, auction/value, sustained
order flow, book pressure, factor-adjusted residuals and execution noise refine
their evidence. Short intraday places more emphasis on sustained book/flow timing;
longer horizons penalize accumulated funding more. Missing data earns no invented
evidence. Quality tiers are SSS 95+, SS 90+, S 85+, A 75+, B 65+, C 50+, D 35+,
E 20+, F below 20 or mandatory rejection. Thresholds are absolute rather than
daily ranks. High quality does not override risk, coverage or execution gates.

ML retains the regularized logistic baseline and the configured two-stage
LightGBM/Random Forest/LSTM → logistic/SVM/probability-forest research pipeline.
Training uses chronological partitions, purging and embargo. The worker resolves
paper labels independently of model fitting. A bounded rolling training window
does not delete older records. Models are not automatically called validated just
because training succeeds. Probability and expected R are shown as UNCALIBRATED
until independent qualification passes. SSS RESEARCH remains distinct from a
statistically validated SSS recommendation.

## Managed storage

The outcome worker checks recording integrity before replay. Its progress appears
in ML diagnostics. Completed labels are immutable. If a recording gap covers the
rest of an elapsed holding period, the worker records an incomplete outcome rather
than waiting indefinitely or inventing an exit. Incomplete outcomes never become
training wins or losses. Decisions still within their holding period remain pending.

Replay dispatches prints by venue and symbol and processes deadlines in time order.
Order-book envelopes remain integrity-checked but are not replayed as price fills.
Historical feature snapshots and labels survive raw-recording cleanup.

The application budget remains `FLOW_MAX_STORAGE_GB=10`. It includes application
data and metadata backups, not Docker images or the Docker virtual-disk allocation.
Do not raise the limit repeatedly or delete the volume to recover space.

Permanent data includes signals, original plans, features, labels, models,
experiments and provenance. Raw event/depth segments are temporary only after
their evidence is no longer required. Active setups, unresolved primary labels,
replay/training leases and unprocessed records block unsafe deletion. Post-terminal
research uses compact candles/checkpoints and releases the need for full DOM.

Normal operation is below 70%; 70–85% triggers lossless packing; 85–95% permits
oldest-safe raw cleanup; above 95% requires aggressive *safe* cleanup. The recorder
stops if the budget cannot be respected without losing required evidence. This
condition is operational, not permission to fabricate missing confirmation.
Segment packing preserves original compressed bytes, hashes and manifests inside
verified archives; replay can read packed segments. New recordings rotate at
15 seconds or 10,000 rows by default, with a memory-pressure flush, rather than
creating a tiny segment for each hundred events.

From CMD in the project directory:

```bat
docker compose exec desk bybit-flow storage status
docker compose exec desk bybit-flow storage cleanup --dry-run
docker compose exec desk bybit-flow storage cleanup
docker compose exec desk bybit-flow storage compact
```

Dry-run reports candidate segment/archive IDs, the protection cutoff, the reason
and bytes that would be freed. SQLite history and model state are not cleanup
targets. A replay lease protects evidence while a worker is processing it.

`test-discord` verifies external delivery with BYBITFLOW CONNECTION TEST / NOT A
TRADE SIGNAL. `test-signal` exercises a synthetic candidate, score, horizon, risk
and formatting before sending TEST SIGNAL / NOT A REAL TRADE. Neither enters the
signal or ML datasets. Passing these tests is separate from receiving an actual
market-confirmed setup, and neither proves profitable performance.

## Collection and alerts

Eligible contracts require sufficient history and turnover. Spread qualification
requires at least 12 observed five-minute buckets and coverage checks. Execution
confirmation requires a complete closed trade window and a fresh book. With
`FLOW_EXECUTION_WINDOW_SECONDS=60`, the scanner evaluates a new immutable candidate
each minute using the preceding complete minute of executed trades. The 4H/1H/15M
price setup, required family-specific order-flow trigger, entry zone and risk gates
still apply. Missing or interrupted flow cannot confirm a signal. A prior minute's
failed confirmation does not prevent the next minute from qualifying. Fast decisions
must be evaluated within two minutes of window close. Discord cooldowns still apply.
This policy has a separate strategy version; incompatible ML models abstain until
trained on that version. Lower latency does not establish predictive accuracy.
With FLOW_SPREAD_BUCKET_SECONDS=60 and FLOW_SPREAD_WINDOW_MINUTES=30, the spread baseline requires at least 12 distinct valid minute samples and 80% coverage. Initial warm-up normally takes 12–15 minutes; gaps can extend it. The legacy five-minute/six-hour defaults remain available when these settings are omitted. A healthy process alone does not prove that a signal has enough evidence to confirm.

The Compose setup uses a five-minute delay between broad scans. Each closed
15-minute execution window receives a distinct candidate ID under rules-0.2.0,
while repeated scans of the same window remain deduplicated. Native feeds recover
per symbol; one stale coin does not reset the other coins' retained history.
Source changes and interruptions reset affected continuity.

Cards include direction, setup, quality, entry zone, stop, TP1, TP2, and risk
information. Grades do not override confirmation or freshness checks. A quiet
channel can indicate warmup, unavailable data, or no qualifying setup.

The model worker monitors labels every 15 minutes. Two-stage mode checks training
readiness every 15 minutes; the original tabular mode retries daily. Successful
research-model cycles run weekly. A new installation can collect
data without a model. See [ML research](ML_RESEARCH.md) for readiness and approval.

## Recording retention

`FLOW_RECORDING_RETENTION_ENABLED=true` enables raw-data cleanup in the ML worker
after successful outcome processing. At 85% of `FLOW_MAX_STORAGE_GB`, it removes
the oldest raw, normalized and sidecar segment files toward 60% usage. At least
six recent hours are protected; older active plans, unresolved labels and replay
leases can extend that protection. The database,
feature snapshots, labels, training datasets, models and segment hashes remain.
Removed raw history cannot be replayed again; unlabelled decisions before the
retention boundary are excluded rather than assigned invented outcomes. The
worker must remain enabled. If protected history or preserved ML artifacts alone
fill the budget, recording stops explicitly rather than deleting those records.

## Stop, restart, and update

In Windows CMD, first enter the repository directory. Replace
`C:\path\to\bybitflow` with the folder where you cloned or extracted this
repository:

```bat
cd /d C:\path\to\bybitflow
```

Use these commands for everyday operation:

```bat
REM Stop both services; preserve data.
docker compose --profile ml stop

REM Resume.
docker compose --profile ml up -d

REM Inspect recent logs.
docker compose --profile ml logs --tail 100
```

Closing the dashboard or terminal does not stop containers. Windows sleep interrupts
collection. For an update, create a backup, stop the services, pull the intended
revision with `git pull --ff-only`, and rebuild with
`docker compose --profile ml up -d --build`.

## Connectivity recovery

Use Exchange health and `test-market` to identify the failing source. Certificate
hostname mismatches indicate that the endpoint cannot be authenticated; check DNS,
network filtering, and the network provider. Keep TLS verification enabled.

Scan failures retry within 60 seconds after an attempt completes. Normal successful
scans use the configured interval. Correcting connectivity allows automatic
requalification and fresh-data warmup.

## Backups and retention

The persistent `research-data` volume contains SQLite metadata, segments, and ML
artifacts. Credentials remain in the private `.env`.

For an online metadata backup:

```sh
docker compose exec desk bybit-flow backup /app/data/research-backup.sqlite
docker compose cp desk:/app/data/research-backup.sqlite ./research-backup.sqlite
```

Use a new backup filename each time. A metadata backup alone excludes raw recordings
and model files. For a complete archive, stop both writers and back up the entire
volume plus an encrypted copy of the environment configuration. Verify file hashes
and restore into a separate volume before replacing an installation.

`bybit-flow retention-plan` reports archive candidates; it does not delete data.
Keep the outbox and recording manifests with their related artifacts.
See [operations](OPERATIONS.md) for recovery and delivery semantics.

## Multi-horizon discovery and notification hardening

Stage A1 ranks short intraday, core intraday and swing opportunities independently
using closed candles, spread and liquidity. The best supported priority determines
shortlisting; these ranks are not quality scores or probabilities. Stage A2 checks
real visible depth for protected active/core symbols, exploit and exploration
selections, then reserves only as needed. `FLOW_STAGE_A_DEPTH_CANDIDATES=0` derives
reserve capacity as three times deep capacity. Existing active feeds remain protected.

New versions use explicit context/setup/execution timeframe provenance. Existing
plans, scores, horizons and deadlines are unchanged. Fundamentals now separate
source coverage from an explicit sourced quality/risk assessment; unassessed facts
provide no favorable quality. Severe adverse evidence reduces credit. The five
cross-market points primarily use beta-adjusted residual evidence, with a capped
fallback when factors are unavailable. The seven category weights still total 100.

An economic plan has an `alert-fingerprint-v1` identity independent of evaluation
timestamps. SQLite atomically claims it before HTTP delivery. Existing outbox
idempotency remains. Ambiguous delivery is not automatically retried. Candidate
reevaluations remain in history but contribute at most one independent opportunity
to future ML datasets. Different horizons and families retain separate research data.

Thesis clustering affects **notification presentation only**. A stronger related
plan can become the primary displayed thesis, but it never replaces, pauses,
rescales, merges or terminates either original setup. Related horizons receive
compact confirmations; materially different plans can receive separate cards;
opposite directions are labelled as conflicting. These cards do not imply multiple
full positions. Existing active alerts are seeded individually, without retroactive
clustering. `/api/delivery` and the Signals page report real initial delivery metrics,
excluding connection/synthetic tests and lifecycle-only updates.

Late-outcome research joins immutable decision-time snapshots to separate completed
post-terminal labels. Future labels never enter decision inputs or rewrite a primary
expiry. The pooled logistic horizon experiment uses chronological train/validation/
test partitions, outcome purging, a one-day embargo and a reserved holdout. Its
minimum is `FLOW_HORIZON_MODEL_MIN_SAMPLES` (default 500). Below that it reports
`INSUFFICIENT_EVIDENCE` and no recommendation. It remains research-only, with no
production promotion or authority to change live plans.

Retention is enabled by default for new installations. Explicit
`FLOW_RECORDING_RETENTION_ENABLED=false` still disables it. Upgrades do not edit
an existing `.env`. Storage status includes retention state, raw/permanent/protected/
deletable bytes, usage percentage, cleanup history and cumulative reclaimed space.
Active setups and unresolved primary recording outcomes remain
protected. At budget pressure the recorder attempts safe cleanup before opening its
storage circuit breaker. Late observations use compact candle checkpoints rather than pinning raw depth for days.


## Signal pipeline diagnostics

Run `docker compose exec desk bybit-flow signals status` or open the Signal journal.
Recorded exchange events are separate from generated setups. The panel reports
confirmation checks, rejection reasons and genuine Discord attempts over recent
windows. Tests and synthetic cards are excluded from genuine delivery counts.
A runtime-error state requires attention; healthy flow or risk rejections are
recorded strategy decisions. A successful Discord test does not bypass those gates.

ML dashboard summaries are materialized by the worker and include their observation
time. Participation baselines need 20 prior complete windows from the same market,
venue, horizon and session. Swing path and confirmation challengers remain research
until their independent sample and chronological validation requirements are met.
Existing signals keep their original entry, stop, targets and deadlines throughout
these experiments. See [v4 verification](V4_VERIFICATION.md).
