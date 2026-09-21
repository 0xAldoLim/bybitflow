# BybitFlow

BybitFlow monitors crypto perpetual markets and sends Discord alerts when a setup
passes price-action, order-flow, liquidity and risk checks. Each alert includes a
direction, entry zone, stop loss and two targets. It does not place orders.

The engine evaluates **short intraday (15 minutes–2 hours), core intraday
(1–4 hours), swing (4–48 hours), and extended swing (2–7 days)** profiles using
shared market feeds. Extended swing initially runs as shadow research. Core
intraday preserves the original 4H/1H/15M strategy. A 60-second flow window is an
entry-confirmation window, not a one-minute holding period. Grades run from SSS
to F; the combined hand-engineered quality score is not a win probability.

Existing setups retain their original plan, score and lifecycle. Operationally
expired signals can continue lightweight research observation, but a late target
never changes an expired trade into a historical win. See the
[horizon and expiry guide](docs/USER_RUNBOOK.md#multi-horizon-research) and
[research references](docs/RESEARCH_REFERENCES.md).

## Start and stop on Windows

Open **Docker Desktop** and wait for its engine to be ready. Then open **Command
Prompt (CMD)**. These examples assume the project is in the folder below; change
the path if it is installed elsewhere.

**Start or resume the scanner and ML worker:**

```bat
cd /d "%USERPROFILE%\Documents\Codex\bybitflow"
docker compose --profile ml up -d
```

Open [the dashboard](http://127.0.0.1:8000). Sign in with username `research` and
the password stored in `FLOW_ADMIN_TOKEN` in the private `.env` file.

**Stop both services and keep recorded data and ML state:**

```bat
cd /d "%USERPROFILE%\Documents\Codex\bybitflow"
docker compose --profile ml down
```

**Check whether the services are running:**

```bat
docker compose --profile ml ps
docker compose exec desk bybit-flow doctor
```

**View recent logs:**

```bat
docker compose --profile ml logs --tail 50
```

Closing CMD or the dashboard does not stop the services. Keep Docker running and
the computer awake for continuous monitoring. Use the stop command before shutting
down when possible. Do not delete Docker's data volume to restart the program.

## First-time setup

Requirements: Docker Desktop, access to public exchange endpoints, and a Discord
channel webhook. No exchange trading keys or paid chart subscription are required.

From the project folder in CMD:

```bat
if not exist .env copy .env.example .env
notepad .env
```

Set a long private dashboard password in `FLOW_ADMIN_TOKEN` and the Discord webhook
URL in `FLOW_RESEARCH_WEBHOOK`. Never commit `.env` or share its contents.

For confirmed alerts, minute-by-minute order flow, two-stage ML research and bounded
recording storage, use these settings in `.env`:

```dotenv
FLOW_SCAN_ENABLED=true
FLOW_RESEARCH_ALERTS=true
FLOW_EXECUTION_WINDOW_SECONDS=60
FLOW_ML_ENABLED=true
FLOW_ML_TWO_STAGE=true
FLOW_ML_FILTER_RESEARCH=false
FLOW_MAX_STORAGE_GB=10
FLOW_RECORDING_RETENTION_ENABLED=true
```

Build and start both services:

```bat
docker compose --profile ml up -d --build
```

The first build downloads dependencies, including CPU-only PyTorch for LSTM
training. Later starts can use `up -d` without rebuilding. For a connectivity check:

```bat
docker compose exec desk bybit-flow test-market
```

To send an explicitly labelled Discord connection test:

```bat
docker compose exec desk bybit-flow test-discord
```

A connection test confirms webhook access; it is not a trade signal. Settings are
read at startup. Apply `.env` changes with `docker compose --profile ml up -d`.

## What to expect from alerts

A new card shows LONG or SHORT, grade, entry, SL, TP1, TP2 and the entry deadline.
Updates identify expired or withdrawn setups and say **no new entry**. A withdrawal
caused by unavailable data does not establish that the stop loss was hit.

Signals require actual order-flow confirmation even when the grade is low. They
are not sent on a fixed schedule. With the recommended minute-spread configuration, initial liquidity collection takes about
12–15 minutes; interrupted feeds can extend it. With the 60-second setting, a failed
confirmation gets a new opportunity in the next minute while the price setup is valid.

If alerts stop, check the dashboard and `doctor`. Common causes include clock skew,
exchange connection failures, incomplete market history and the storage limit.
Restarting repeatedly discards live flow history and can prolong warm-up.

## Trace missing alerts

Use the Signal journal's **Signal pipeline** panel or run:

```bat
docker compose exec desk bybit-flow signals status
```

The report separates recorded events from actual setups and shows 15-minute,
hourly, daily and process counters, confirmation checks, rejection reasons and
Discord results. Tests and synthetic cards do not count as genuine deliveries.
Rejection checks can overlap. Historical gate outcomes that were not recorded
remain unavailable.

A confirmation runtime error is a software failure. A healthy pipeline reporting
flow, entry-zone or risk rejections has evaluated those gates without passing them.
A successful connection test verifies Discord access, not setup qualification.

## How machine learning works

The optional two-stage pipeline compares these algorithms:

| Stage | Models | Purpose |
|---|---|---|
| 1 | LightGBM, Random Forest, LSTM | Estimate a setup's outcome from its recorded evidence |
| 2 | Logistic Regression, SVM, Random Forest probabilities | Combine stage-one estimates and produce a separately calibrated ranking |

LightGBM is the selected boosting implementation; XGBoost is not installed.
Stage two trains on later observations that stage one did not train on. Separate
calibration, validation and untouched test periods follow. LSTM inputs contain
16 past observations for the same venue, symbol, direction and setup family.

Learning uses saved candidate snapshots, including rejected setups, and simulated
outcomes from later recorded trades. A win means positive simulated profit after
assumed costs, using TP1, SL or the frozen horizon's time limit (four hours for
legacy/core intraday). Missing data and incomplete
fills are excluded. It does not read a personal trading account or know actual fills.

**Enabling ML starts collection and background processing, not an instantly trained
model.** Two-stage training requires at least 500 complete sequence-labelled outcomes,
plus enough data and both outcome classes in each partition. Readiness is checked
every 15 minutes; successful training cycles remain weekly. New models remain research
candidates until reviewed. There is no automatic promotion to validated probability.

With `FLOW_ML_FILTER_RESEARCH=false`, an unavailable or untrained model does not
block otherwise confirmed Discord research signals. See [ML research](docs/ML_RESEARCH.md)
for the training policy and validation requirements.

## Local storage

Market recordings and ML data are stored **locally in Docker's `research-data`
volume**. `FLOW_MAX_STORAGE_GB=10` sets the application data budget; it does not
include Docker images or installed training software.

With retention enabled and the ML worker running, cleanup starts at 8 GB and aims
back toward 6 GB after outcome processing. It removes the oldest raw recording
files while protecting at least six recent hours. Saved feature snapshots, labels,
training datasets, models and audit hashes are kept. Deleted raw history cannot be
replayed later. If protected data alone fills the budget, recording stops explicitly.

## Updating the program

Back up important data first, then run from the project folder:

```bat
docker compose --profile ml down
git pull --ff-only
docker compose --profile ml up -d --build
```

## Further reading

- [Operator guide](docs/USER_RUNBOOK.md): configuration, troubleshooting and backups.
- [ML research](docs/ML_RESEARCH.md): outcome labels, algorithms and validation.
- [Scoring](docs/SCORING.md): evidence scoring and grades.
- [Asset facts](docs/ASSET_FACTS.md): reviewed research and asset selection.
- [Contributing](CONTRIBUTING.md): development setup and tests.
- [Architecture](docs/SELF_HOSTED_ORDERFLOW.md) and [exchange adapters](docs/MULTI_EXCHANGE.md): technical design.

## Optional encrypted DNS for Docker

If exchange domains resolve incorrectly on the local network, BybitFlow can use a project-local DNS-over-HTTPS relay. It forwards DNS wire queries to [Google Public DNS](https://developers.google.com/speed/public-dns/docs/doh) using verified HTTPS. It does not fall back to unencrypted upstream DNS. Only the project containers use it; Windows and browser DNS settings stay unchanged.

After building the normal project images, enable it in Windows CMD:

```cmd
copy compose.doh.yaml compose.override.yaml
docker compose --profile ml up -d --no-build
```

The local override is ignored by Git. The relay uses the existing `bybitflow-desk:latest` image, exposes no host ports, and reserves Docker subnet `172.30.53.0/24`. Choose an unused subnet and update the DNS addresses in the override if that subnet conflicts with another network. Google receives the containers' DNS queries. DNS latency includes an HTTPS request; established exchange connections are unaffected by that lookup overhead.

The usual start and stop commands also manage the DNS service. Check it with `docker compose ps`. To disable the override without deleting it:

```cmd
ren compose.override.yaml compose.override.yaml.disabled
docker compose --profile ml up -d --no-build --remove-orphans
```

Do not disable certificate verification or pin exchange IP addresses as a workaround. The relay pins only Google's DNS bootstrap address; exchange addresses are resolved dynamically.

### Signal liquidity warm-up

For the one-minute signal configuration, set `FLOW_SPREAD_BUCKET_SECONDS=60` and `FLOW_SPREAD_WINDOW_MINUTES=30`. The scanner requires at least 12 distinct valid minute observations, at least 80% coverage, and acceptable median and tail spreads. Repeated quotes in one minute count once. A fresh start normally needs about 12–15 minutes of uninterrupted quotes; gaps may extend this. Older six-hour/five-minute histories remain separate and are not treated as minute observations. The legacy defaults remain available when these settings are omitted.

Order-flow confirmation still uses a fully observed closed minute. Setups whose confirmation window has expired are marked expired, while new minutes create independent decisions. ML training and research scores do not bypass flow, entry, spread, or risk requirements.

Selected live feeds continue generating fresh minute candidates after older candidates expire. Transient refresh errors retain the last observed context for retry; existing candle and context-age checks prevent stale data from qualifying. Refresh activity and errors are recorded under `refresh_health` for diagnostics.

### Feed interruptions after a signal

A temporary feed interruption pauses monitoring; it does not establish a stop-loss hit or invalidate the price setup. The dashboard shows **MONITORING PAUSED** immediately. An interruption lasting at least one minute produces one Discord pause notice, followed by a recovery notice only after historical catch-up and fresh live coverage both succeed. These updates are not new entry signals. Closed candles can establish conservative historical price outcomes; intrabar order flow and account fills remain unverified. New signals still require complete order-flow evidence.

Published setups are monitored until their holding deadline, independently of the shorter entry window. Fresh observed stop crossings, a changed market regime, or a known major asset event can invalidate a setup. At the holding deadline, **TRACKING ENDED** reports the end of monitoring without claiming an account result. Previously withdrawn messages are historical records and are not reactivated.

## Automatic maintenance

The scanner, recording writer, source checks, storage maintenance, calendar and
research workers recover independently. The recorder uses a bounded queue and a
separate writer process. Under pressure, optional collection is reduced before
active setups, unresolved primary outcomes and BTC/ETH evidence. Any lost data is
recorded as a coverage gap; it cannot count as complete research evidence.

Storage maintenance starts lossless compaction at 70% of the configured budget,
adds safe pruning at 85%, and enters emergency maintenance at 95%. Short renewable
range leases protect only data being read. Permanent signals, labels, model
artifacts and audit provenance remain. Explicit retention opt-out is respected;
existing `.env` files are never rewritten. If protected evidence fills the budget,
the dashboard reports backpressure rather than deleting it.

After downtime, existing setups are checked against original-exchange closed
one-minute candles before monitoring resumes. Stops, targets and deadlines retain
the original plan. Missing history keeps monitoring paused. Same-candle stop and
target ambiguity is treated conservatively; reconstructed prices are not account
fills.

New entries pause around high-impact USD events scheduled during the New York
08:00–17:00 session (DST-aware). Active setup monitoring continues. The calendar
uses [Forex Factory's weekly export](https://www.forexfactory.com/calendar), caches
schedules and reports provider failures. With no fresh cache, entry delivery is
fail-open and the calendar is visibly degraded.

New policy versions add market-conflict checks and persistent, multi-source thesis
health checks. Existing setups keep their original version. Notification clusters
provide exposure context only: every individual setup continues its own lifecycle.

ML labels are accumulated incrementally using durable restart checkpoints. Repeated
technical evaluations are excluded as independent opportunities. Horizon and
score-profile studies remain research-only; insufficient evidence means abstention.
Swing stop research does not move existing stops or automatically widen future ones.
Current Swing stop policy retained until convincing out-of-sample evidence exists.

Use `docker compose exec desk bybit-flow doctor` for health and
`docker compose exec desk bybit-flow storage status` for storage details.
Do not use `docker compose down -v` for ordinary shutdown: `-v` removes data volumes.

See [deployment verification](docs/AUTONOMY_VERIFICATION.md) for test evidence, continuity checks and current research limitations.

## Decision evidence and policy research

New decisions retain participation percentiles from prior observations of the same
market, venue, horizon and session. At least 20 prior windows are required. These
baselines sample complete flow windows from selected markets independently of
setup generation. Missing feed coverage contributes no observation.

Executed-volume profiles use complete closed 30-minute intraday or four-hour Swing
native trade windows with
explicit tick size, binning and availability time. Profile work runs outside the
confirmation loop. Missing coverage produces no profile levels. New plans may use
suitable POC, value-area or volume-node targets, subject to the existing risk gates.
Existing plans are never retargeted.

Swing path research freezes four stop policies at the decision and follows closed
one-minute candles separately from live monitoring. It records stop/target timing,
overshoot, reclaim and post-stop excursions. Ambiguous paths are excluded from
complete executable-return samples. The original live stop remains unchanged.
Confirmation and stop challengers use separate chronological partitions, purging
and an embargo. Descriptive results do not automatically promote a policy.

Feature schema `candidate-v6` adds participation percentiles. Earlier snapshots,
labels and model files remain intact. Old decisions are not recaptured under the
new schema. New training cohorts must satisfy the existing validation requirements.

An offline recorder smoke test runs in CI. For a longer test, use a separate empty
data directory, never the production directory:

```bat
python examples/recorder_soak.py --seconds 1800 --rate 1000 --data-dir recorder-soak-data
```

This tests the recorder without exchange or Discord connections. See
[v4 verification](docs/V4_VERIFICATION.md) for measured results and limitations.
