# BybitFlow

BybitFlow monitors crypto perpetual markets and sends Discord alerts when a setup
passes price-action, order-flow, liquidity and risk checks. Each alert includes a
direction, entry zone, stop loss and two targets. It does not place orders.

The current setups are **intraday trades, tracked for up to four hours**. A
60-second order-flow window is an entry-confirmation window, not a one-minute
holding period. Setup grades run from SSS to D; the score is not a win probability.

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
docker compose --profile ml stop
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
assumed costs, using TP1, SL or the four-hour time limit. Missing data and incomplete
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
docker compose --profile ml stop
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

A temporary feed interruption pauses monitoring; it does not establish a stop-loss hit or invalidate the price setup. The dashboard shows **MONITORING PAUSED** immediately. An interruption lasting at least one minute produces one Discord pause notice, followed by a recovery notice when fresh data returns. These updates are not new entry signals. Prices during a gap remain unverified, and new signals still require complete order-flow evidence.

Published setups are monitored until their holding deadline, independently of the shorter entry window. Fresh observed stop crossings, a changed market regime, or a known major asset event can invalidate a setup. At the holding deadline, **TRACKING ENDED** reports the end of monitoring without claiming an account result. Previously withdrawn messages are historical records and are not reactivated.
