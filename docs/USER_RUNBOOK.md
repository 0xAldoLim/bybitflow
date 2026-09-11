# BybitFlow operator runbook

This is the authoritative guide for the exchange-native installation on `main`.
You are installing an alerts-only research application, not an order-execution bot.
Follow the platform section, then connection tests, warmup and ML sections in order.
No strategy, probability or profitability has been validated on real data here.

## WHAT YOU NEED BEFORE STARTING

A quiet Discord channel does not establish a delivery failure. Check `doctor`,
scanner eligibility and actual feed freshness. Failed scans retry within 60 seconds
after the failed attempt finishes; successful scans use the configured scan interval.
Keep collection running through spread and execution warmup. Missing reviewed
asset facts still cap quality below the SSS research threshold. Do not add filler
facts or lower gates to force an alert.

If ML reports non-monotonic recording receipt time, preserve the original segment
and manifest. Clock synchronization cannot repair past bytes. Any audited quarantine
must retain the original hashes and expose the missing-chain gap during replay;
never sort timestamps or treat affected outcomes as complete.

You personally need:

- Windows 11 with Docker Desktop, or Linux/Ubuntu workstation/VPS with Docker Engine.
- Git; Docker Compose v2 (included with Docker Desktop, plugin on Ubuntu).
- A Discord account and a server where you can create a private channel and manage webhooks.
- A public internet connection lawfully permitted to reach at least one supported exchange.
- Suggested initial capacity: 2 vCPU, 4 GB RAM, 30–80 GB disk. Multi-venue collection and
  simultaneous ML training may need more. This is a starting budget, not a measured load guarantee.
- Enough free disk for recordings and a separate backup destination.

You do NOT need TradingView, GoCharting, Pine Script, exchange trading keys, exchange
passwords, deposited trading funds, a domain, public HTTPS ingress or paid market data.
You execute any trades yourself, separately. Signals from one venue are not guaranteed
to be executable at the same price, fees, specifications or liquidity on another exchange.

## 1. Create your Discord destination

1. Open or create your Discord server.
2. Create a private text channel named `bybitflow-research`; set its members/roles as desired.
3. Open **Server Settings**.
4. Select **Integrations**.
5. Select **Webhooks**.
6. Choose **New Webhook / Create Webhook**.
7. Select the `bybitflow-research` channel and save.
8. Copy its webhook URL.
9. Save it locally as `FLOW_RESEARCH_WEBHOOK` in `.env` below.

If you cannot see Webhooks, ask your server administrator for permission. Never paste
the URL into chat, an issue, a screenshot, a commit or a public repository. It permits
posting to that channel. If exposed, delete/regenerate it in Discord and update `.env`.
An optional separate operations channel uses `FLOW_OPS_WEBHOOK`. Validated alerts use
`FLOW_DISCORD_WEBHOOK`, but that variable does not unlock validation.

## 2A. Windows 11 — PowerShell

Install Git and Docker Desktop if absent (Windows Package Manager):

```powershell
winget install --id Git.Git -e
winget install --id Docker.DockerDesktop -e
```

Start Docker Desktop. Complete its WSL2/virtualization setup and reboot if requested.
See [Docker's Windows prerequisites](https://docs.docker.com/desktop/setup/install/windows-install/)
if virtualization or WSL setup fails.
Open a new PowerShell window after installation. You do not need WSL shell commands
for this guide. Verify the engine, not just the command-line client:

```powershell
git --version
docker version
docker compose version
docker info
git clone https://github.com/0xAldoLim/bybitflow.git
Set-Location bybitflow
git switch main
git pull --ff-only origin main
git status
if (!(Test-Path -LiteralPath .env)) { Copy-Item .env.example .env }
```

Preserve an existing `.env` and dashboard password. For a new installation, generate
the dashboard password directly into the local file without printing it:

```powershell
$flowBytes = New-Object byte[] 32
$flowRng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$flowRng.GetBytes($flowBytes)
$flowRng.Dispose()
$flowSecret = [Convert]::ToBase64String($flowBytes)
(Get-Content .env) -replace '^FLOW_ADMIN_TOKEN=.*$', "FLOW_ADMIN_TOKEN=$flowSecret" | Set-Content -Encoding ascii .env
Remove-Variable flowSecret,flowBytes
notepad .env
```

Set `FLOW_RESEARCH_WEBHOOK` to your copied URL. Leave `FLOW_MARKET_SOURCE=auto` and
`FLOW_SCAN_ENABLED=true`. Read the SSS section before optionally enabling research alerts.
The password in the file is your dashboard login; keep the file private.

```powershell
docker compose up -d --build
docker compose ps
docker compose logs --tail 100
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow doctor --json
docker compose exec desk bybit-flow test-market
docker compose exec desk bybit-flow test-discord
Start-Process http://127.0.0.1:8000
```

Log in as `research` with the `FLOW_ADMIN_TOKEN` password. Docker Desktop must stay running.
Keep the computer awake for continuous collection; sleep interrupts feeds and affected
windows must warm up again. Check available memory before also starting the ML worker.

If Desktop aborts before creating its Linux engine pipe, inspect its local backend log
under `%LOCALAPPDATA%\Docker\log\host`. On one Windows 11 installation, error 1920
named stale `Docker\run\dockerInference` and `docker-secrets-engine\engine.sock`
runtime sockets. Preserving/renaming their containing runtime directories while Desktop
was fully stopped allowed startup. This is a targeted recovery, not general cleanup:
inspect the exact paths and contents first, preserve both affected directories in the
same stopped interval, and obtain approval before interrupting unrelated Docker work.
Do not reset to factory defaults, delete WSL disks/volumes, or change security settings
to address this symptom. See the [upstream report](https://github.com/docker/desktop-feedback/issues/531).

```powershell
# Stop without deleting data; resume or restart.
docker compose stop
docker compose start
docker compose restart desk
# After editing environment values, recreate instead of merely restarting.
docker compose up -d --build
```

## 2B. Ubuntu / Linux / VPS

Use Ubuntu with a supported Docker Engine installation. The distribution package route
below may vary with your Ubuntu release; verify that Compose v2 is installed. If a package
is unavailable, follow [Docker's official Ubuntu installation instructions](https://docs.docker.com/engine/install/ubuntu/) rather than
running an unreviewed third-party install script.

```sh
sudo apt update
sudo apt install git docker.io docker-compose-v2
sudo systemctl enable --now docker
sudo docker version
sudo docker compose version
git clone https://github.com/0xAldoLim/bybitflow.git
cd bybitflow
git switch main
git pull --ff-only origin main
git status
cp .env.example .env
chmod 600 .env
```

Edit `.env` with `nano .env`. Generate a random dashboard password using
`openssl rand -hex 32` on your own terminal and paste it into `FLOW_ADMIN_TOKEN`.
Set `FLOW_RESEARCH_WEBHOOK` locally. Keep the file out of Git. Operators who deliberately
join the `docker` group may omit `sudo`, but that group grants root-equivalent host access.

```sh
sudo docker compose up -d --build
sudo docker compose ps
sudo docker compose logs --tail 100
sudo docker compose exec desk bybit-flow doctor
sudo docker compose exec desk bybit-flow doctor --json
sudo docker compose exec desk bybit-flow test-market
sudo docker compose exec desk bybit-flow test-discord
```

On a VPS, leave TCP 8000 closed publicly. Compose binds it to `127.0.0.1`. Use your
existing SSH port/firewall rules; do not enable a new firewall without allowing your
actual SSH port first. Outbound HTTPS and the exchanges' public WS ports must be allowed
(OKX uses TCP 8443). No inbound Discord port is needed.

From your personal computer:

```sh
ssh -L 8000:127.0.0.1:8000 user@server
```

Keep that connection open and visit http://127.0.0.1:8000 locally. Authentication remains
required. A public domain/reverse proxy is optional and not necessary for ordinary use.

## 3. Safe first-run environment

The full `.env.example` is safe: collection is enabled, notification opt-ins are off.
This minimal starter is also sufficient after replacing only the local placeholders:

```dotenv
FLOW_MARKET_SOURCE=auto
FLOW_SCAN_ENABLED=true
FLOW_ADMIN_TOKEN=<your-generated-random-dashboard-password>
FLOW_RESEARCH_WEBHOOK=<your-private-Discord-webhook-URL>
FLOW_RESEARCH_ALERTS=false
FLOW_SSS_RESEARCH=false
FLOW_ML_ENABLED=false
FLOW_ML_FILTER_RESEARCH=false
FLOW_EQUITY=
```

Do not literally keep the angle-bracket placeholders. `FLOW_DATA_DIR` is `/app/data`
inside Compose regardless of the host `.env` path; the named volume persists it.
Lists use JSON syntax, such as `["BTCUSDT","ETHUSDT"]`. Changes require container
recreation with `docker compose up -d --build`. Do not paste `.env` output into support requests.

### Environment reference

Defaults below are Python defaults; Compose/`.env.example` explicitly enable scanning.
No exchange key/password configuration exists. Legacy TV variables are optional only.

#### REQUIRED FIRST RUN

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_ADMIN_TOKEN` | Docker | `empty` | new random secret | Dashboard password; username research |
| `FLOW_RESEARCH_WEBHOOK` | For Discord | `empty` | your private webhook | Private research channel webhook |

#### MARKET DATA

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_MARKET_SOURCE` | No | `auto` | auto | auto, binance, bybit, okx or multi; transparent public source choice |
| `FLOW_SCAN_ENABLED` | No | `false` | true (Compose) | Continuous scanner/recorder; Compose example enables it |
| `FLOW_SCAN_SECONDS` | No | `900` | 900 | Broad scan interval in seconds |
| `FLOW_SETTLE_COINS` | No | `["USDT"]` | ["USDT"] | Bybit settlement choices; Binance/OKX adapters USDT only |
| `FLOW_CORE_WATCHLIST` | No | `["BTCUSDT","ETHUSDT"]` | ["BTCUSDT","ETHUSDT"] | Priority canonical symbols; multi peers use first two |
| `FLOW_DEEP_SYMBOLS` | No | `8` | 8 | Primary subscription budget |
| `FLOW_MIN_AGE_DAYS` | No | `30` | 30 | Minimum listing age |
| `FLOW_MIN_DAILY_TURNOVER` | No | `20000000` | 20000000 | Trailing 7-day median daily quote turnover gate |
| `FLOW_MAX_SPREAD_BPS` | No | `5` | 5 | Maximum normal/current spread |
| `FLOW_QUOTE_SAMPLE_SECONDS` | No | `60` | 60 | Public quote polling cadence |
| `FLOW_SPREAD_MIN_SAMPLES` | No | `12` | 12 | Minimum distinct 5-minute quote buckets |
| `FLOW_BOOK_DEPTH` | No | `50` | 50 | Bybit subscription depth; other venues use audited depth |
| `FLOW_BOOK_STALE_MS` | No | `5000` | 5000 | Book maximum age |
| `FLOW_TRADE_STALE_MS` | No | `15000` | 15000 | Trade maximum age |
| `FLOW_REST_REQUESTS_PER_SECOND` | No | `3` | 3 | Per-adapter REST request pacing |

#### DISCORD

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_RESEARCH_ALERTS` | No | `false` | false | Explicit broader unvalidated research opt-in |
| `FLOW_SSS_RESEARCH` | No | `false` | false | Explicit ≥95 all-gates SSS RESEARCH opt-in |
| `FLOW_DISCORD_WEBHOOK` | No | `empty` | empty | Approved-model channel; no validation bypass |
| `FLOW_OPS_WEBHOOK` | No | `empty` | empty | Separate operational Discord channel |
| `FLOW_COOLDOWN_MINUTES` | No | `240` | 240 | Per-symbol initial alert cooldown |

#### ML

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_ML_ENABLED` | No | `false` | false | Shadow/approved inference; never trains in API |
| `FLOW_ML_FILTER_RESEARCH` | No | `false` | false | Permit research model to reject candidates |
| `FLOW_VALIDATED_ALERT_TIERS` | No | `["SSS"]` | ["SSS"] | Allowed grades only with approved evidence |

#### RISK

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_HYPOTHETICAL_NOTIONAL` | No | `1000` | 1000 | Illustrative order size without account equity |
| `FLOW_EQUITY` | No | `unset` | unset | Optional manual account equity; requires fresh portfolio snapshot |
| `FLOW_RISK_FRACTION` | No | `0.0025` | 0.0025 | Fraction of equity risked, not conviction-linked |
| `FLOW_DAILY_LOSS_LIMIT` | No | `0.015` | 0.015 | Manual portfolio daily loss fraction ceiling |
| `FLOW_WEEKLY_LOSS_LIMIT` | No | `0.04` | 0.04 | Manual portfolio weekly loss fraction ceiling |
| `FLOW_CORRELATED_RISK_LIMIT` | No | `0.0075` | 0.0075 | Aggregate crypto risk ceiling |
| `FLOW_LEVERAGE` | No | `3` | 3 | Illustrative ceiling; Binance missing brackets caps at 1x |
| `FLOW_MAINTENANCE_MARGIN_ASSUMPTION` | No | `0.01` | 0.01 | Stress assumption, not exact exchange liquidation |
| `FLOW_LIQUIDATION_BUFFER_MULTIPLE` | No | `3` | 3 | Minimum stop/stress buffer |
| `FLOW_MIN_NET_RR` | No | `2` | 2 | Minimum after-cost planned reward:risk |
| `FLOW_TAKER_FEE_BPS` | No | `5.5` | 5.5 | Assumed fee per side; configure actual applicable rate |
| `FLOW_SLIPPAGE_BPS` | No | `2` | 2 | Assumed adverse slippage per side |
| `FLOW_FUNDING_RESERVE_BPS` | No | `3` | 3 | Minimum assumed funding reserve |

#### STORAGE

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_DATA_DIR` | No | `data` | data | Host/native data directory; Compose fixes /app/data |
| `FLOW_QUEUE_SIZE` | No | `2000` | 2000 | Maximum queued raw envelopes |
| `FLOW_QUEUE_BYTE_LIMIT` | No | `32000000` | 32000000 | Maximum queued payload bytes |
| `FLOW_TAPE_MAX_TRADES` | No | `50000` | 50000 | Retained reported executions per symbol |
| `FLOW_MAX_STORAGE_GB` | No | `10` | 10 | Recorder disk quota; archive before exhaustion |
| `FLOW_RAW_RETENTION_DAYS` | No | `14` | 14 | Retention report age; no automatic deletion |

#### REMOTE DASHBOARD

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_DASHBOARD_URL` | No | `http://127.0.0.1:8000` | http://127.0.0.1:8000 | Dashboard link on Discord cards |

#### ADVANCED — OPTIONAL LEGACY ONLY

| Variable | Required? | Default | Safe first value | Purpose |
|---|---|---|---|---|
| `FLOW_TV_ENABLED` | No | `false` | false | Legacy TV ingress; not required |
| `FLOW_TV_PROXY_RESEARCH` | No | `false` | false | Legacy classified-volume proxy opt-in |
| `FLOW_TV_REQUIRE_NATIVE_CONFIRMATION` | No | `false` | false | Legacy TV native-confirmation gate |
| `FLOW_TV_TOKEN` | No | `empty` | empty | Legacy ingress secret, unused in native path |
| `FLOW_TV_SYMBOLS` | No | `["BTCUSDT","ETHUSDT"]` | ["BTCUSDT","ETHUSDT"] | Legacy ingress allowlist |
| `FLOW_TV_MAX_AGE_MS` | No | `90000` | 90000 | Legacy ingress age bound |
| `FLOW_TV_QUEUE_LIMIT` | No | `1000` | 1000 | Legacy durable ingress queue bound |

## 4. First start and connection tests

```sh
docker compose up -d --build
docker compose ps
docker compose logs --tail 100
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow doctor --json
docker compose exec desk bybit-flow test-market
docker compose exec desk bybit-flow test-discord
```

`desk` should be running. A healthy application check does not itself prove market
coverage; inspect doctor and live data health. All exchanges failing is an actionable
market-data outage, not permission to trade from stale data. First image builds can
take time depending on downloads. No exact startup completion time is promised.

The CLI can also be run as `bybit-flow doctor`, `bybit-flow test-market`, etc. after
a native Python installation. In Docker always use `docker compose exec desk` as above.

Doctor reports configuration, SQLite quick-check, writable storage/free bytes, actual
public probes, persisted scanner/runtime/stream state, recorder segment count, Discord
configuration, feature-store/registry access and champion state. `NOT_TESTED` is not
healthy. `NOT_OBSERVED_OR_STALE` means no recent collector heartbeat. `CONFIGURED` does
not mean Discord delivery succeeded. Probe timestamps are distinct from continuous feed
health. Missing model/champion is normal initially. Disk below 2 GB triggers a warning.

Test a particular venue independently:

```sh
docker compose exec desk bybit-flow test-market --exchange binance
docker compose exec desk bybit-flow test-market --exchange bybit
docker compose exec desk bybit-flow test-market --exchange okx
```

A successful market test prints the source, symbol, genuine public trade sample,
exchange/receipt times and freshness. It authenticates to no account and submits no order.
Failure exits nonzero. OKX needs actual contract metadata to normalize contract size;
if REST metadata fails, its normalized trade test cannot be established either.

A successful Discord test must appear in your actual private channel as:

```text
BYBITFLOW CONNECTION TEST
NOT A TRADE SIGNAL
UTC timestamp · application version · configured source · last diagnostic health
```

This test works without enabling research signals. It creates only a delivery-attempt
record, never a candidate, paper position or training outcome. Check the channel yourself.
`sent` means the webhook returned success; ambiguous timeouts are not blindly retried.
There is no configured developer webhook, so development mocks do not establish your delivery.

## 5. What happens during warmup

The app discovers actual instruments, verifies listing/contract metadata and 30 contiguous
completed daily bars, checks seven-day median turnover, collects normal spread samples,
prepares closed 4H/1H/15M features, selects deep symbols, reconstructs books and records
executed trades. Core/pending collection may begin before a long broad scan finishes.

Normal spread needs at least 12 observed five-minute buckets with adequate coverage:
roughly an hour is the minimum under continuous sampling, not a guarantee of eligibility.
A complete closed 15M trade window after subscription is required; depending on alignment
this takes at least one full window and can take longer. Reconnects, missing prints, stale
books, tape retention overflow or recorder gaps restart/block affected confirmation.
Long profiles need actual longer retained history; daily/weekly values may remain unavailable.

Broad scanning can take minutes at the default request budget. Check data health rather
than assuming completion after a fixed timer. A selective strategy may produce no signals
for hours or days. Even after warmup, sourced asset facts, market structure, native flow
and costs may prevent a 95-point score. Do not enter invented facts to force an alert.

## 6. First real research signal and lifecycle

`WATCHLIST → PENDING CONFIRMATION → CONFIRMED → ALERTED → INVALIDATED / EXPIRED / RESOLVED`

Candidates are generated internally from liquidity sweep, trend pullback, range rejection
or breakout/retest rules. Each side/family remains separately labeled. WATCHLIST is a plan;
PENDING needs real confirmation; CONFIRMED passes the mandatory research decision gates;
ALERTED means Discord received a card, not that you placed a trade. Invalidation can follow
price, regime, spread, known-event or required-feed failure. Expiration closes the entry
opportunity. RESOLVED is a manual paper journal state; manual outcomes do not train the model.

Timing is separate: current closed 15M execution trigger valid for 15 minutes; 1H setup
expires one hour after its setup close; paper holding policy is at most four hours from
the frozen execution decision. The first fully covered decision per setup ID is immutable.
Rejected decisions remain research data; a later materially new setup gets a new ID.
Recently alerted symbols remain subscription candidates through their holding deadline.
There is a 240-minute default symbol cooldown and persistent deduplication.

Synthetic layout only — not a current trade:

```text
SSS RESEARCH · UNCALIBRATED · EXAMPLEUSDT · LONG
Source: <actual venue> · trend_pullback · 4H/1H/15M
Quality: <measured score>/100 · Probability: Uncalibrated
Entry zone / stop / TP1 / TP2: <actual generated plan>
Net RR / costs: <risk calculation with stated assumptions>
Delta / CVD / stacks / absorption / PoC / VA: <covered executions>
Potential trapped buyers/sellers: <heuristic; inventory unknown>
OI/funding/liquidations: <actual observations or unavailable>
Why / invalidation / expiration / signal ID / model version
```

Stops, size and margin are hypothetical. Gaps can exceed planned losses. The bot never
increases leverage because of a grade. If you configure equity, enter a current manual
portfolio snapshot under Settings; it expires after 24 hours and is required for sizing.

## 7. Enable SSS RESEARCH deliberately

Set these values in your local `.env`, then recreate the container:

```dotenv
FLOW_RESEARCH_WEBHOOK=<your-private-research-channel-webhook>
FLOW_SSS_RESEARCH=true
FLOW_RESEARCH_ALERTS=false
```

This permits only native research cards with raw quality ≥95 and all required structure,
covered executed-flow, fresh book, normal spread, actual depth, entry-zone, defined stop,
minimum net RR, estimated costs, funding freshness/crowding, event and portfolio-risk gates.
No missing core feature can be replaced by unrelated points. Verified manual asset facts
contribute diligence coverage, not token intrinsic value. `FLOW_RESEARCH_ALERTS=true` is
a separate broader research opt-in and can send qualifying lower-score candidates; leave
it false if you want SSS-only research.

The native rubric allocates 10 points to source-attributed diligence. Without any such
facts its theoretical maximum is 90, even with perfect other components. This is not
automatic token research and the app does not verify a claim merely because it has a URL.
To record genuinely reviewed information, open **Research & experiments → Record a
reviewed asset fact** in the dashboard. Use the base asset (BTC, not BTCUSDT), a source
you have read, a precise definition, the actual observation and limitations, known/effective
times in UTC, and a review-expiration time. Categories earning coverage are
`economic_purpose`, `value_accrual`, `dilution`, `security`, and `governance`. A documented
absence of token-holder value capture is legitimate context, not a positive valuation.
Mark a major event only when it invalidates strategy assumptions; that blocks affected
setups. The form stores user-reviewed facts with the current collection time, cannot
retroactively change past decisions, and does not create a signal. If you cannot substantiate
a category, leave it missing and accept the lower score. Do not invent facts to reach SSS.

SSS RESEARCH is not validated SSS. No trained model is needed to collect deterministic
research evidence. While uncalibrated, public probability and expected net R stay null.
Optional ML shadow/filtering never weakens risk gates. It may abstain or reject.

## 8. Machine learning: collection to challenger

Candidate snapshots accumulate even with ML inference disabled. Generation snapshots
include candidates that never qualify; the first covered scored decision captures accepted
and rejected candidates. Missing data stays missing. Source/schema-separated exports preserve
exact decision-time features; manual journal numbers and unresolved/incomplete fills are excluded.

Start the separate label/drift/training worker when you have a functioning recorder:

```sh
docker compose --profile ml up -d --build
docker compose exec desk bybit-flow ml status
docker compose --profile ml logs --tail 100 trainer
```

It monitors labels/drift approximately every 15 minutes and attempts training weekly. It
abstains without sufficient real evidence. It does not retrain after every trade or auto-promote.

For an explicit bounded research cycle (do not run concurrently with the worker):

```sh
docker compose --profile ml stop trainer
docker compose --profile ml run --rm trainer bybit-flow ml cycle
docker compose --profile ml up -d trainer
```

Manual commands, replacing paths/IDs with the actual printed values:

```sh
docker compose exec desk bybit-flow ml label /app/data/segments/<first>.jsonl.gz /app/data/segments/<next>.jsonl.gz
docker compose exec desk bybit-flow ml export --source binance
docker compose --profile ml run --rm trainer bybit-flow ml train /app/data/ml/datasets/<hash>.parquet --model logistic
docker compose --profile ml run --rm trainer bybit-flow ml train /app/data/ml/datasets/<NEW-unseen-data-hash>.parquet --model both
docker compose exec desk bybit-flow ml status
docker compose exec desk bybit-flow ml drift <model-id>
```

Use all relevant ordered recording segments. The worker discovers files automatically;
you do not need to expand Unix wildcards from PowerShell. `--source bybit` or `--source okx`
selects that venue instead. Old feature schema snapshots remain stored but new exports use
the current schema. Do not rerun different model searches on a consumed holdout; a new
filename is not new evidence. One `--model both` run compares models on development data
before consuming its untouched final period.

Training requires at least 200 training, 100 separate calibration, 100 validation and 100
holdout labels after purging/embargo; actual calendar coverage/class diversity can demand
more. Sigmoid starts at 100 calibration examples; isotonic needs 1,000. Counts alone do not
qualify live deployment. Small/one-class/overlapping/incomplete datasets abstain.

After an actual challenger exists, set `FLOW_ML_ENABLED=true` and recreate `desk` for
shadow inference. Set `FLOW_ML_FILTER_RESEARCH=true` only if you deliberately want its
research acceptance thresholds to reject candidates. Neither enables validated labels.
No untrusted pickle artifact can be loaded: application registry models use hash-checked JSON.

Manual approval/rollback commands exist but refuse missing evidence:

```sh
docker compose exec desk bybit-flow ml promote <model-id> --reviewer "Your name"
docker compose exec desk bybit-flow ml rollback --reviewer "Your name"
```

The dashboard's ML Research page shows snapshot/label counts, cycle time, challengers,
champion, validation/holdout performance, reliability, importance, family/regime/tier
breakdowns, thresholds and promotion/drift history. More restrictive does not mean improved.
No model is a normal first-run state. Do not generate test outcomes to inflate counts.

## 9. Validated SSS: actual remaining requirements

The current approved-artifact gate is deliberately SSS-level for any champion:

- Real source-compatible data, verified cost evidence and recorded point-in-time membership.
- Clean reproducible code commit, current feature/promotion schema and immutable artifact hashes.
- At least 200 selected untouched holdout outcomes and 78 market-week clusters.
- Positive conservative lower bound on net EV and incremental EV over deterministic baseline.
- At least 200 separate calibration observations; Brier better than constant/base-rate classifier.
- Three positive chronological folds; at least two supported regimes with ≥30 observations
  each and no qualifying negative-regime expectancy.
- Additive drawdown ≤20R; worst-five-percent mean loss no worse than −1.5R.
- Manual named approval; a new champion needs a paired improvement interval over the frozen
  old champion on genuinely unseen data. No flag can override these checks.
- Live cohort additionally needs ≥100 outcomes, ≥26 weekly clusters, positive EV lower bound
  and probability interval width ≤0.20, fresh compatible features/inference and all risk gates.

Older protocol discussions mention 26 clusters for baseline probabilities and 52 for SS;
they do not override the current stricter 78-cluster champion gate. No thresholds have
been weakened merely to obtain earlier alerts.

Important blocker: current `prints-v1` labels apply assumed fee/slippage/funding costs and
set `costs_verified=false`. Collecting more such labels alone cannot unlock validated SSS.
A reviewed verified-cost labeling extension, genuine holdout evidence, required ablations
and operational validation remain necessary. Current code has no approved model or claimed edge.

## 10. Historical research without a subscription

```sh
docker compose exec desk bybit-flow download-candles BTCUSDT --exchange binance --interval 60 --days 90
docker compose exec desk bybit-flow download-trades BTCUSDT 2024-01-01 --exchange binance
docker compose exec desk bybit-flow download-trades LINKUSDT 2024-01-01 --exchange bybit
docker compose exec desk bybit-flow aggregate-trades /app/data/archives/<printed-file>.trades.parquet --minutes 60
docker compose exec desk bybit-flow research /app/data/<printed-candle-file>.parquet
```

Binance ZIPs require the published checksum sidecar; Bybit archives retain local hashes.
Sources can remove/correct files; a requested date is not a promise of coverage. Archives
contain no fabricated books/liquidations or historical universe. Aggregate-trade counts
differ from individual fills. OHLCV `research` is an ATR baseline, not a full-flow backtest.
Legacy `replay` is Bybit-only and explicitly refuses Binance/OKX native recordings; use
source-separated frozen-candidate `ml label` for their counterfactual print outcomes.

## 11. Weekly maintenance and safe updates

Weekly: doctor, feed freshness/gaps, Discord connection test, free disk, recorder/scanner
activity, candidate growth, ML status and rejected challengers. Periodically: off-machine
backup, restore test, main update, rebuild, check drift and immutable experiment history.
Never selectively delete losing experiments. Do not clear the outbox to force resends.

```sh
git switch main
git status
git pull --ff-only origin main
docker compose up -d --build
docker compose exec desk bybit-flow doctor
docker compose logs --tail 100
docker compose exec desk bybit-flow test-market
```

Back up first. Stop if Git reports unexpected local changes or diverged history; preserve
them and seek review. No force pull, reset, feature branch or PR is needed for operation.
SQLite migrations run automatically at startup and are additive. An enabled trainer image
also needs `docker compose --profile ml up -d --build` after updates.

## 12. Backup and restore

All application data lives in the `research-data` Compose volume: SQLite (including outbox,
features/labels and registry metadata), raw/normalized segments, Parquet exports, experiments,
models and cards. `.env` is separate: back it up securely, never in a public data bundle.

A consistent database-only snapshot, while running:

```sh
docker compose exec desk bybit-flow backup /app/data/research-backup-YYYYMMDD.sqlite
```

Use a NEW filename. SQLite's backup API is used; this does not include raw files/models.
For a complete portable backup, stop ALL writers first. Existing stopped containers are
kept so Compose can copy their mounted data. Never use `down -v`.

Windows PowerShell:

```powershell
$flowStamp = Get-Date -Format yyyyMMdd-HHmmss
New-Item -ItemType Directory -Force .\backups | Out-Null
docker compose --profile ml stop
docker compose cp desk:/app/data ".\backups\$flowStamp"
docker compose up -d
# Only if you had enabled the worker:
docker compose --profile ml up -d trainer
```

Ubuntu (substitute a new timestamp yourself; prepend sudo if needed):

```sh
mkdir -p backups
docker compose --profile ml stop
docker compose cp desk:/app/data ./backups/20260910-120000
docker compose up -d
```

Copy the backup directory off the host and compare checksums/file counts. Record the code
commit and `.env` separately. Treat market-data redistribution according to source terms.

Restore into a fresh installation/Compose project, not over active or unreviewed data:

```sh
docker compose build
docker compose create desk
# Replace with your actual backed-up data DIRECTORY. It should contain research.sqlite.
docker compose cp ./backups/<timestamp>/. desk:/app/data
docker compose run --rm --no-deps --user 0 --cap-add CHOWN --cap-add DAC_OVERRIDE --entrypoint chown desk -R 10001:10001 /app/data
docker compose up -d
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow ml status
```

Do not proceed if the destination already contains unrelated data. Use a separate checkout
and project name for a restore drill. Retain the source backup; the app does not delete it.
The one-shot ownership repair needs the explicitly listed capabilities because normal
containers drop all capabilities. They are not added to the running application service.
Verify restored snapshots, model IDs, manifests and outbox, not merely a running container.
The restored collector must establish fresh market continuity; old quotes cannot qualify signals.

## 13. Troubleshooting

| Symptom | Probable cause | Diagnose | Safe resolution |
|---|---|---|---|
| Docker commands fail | Desktop/daemon stopped | `docker info` | Start Docker Desktop/service; complete virtualization setup |
| Container fails before starting | Missing admin secret, invalid env or image failure | `docker compose config --quiet`; `docker compose logs --tail 100` | Correct `.env`; set admin secret; rebuild; do not print full expanded config publicly |
| Dashboard unavailable | Container stopped, wrong port, tunnel closed | `docker compose ps` | Use localhost:8000, restore tunnel; do not publish port publicly |
| Login rejected | Wrong admin password | Check local `.env` privately | Recreate container after changing secret |
| Discord absent/wrong channel | Wrong/missing webhook or signal gates | `docker compose exec desk bybit-flow test-discord` | Correct local webhook/channel; check actual test message |
| Discord uncertain/rejected/rate-limited | Network ambiguity, revoked webhook, rate limit | Test output and channel/outbox | Reconcile before another explicit test; do not clear history or blindly retry alerts |
| REST fails | TLS/DNS/time/firewall/regional denial | `test-market --exchange <venue>` | Check system clock/trust/network and exchange availability lawfully; never disable TLS |
| Bybit TLS mismatch | Certificate is not for official hostname | `test-market --exchange bybit` | Correct host/network trust issue with provider; use another lawfully available venue, not a routing bypass |
| Binance fails | Access restriction, TLS or rate limit | `test-market --exchange binance` | Wait for circuit cooldown; check supported region and official endpoint health |
| OKX fails | REST metadata unavailable, WS8443 blocked, sequence reset | `test-market --exchange okx` | Check lawful outbound access; wait for fresh snapshot; no guessed multiplier |
| All exchanges unavailable | Host network/security/access issue | `doctor --json` and individual market tests | NO TRADE; resolve external connectivity before expecting signals |
| WS disconnect/stale book | Feed/network gap or missing snapshot | Data health, logs | Allow reconnect and new full-window warmup; no stale-book override |
| No eligible symbols | Listing/turnover/depth/spread gates | Scanner and normal-spread reasons | Wait for real samples; inspect liquidity thresholds, do not invent history |
| Liquidity warming up | Insufficient five-minute quote buckets | Market scanner sample counts | Wait at least the required observed coverage, restart only if necessary |
| No signals | Correct selectivity, missing facts/flow, unsupported regime | Watchlist, rejection reasons, data health | Accept NO TRADE; test Discord separately from signal quality |
| Recording gaps | Queue, disk or persistence failure | Doctor/Data health/recorder reason | Preserve files; restore disk capacity and restart; broken windows remain incomplete |
| Disk full/quota | Raw data accumulated | Doctor, `retention-plan` | Back up/archive verified old files before scoped manual removal; never delete root/volume indiscriminately |
| ML Uncalibrated/no model | No complete real labels or insufficient partitions | `ml status`; trainer logs | Collect real data; run worker; no synthetic fill injection |
| No challenger | Weekly cadence, one-class labels, insufficient unseen holdout | Trainer logs/ML cycle reason | Wait for new resolved evidence; do not rename old datasets to reuse holdout |
| Challenger rejected | EV/calibration/regime/cost/cluster gate failed | Model card/promotion reasons | Keep simpler incumbent or abstain; rejection is normal research output |
| SSS research blocked | Score <95, missing data/facts or mandatory gate | Signal detail and score contributions | Fix actual data issue; do not inflate score, facts or leverage |
| Validated SSS locked | No qualifying approved artifact/verified costs | `ml status`, model card | Complete real evidence and verified-cost protocol; no config bypass exists |

Commands abbreviated in the table run through `docker compose exec desk bybit-flow ...`.
Keep error reports to status/type/timestamp, never secrets. TLS verification stays enabled.

## 14. Final checklist

- [ ] Git installed
- [ ] Docker installed and engine running
- [ ] Repository cloned
- [ ] main current
- [ ] .env created and private
- [ ] Discord webhook configured
- [ ] Containers healthy
- [ ] doctor checks reviewed and required services pass
- [ ] market test passes on at least one lawful source
- [ ] Discord connection test received
- [ ] Market data recording
- [ ] Scanner active
- [ ] Footprint data generating after complete-window warmup
- [ ] Candidate store growing when genuine setups occur
- [ ] ML feature store growing without invented data
- [ ] Backup and separate restore tested
- [ ] SSS RESEARCH status understood
- [ ] Validated SSS status and remaining evidence requirements understood
