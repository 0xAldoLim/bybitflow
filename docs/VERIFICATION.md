# Verification record

## Windows operator deployment — 2026-09-11 UTC

### Follow-up recovery verification

The isolated Docker suite passed **93 tests**, with two dependency deprecation
warnings; Ruff lint passed. Regression tests reproduced two operational defects
before their fixes: generic recorder-chain gaps did not invalidate Binance/OKX
outcomes, and failed scans waited 900 seconds before retrying. Global gaps now
invalidate every affected venue while venue-specific gaps remain isolated.
Failed scans retry after at most 60 seconds; successful scans retain their normal
configured cadence. Signal and risk gates are unchanged.

Segment `1789091364628-c395e0d8` contained a 59 ms backward receipt-time step.
With writers stopped, its raw, manifest and Parquet files were moved intact into
an audited data-volume quarantine. All three hashes matched; 1,755 remaining
segments replayed with an explicit missing-chain gap. Database provenance and
original timestamps were retained. Real ML monitoring then returned `observed`,
zero complete labels; a bounded cycle abstained because no complete resolved
candidate labels existed. Four genuine snapshots and no models were observed.
This establishes processing, not successful learning or financial validation.

The operator confirmed Windows time synchronization. Subsequent OKX REST and
genuine trade WS probes passed, but later probes failed with ConnectError.
Strict TLS checks continued to show venue-dependent network variability.
Current API health and fresh trade receipts remain essential checks: a running
container alone does not establish healthy collection. Discord receipt is confirmed.

After deploying both final images, authenticated health returned 200 and Docker
marked desk healthy. OKX BTC book and trade events were fresh, 463 ticker symbols
were observed, and 247 envelopes had already been written since startup. Trainer
was running; compilation, runtime/trainer dependency checks, Ruff formatting
(60 files) and lint passed. Continuous availability and future alerts remain
unverified; spread warmup and reviewed facts still gate research signals.

Checked out clean `main` at `80f3c80508cb63cf09c5782baf83d9b7f669d7c8`,
fast-forward pull was already current, and push authentication passed a dry run.
Host: Windows 11 Home Single Language 25H2/build 26200, PowerShell 7.6.6,
Git 2.49.0, Docker Desktop 4.49.0, engine 28.5.1 and Compose 2.40.3.
Approximately 15.4 GiB usable RAM and 196 GiB free host disk at initial inspection.

Docker initially aborted on inaccessible stale runtime sockets. With the failed
Desktop processes stopped, the exact affected runtime directories were renamed
and retained. Fixing both in the same stopped interval allowed the WSL2 engine
to start. No reinstall, reboot, factory reset, volume deletion, WSL reset,
security-setting change or DNS override was used. Initial Docker inventories
after recovery contained no existing containers or named volumes.

The actual Compose desk image built and started with the original non-root user,
read-only root, dropped capabilities, no-new-privileges and localhost-only port.
A generated dashboard password and operator-entered webhook stayed in ignored
`.env`. Unauthenticated health returned 401; authenticated dashboard APIs returned
200. Health initially returned 503 for the real scanner outage, then 200 during
real OKX collection. `pip check` passed. No trading permissions or orders exist.

### Real external and collection evidence

- Discord `test-discord` returned `sent` at epoch ms `1789091170064`; the operator
  confirmed the connection-test message in the channel. Its persisted outbox row
  has a message ID and null signal ID. Candidate/label counts stayed zero at that
  check; the test did not create financial research evidence.
- Binance REST failed with ConnectError and trade WS with certificate/handshake
  errors. Bybit REST intermittently succeeded; its WS failed with TLS errors.
- OKX initially returned actual normalized trades rejected by the unchanged
  freshness gate because the computer was about 2.2 seconds behind. Windows Time
  reported unsynchronized; an attempted resync was denied. A later measurement
  improved to 57 ms clock difference and 509 ms trade age: both REST and genuine
  normalized trade WS passed. No code-based timestamp adjustment was made.
- DNS and strict-SNI probes showed network variability: several official hosts
  resolved to filtering-service addresses and failed hostname verification, while
  OKX WS also resolved to its serving addresses and passed verification. Successful
  application probes do not establish uninterrupted connectivity. TLS stayed on;
  no alternate routing or geographic-access bypass was introduced.
- At 01:54 UTC auto mode selected OKX: 463 discovered instruments, 47 provisional,
  zero fully eligible during spread warmup, eight deep subscriptions and four scan
  errors. Recorder reported 19,333 envelopes, no recorder gap, and a fresh BTC book.
  These envelope counts are not execution/fill counts.
- A complete real BTC 01:53–01:54 UTC one-minute footprint contained 248 trade
  records, delta 2.1183 base, delta 14.0875325%, window CVD 2.1183, PoC 76908.3,
  VAL 76899.9 and VAH 76908.3. This is a feature observation, not a strategy result.
  The full closed 15-minute window and longer profiles remained unavailable then.
- Two genuine ADA candidate generation snapshots were PENDING CONFIRMATION;
  no complete labels, model, champion, calibrated probability or validated SSS.

### Recovery and limits

The running SQLite backup command succeeded. Writers were stopped for a full-volume
copy, then the main service restarted. A separate checkout/project restored to a
different localhost port with scanning/webhooks disabled. Before restored startup,
both SQLite hashes matched the copied files; quick-check was OK, the Discord outbox
receipt and zero snapshot/model counts were preserved, and ML status/doctor ran.
The disposable restore service was stopped and its volume retained. This first drill
used the small pre-collection database; it does not prove large-data/model recovery.

A second stopped-writer backup included real collection: all 2,093 files (27,206,379
bytes) matched SHA256 after copying into another fresh isolated project. Its 697
recording segments, two candidate snapshots, one Discord outbox receipt and SQLite
integrity survived startup. That restore service was also stopped, with its volume
retained. Neither drill establishes recovery of a large trained-model installation.
Main subsequently resumed OKX collection. A read-only Parquet query separately
observed 8,326 normalized OKX trade rows; recorder envelope counts are not used as
a substitute for this executed-trade evidence.

After real OKX and confirmed Discord checks passed, `FLOW_SSS_RESEARCH=true` was
applied by recreating desk. Authenticated settings verified research alerts false,
ML inference false and research ML filtering false. No risk or freshness gate changed.

The in-app browser refused localhost navigation with ERR_BLOCKED_BY_CLIENT. A normal
browser was opened, but native browser automation then stopped because it could not
confidently identify the current URL. Visual dashboard and reviewed-fact-form checks
are not claimed; authenticated HTTP/API checks are established separately.

Missing reviewed asset facts cap native quality at 90. Normal spread requires the
real 12 five-minute buckets and execution requires a complete closed 15-minute tape
window. Current assumed-cost labels still cannot unlock validated SSS; a verified-cost
extension, independent statistical evidence and manual approval remain required.

## Exchange-native milestone — 2026-09-10–11 UTC

This section supersedes the TV-first runtime descriptions below. It reports executed
software checks and real external observations separately; no financial performance is
inferred from either.

### Software verification

- Full host Python 3.13.12 suite: **87 passed**, 18.41 seconds, two existing third-party
  Starlette/AnyIO deprecation warnings. Ruff, format check, compileall, JavaScript syntax,
  `pip check` and `git diff --check` passed. FastAPI tests ran outside the restricted
  sandbox because its in-process transport stalls there.
- New synthetic tests cover Binance/OKX public normalization and base-contract units,
  full REST adapter → liquidity/candles/derivatives → persistent scanner watchlist,
  Binance snapshot bridge/pu gaps, OKX snapshot/sequence/heartbeat, missing profile
  history, source-transition resets, stale/future/old-venue regime exclusion, stale
  cross-venue comparisons, funding freshness distinct from quote freshness, normalized
  schema v2 provenance, optional gateway venue isolation and non-trade Discord diagnostics.
  Existing deterministic strategy, paper fills, risk, ML/logistic/LightGBM, purged folds,
  calibration, holdout, registry/drift, authenticated APIs and mocked Discord tests pass.
  The reviewed-asset-fact form uses the existing protected endpoint; a new synthetic API
  test verifies source/collection-time attribution, future-knowledge refusal and no signal
  creation. Runbook environment coverage was checked against all **52** actual Settings fields.
- Runtime image built: `sha256:e91e89fc5536a24a0f644330226df447b89665dbc11c75f9d23b20db4890519f`.
- Separate trainer built: `sha256:568cbc1be16afc17ac38b613e50dead13cd424528944ee86f82e32c5a88625f7`.
- `examples/ml_docker_smoke.py --image bybit-flow:native-review --trainer
  bybit-flow:native-trainer-review` passed: non-root/read-only runtime, authenticated
  health, unauthorized 401, zero models/no champion, alerts-only true, legacy TV worker
  **off**. Trainer imports sklearn 1.9.0 and LightGBM 4.7.0. Numerical pytest ran on the
  host, not inside the trainer image.
- `examples/native_operations_smoke.py` exercised human/JSON doctor, `test-market`,
  missing-webhook refusal, ML status and SQLite backup/reopen in a disposable container.
  Tiny intentional 128 MB tmpfs caused the documented disk warning. Market-test failure
  exited 1 correctly; it is **not** recorded as a successful exchange connection.
- `examples/compose_smoke.py` built/started the actual base Compose service with scanning
  enabled and no webhook, stopped writers, copied the full volume, restored into a second
  fresh named project, repaired ownership, verified the software-test metadata marker and
  SQLite integrity, and ran the restored ML registry CLI. An initial restore attempt
  found missing CHOWN/DAC_OVERRIDE capabilities; the one-shot command and runbook were
  corrected, then the entire drill passed. Normal service capabilities remain dropped.
  This was a small empty-market installation, not a large-data/model disaster-recovery test.
- Compose `ml` profile configuration passed. Real Chromium rendered desktop 1440/scroll
  1425 and mobile 390/scroll 390, with no reported page errors, including exchanges,
  order-flow/DOM, ML and retained optional legacy routes. Screenshots remain ignored.
- Temporary test containers and loopback server were stopped. Unique Compose test volumes
  were deliberately retained; unrelated running containers/data were not modified.

### Real external connectivity and historical data

- Host and final Docker live Binance/Bybit REST+trade WS probes (including the final
  September 11 UTC Docker rerun) did **not** establish
  connectivity. REST reported certificate/connect errors (Bybit also timed out in Docker);
  Binance/Bybit WS reported certificate-verification errors.
- OKX REST failed; its normalized trade probe could not acquire required contract metadata.
  An independent successful OKX WS handshake/normalized trade is **not established**.
- Host DNS on September 10 returned filtering-service names `internetpositif.id` /
  `internetsehatku.com` and addresses 36.86.63.185 / 195.35.23.222 for official exchange
  hosts. System NTP reported synchronized; proper-SNI hostname verification failed.
  No TLS disablement, DNS override or regional-access bypass was used. These observations
  describe this development host, not universal exchange outages.
- **Real historical archive success:**
  `https://data.binance.vision/data/futures/um/daily/aggTrades/LINKUSDT/LINKUSDT-aggTrades-2024-01-01.zip`.
  The official checksum sidecar matched SHA256
  `93877f83f4a97a5c10f4cf50f6ab54ff4da78e502155b9e29cf8c11abbe0acc7`.
  Download size 1,780,848 bytes; **115,985** actual reported aggregate executions converted.
  Parquet SHA256: `ded63852cc2b11f5b0806a2212afd621e1685a9b0b61210fd8643023bf8c7797`.
  File, normalized trades and manifest remain outside Git under `data/archives`.

Reproduce archive acquisition and feature calculation from a Python checkout:

```sh
bybit-flow download-trades LINKUSDT 2024-01-01 --exchange binance
python examples/archive_footprint.py data/archives/LINKUSDT-aggTrades-2024-01-01.zip.trades.parquet --bucket-increment 0.001
```

The executed historical sample was January 1, 2024, 23:30–23:45 UTC: 1,715 aggregate
execution records, buy 89,991.04 base, sell 85,974.89 base, delta 4,016.15 base, delta
2.2823452244%, PoC 15.576, VAL 15.539, VAH 15.589, VWAP 15.55312944. These are **historical
feature values**, not a current trade or strategy result. The explicitly supplied 0.001
analytical bucket increment is not verified historical instrument metadata. No historical
book/absorption, point-in-time universe, fill, calibration or profitability is established.

### Still unverified or unavailable

No valid local Discord webhook was supplied: actual delivery was not attempted. The CLI
correctly refuses absent configuration; synthetic HTTP mocks are not real delivery.
No live market recording, genuine native candidate-to-Discord alert, resolved real candidate
label, real-data model training/backtest, champion, calibrated probability, profitable edge
or validated SSS exists in this development installation. The archive feature demonstration
is not a family backtest or model dataset. Windows instructions were checked for PowerShell
syntax but were not executed on a Windows machine. Loaded VPS operation, long-running
rotation/failover, large-data restore, and complete native strategy replay remain unverified.
Current assumed-cost labels cannot approve a validated model. See STATUS.md and the
authoritative [USER_RUNBOOK.md](USER_RUNBOOK.md) for precise operator steps and limitations.

## ML research extension — 2026-09-09 UTC

- Final local Python 3.13 suite: **71 passed** in 13.62 seconds; two existing third-party
  Starlette/AnyIO deprecation warnings. Ruff, Python compile, JavaScript syntax, `pip check`
  and `git diff --check` passed. Synthetic numerical tests are not financial evidence.
- Tests cover frozen rejected-candidate snapshots, availability timestamps, signed BOS/CHoCH,
  E/F grades, funding deduplication, MFE/MAE, gap/missing-label exclusion, source-separated chart
  fallback, logistic/LightGBM JSON prediction parity, independent calibration, purged folds,
  next-cycle unseen holdouts, holdout reuse rejection, model hash checks, refusal of synthetic
  promotion, drift degradation, stale approval clearing, dashboard auth and webhook secrecy.
- Complete synthetic path with ML enabled: TradingView setup → first-decision feature snapshot
  → mocked Discord SSS research card → invalidation update → manual paper journal. Manual
  outcomes remain excluded from training. Existing authenticated ingress/durable queue tests
  also pass. **No real webhook was contacted.**
- `bybit-flow ml status` on the actual local data directory: zero snapshots, labels and models;
  no champion. SQLite also contains zero market recording segments. No synthetic financial
  results or fitted test models were inserted into that directory or committed to Git.
- Final lightweight runtime Docker build:
  `sha256:fe973d03ae884ed643fe8f9308cb81b3e042213f84ec90be73d46cd70a187176`.
- Final separate trainer build:
  `sha256:110eb238dd00ef654b28bd7f5f2c0cae4b5f1361ace3edbfcb484e57d1ba7f42`.
- `examples/ml_docker_smoke.py` ran against these final images. Isolated non-root/read-only
  runtime started, returned HTTP 401 without credentials, and authenticated health reported
  `ok`, recorder readiness, `alerts_only` and a live TradingView queue worker. `/api/ml` had
  no models. Trainer Python 3.12 imported sklearn 1.9.0 and LightGBM 4.7.0 successfully.
  The full numerical pytest suite ran on the host, not inside the trainer container.
- Compose `ml` profile configuration validated. Local Chromium rendered overview and every
  page including ML Research with no reported page error. Desktop width 1440 / scroll 1425;
  mobile width 390 / scroll 390. Screenshots remain ignored under `data/ui`.
- Rechecked real Bybit HTTPS on the host and in the final Docker runtime: hostname certificate
  mismatch remains. TLS verification was never disabled; DNS/routing were not altered.
- Temporary smoke containers/tmpfs data and the loopback browser-test server were stopped.
  Unrelated existing containers were not changed. The reusable built images remain local.

Not verified: real market collection, real-data model training/backtests, financial improvement,
validated SSS, true TradingView/Pine compilation and alert delivery, real Discord delivery,
long-running worker/VPS load, or exact funding/mark execution simulation. Exchange and Discord
credentials are not configured; Bybit connectivity is unavailable. Models trained in the Docker
image lack Git checkout metadata and remain research artifacts; deployment-quality provenance
requires training from a clean checkout. See [ML_RESEARCH.md](ML_RESEARCH.md) for remaining
feature and cost-label limitations. GitHub CI configuration was updated; its remote outcome
was not inspected as part of these local checks.

## TradingView feature branch — 2026-09-09 UTC

- Final Python suite: **54 passed**, about 6–7 seconds, with the same two third-party
  Starlette/AnyIO deprecation warnings. Ruff, format check, JS syntax and `pip check` pass.
- Synthetic end-to-end HTTP test: dedicated authentication → durable SQLite inbox → recomputed
  structure/flow/liquidity/risk gates → mock Discord SSS research card → invalidation update →
  paper journal. Duplicate/conflicting IDs, stale queue recovery, missing footprint/depth, proxy
  caps, mirrored shorts, component sensitivity and heartbeat loss are covered.
- Native score test reaches 100 only with all synthetic component observations; probability stays
  null. Removing fundamental/cross-market observations removes the corresponding points.
- Separate connection-test event produces only a **CONNECTION TEST · NOT A TRADE** mock card;
  it cannot create a signal or paper outcome. Nothing was sent to a real Discord webhook.
- Reversal/continuation historical replay tests use explicitly synthetic observations and candles;
  costs reduce results and missing footprint is never reconstructed. These are software tests,
  not historical performance evidence or calibration approval.
- Incremental subscription rotation retains unchanged tape/book objects; closed-candle caching
  refreshes on timeframe boundaries. Live exchange rotation and broad-market load remain untested.
- Chromium rendering passed at desktop 1440 (scroll width 1425) and mobile 390 (scroll width 390),
  including watchlist, signals, **TradingView gateway**, research, journal, health and settings.
  An earlier startup attempt timed out; a longer readiness window and retry passed.
- Updated Docker image built from Python 3.12-slim:
  `sha256:68f987c6565d1a23633a447838f1f65b194d7a1a80b36d879163740dc15575b6`.
  Initial dependency download timed out; verified downloads with longer timeout/build cache
  succeeded. A connection interruption required resuming the cached build.
- Docker smoke: read-only filesystem, disposable tmpfs data, loopback port, UID/GID 10001;
  unauthenticated dashboard 401, authenticated health 200, TV worker alive, independent ingress
  auth 401, authenticated malformed event 422, configuration secrets omitted. Idle memory about
  64 MiB; this is **not** a loaded scanner benchmark.
- Caddy configuration validated; combined Compose configuration validated with synthetic values.
  Caddy image pinned to the tested manifest digest. No real DNS/ACME certificate was provisioned.
- Host and Docker Bybit TLS diagnosis identified a Telkomsel hostname/expired-certificate response;
  verification was not disabled. Details: CONNECTIVITY_AUDIT.md.

Not performed: Pine compilation in TradingView, live TradingView alert receipt, real Discord
delivery, public domain deployment, actual historical family studies, native-flow collection
on this blocked connection or calibrated probability validation. No account credentials/domain/
Discord webhook were supplied. Those are explicitly pending review/deployment checks.

Date: 2026-09-08. This file reports software checks, not strategy performance.

## Follow-up verification

After the initial `cd2f5fe` milestone, the suite expanded to **41 passed** (same two
third-party deprecation warnings). Additional checks cover normal-spread warmup, no backdating,
time-bucket deduplication, stale/missing quote coverage, spread-tail rejection, restored history,
and omitted intermediate segments becoming explicit replay gaps. The scanner integration now
requires a source-timestamped spread history. Ruff and JavaScript syntax checks also pass.
The initial Docker/browser checks below remain the baseline; follow-up source changes are
identified separately rather than presented as a new long-running live-feed validation.
The follow-up source was additionally mounted read-only into the previously built Python 3.12
container: authenticated health returned `ok: true`, and the normal-spread calculation passed
a direct smoke check. This was a source-mounted test, not a rebuilt image or live-market run.

## Executed successfully

- Python 3.13.12 virtual environment; pinned installed dependencies pass `pip check`.
- `pytest -q`: **37 passed**, approximately five seconds. Two third-party TestClient
  deprecation warnings (HTTPX compatibility and AnyIO BlockingPortal alias); no test failures.
- Full synthetic official-schema REST integration: discovery, daily liquidity, depth, candles,
  OI/funding, watchlist persistence, prioritized symbols, raw recording and Parquet.
- Unit/replay tests: delayed pivots, future extension invariance, candle gaps, metadata exclusion,
  pagination, regional 403 no-retry, snapshot/delta/restart semantics, stale/crossed books, trade
  deduplication, tape overflow, actual liquidation-side mapping, executed delta/value area,
  conservative sizing/portfolio/margin gates, partial/missed/gapped fills, funding sign, clustered
  uncertainty, unfinished-label exclusion, normalized recording, replay tamper detection,
  source-order trade aggregation, PIT facts/membership, Discord mocks, secrets/auth/CSRF and backups.
- `ruff check src tests examples`; Python compile; JavaScript `node --check`.
- `examples/offline_research.py`: generated an explicitly **SYNTHETIC SOFTWARE TEST** experiment
  with parameter and code/data provenance. Its numbers are excluded from all financial evidence.
- `bybit-flow sample-alert`: generated the synthetic `examples/sample-alert.json` card. No webhook sent.
- FastAPI server started on loopback. Chromium/CDP rendered overview plus watchlist, signals,
  research, journal, health and settings without reported page errors.
- Exact responsive viewport check: desktop width 1440 / scroll width 1425; mobile width 390 /
  scroll width 390. Local screenshots: `data/ui/desktop.png`, `data/ui/mobile.png` (ignored by Git).
- Docker image built successfully from Python 3.12-slim with pinned runtime dependencies.
  Final image at verification: `sha256:72bf90031e3967e07c1cce1e50b2bc997583c1b28d3eb89aef466d8d49044d3f`.
- Compose configuration validated with a temporary non-secret verification `.env`, then that
  temporary file was removed. User deployment must create its own `.env` from `.env.example`.
- Final container smoke test: HTTP 401 without credentials; authenticated `/healthz` returned
  `ok: true`, `recording: true`, `alerts_only: true`. Container ran as UID/GID 10001 with a
  read-only root filesystem. Idle measured memory was 57.86 MiB; this is not a live-load benchmark.

The in-process Starlette TestClient stalled inside this environment's restricted sandbox.
The same suite passed outside that sandbox; the stalled process was stopped. This is not
counted as a successful sandbox test run. All exchange transports in the suite are mocks.

## Not established

- Direct Bybit REST/archive requests failed TLS hostname verification in this environment.
  Certificate checks remained enabled. No real live scan, WebSocket subscription or archive
  acquisition succeeded here. Official schemas and archive directories were audited via browsing.
- No actual Discord delivery; notifier tests used an HTTP mock.
- No real historical backtest, strategy win rate, expectancy, probability calibration, SS/SSS
  qualification, long-running VPS load test, verified historical book completeness or account
  liquidation replication. Populated live-chart accuracy awaits actual captured data.
- CI workflow is supplied but was not run on GitHub before this local verification record.

Consult STATUS.md for unfinished research features. Passing software tests does not qualify
the strategy to trade, and no claim of institutional-grade predictive performance is made.
