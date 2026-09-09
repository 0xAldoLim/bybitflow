# Verification record

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
