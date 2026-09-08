# Verification record

Date: 2026-09-08. This file reports software checks, not strategy performance.

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
