# Bybit Flow

A self-hosted, alerts-only crypto perpetual research desk. Public Bybit V5 data feeds a broad
liquidity scanner and a small, prioritized trade/book collector. FastAPI serves a restrained,
dark dashboard; SQLite, compressed raw JSONL and normalized Parquet preserve observations.

**This is a functional research milestone, not a validated trading system.** No strategy has
demonstrated an edge. Every candidate is unvalidated; probabilities display **Uncalibrated**.
Public S/SS/SSS alerts are deliberately locked in code. There is no exchange authentication,
order endpoint, execution module, or order button.

## Run locally

Python 3.12+:

```sh
git clone git@github.com:0xAldoLim/bybitflow.git
cd bybitflow
python -m venv .venv
.venv/bin/pip install -r requirements-dev.lock
.venv/bin/pip install --no-deps -e .
cp .env.example .env
.venv/bin/bybit-flow serve
```

Open http://127.0.0.1:8000. The initial empty dashboard is intentional: no prices or research
results are fabricated. Set `FLOW_SCAN_ENABLED=true` in `.env` and restart to collect public
market data. A full-universe scan can take minutes at the conservative request budget. It
discovers every page, checks 30 completed daily bars and seven-day median turnover, then
downloads 4H/1H/15M candles and derivatives context for liquid instruments. Stage B subscribes
to at most eight eligible symbols, including eligible BTC/ETH core symbols.

Normal spread requires at least 12 observed five-minute buckets (about an hour of warmup),
with at least 80% time-bucket coverage over a rolling six-hour window. The median must be
within five bps and p90 within ten bps. Quotes are polled once per minute; repeated messages
cannot multiply the evidence. Provisional markets can be recorded while warming up but cannot
qualify research alerts. The dashboard distinguishes provisional from eligible rows.

At least one complete 15-minute execution window after subscription is also needed. Reconnects,
rotation, stale books, queue overflow, or unavailable inputs reset or reject confirmation.
`scan-once` performs discovery and ranking then exits; use `serve` with scanning enabled for
continuous recording and signal lifecycle monitoring. Run only one writer process per data directory.

## Notifications and risk

Set a **separate research-channel** `FLOW_RESEARCH_WEBHOOK` and `FLOW_RESEARCH_ALERTS=true`
to enable explicitly labeled unvalidated research cards. Default operation sends nothing.
`FLOW_DISCORD_WEBHOOK` is reserved for a future validated release; configuring it does not
unlock public alerts. No webhook was contacted during development.

```sh
.venv/bin/bybit-flow sample-alert
```

This prints a synthetic formatting example using `EXAMPLEUSDT`, never a market recommendation.
Discord delivery uses durable deduplication and at-most-once attempts. Ambiguous timeouts and
rate-limited attempts need manual reconciliation; exactly-once delivery cannot be guaranteed
by a webhook without an atomic idempotency facility. See [operations](docs/OPERATIONS.md).

Sizing uses stop distance plus estimated costs. Defaults: 0.25% account risk, 1.5% daily loss
limit, 4% weekly loss limit, 0.75% combined correlated risk, 3× illustrative leverage ceiling,
2R minimum net planned reward:risk. Fees are **assumptions**, initially 5.5 bps per side,
2 bps slippage per side, and a conservative funding reserve. Set actual fee assumptions in
configuration. An optional `FLOW_EQUITY` enables rounded quantity and margin calculations;
then a fresh manual portfolio snapshot is required in Settings. Stops and estimated losses
are not guaranteed; the margin stress check is not an exact Bybit liquidation calculation.

## Reproducible research

Commands use actual public data when reachable. Current metadata never becomes historical
metadata. Archive-only studies cannot qualify an order-flow strategy or a historical universe.

```sh
# Historical candles, paginated backwards and excluding open candles.
.venv/bin/bybit-flow download-candles BTCUSDT --interval 60 --days 180
# Use the exact file printed by the downloader.
.venv/bin/bybit-flow research data/BTCUSDT-60-<timestamp>.parquet

# Actual official daily trades; no order book is manufactured from these.
.venv/bin/bybit-flow download-trades LINKUSDT 2024-01-01
.venv/bin/bybit-flow aggregate-trades data/archives/LINKUSDT2024-01-01.trades.parquet --minutes 60

# Replay recorded raw envelopes in receipt order, verifying hashes and segment continuity.
.venv/bin/bybit-flow replay data/segments/<segment>.jsonl.gz

.venv/bin/pytest -q
.venv/bin/ruff check src tests
```

`research` runs a separate OHLCV-only ATR baseline with fixed 60/20/20 chronological partitions,
development-only ATR sensitivity, next-bar entry, adverse fees/slippage, funding reserve, and
pessimistic ambiguous-bar handling. It includes a labeled gross price-return benchmark.
Recording manifests link to their predecessor. Omitted or legacy unchained intermediate
segments become explicit replay gaps. Files are streamed one at a time to bound open handles.
`replay` feeds real recorded observations through the same `candidates`, `confirm`, candle
features, footprint and book algorithms used live. Replay coverage and cost limitations are
explicit; it is not yet a full-fidelity account simulator. Experiments retain parameters, input
SHA-256 hashes, code hash, commit, outcome definitions, and results in immutable JSON files.

The test fixtures are synthetic software-verification data, not historical trading evidence.
See [verification](docs/VERIFICATION.md) for what was actually run and
[research protocol](docs/RESEARCH.md) for qualification requirements and limitations.

## Docker and VPS

```sh
# First set FLOW_ADMIN_TOKEN to a long random password in .env.
docker compose up --build -d
docker compose logs --tail 100
```

Compose binds only `127.0.0.1:8000`, uses a non-root container, a persistent named volume,
read-only root filesystem, bounded memory, and a health check. Log in as `research` with
`FLOW_ADMIN_TOKEN`. Use an SSH tunnel from a VPS (`ssh -L 8000:127.0.0.1:8000 your-vps`),
or deploy a TLS reverse proxy with authentication. Do not publish the port unauthenticated.
No Redis, Kafka, Kubernetes, paid services, or private exchange keys are required.

## Project map

| Module | Responsibility |
|---|---|
| `ingestion`, `history`, `normalization` | Allowlisted public REST, historical downloads, precision-preserving rows |
| `streams`, `orderflow`, `storage` | Prioritized WebSockets, reconstructed books, executed tape, durable segments |
| `features`, `strategy`, `scanner` | Causal structure, four distinct experimental families, two-stage scanning |
| `risk`, `scoring`, `calibration` | Mandatory gates, separate quality score, research uncertainty and abstention |
| `backtest`, `replay`, `research` | Paper fills, recorded-event replay, baselines and experiment provenance |
| `fundamentals` | Source-attributed, availability-dated manual asset facts |
| `notifications`, `app`, `static` | Research Discord cards, protected local API, browser dashboard |

Read [data-source audit](docs/DATA_SOURCES.md), [feature status](docs/STATUS.md), and
[operations/deployment](docs/OPERATIONS.md) before interpreting results.
