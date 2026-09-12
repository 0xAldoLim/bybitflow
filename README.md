# BybitFlow

A self-hosted research workstation for crypto perpetual markets. BybitFlow collects
public exchange data, evaluates trading setups, and delivers Discord research
alerts with an entry zone, stop-loss, and two targets. It does not execute orders.

## Quick start

Requires Docker Desktop on Windows, or Docker Engine with Compose on Linux.

1. Copy `.env.example` to `.env`.
2. Set `FLOW_ADMIN_TOKEN` and `FLOW_RESEARCH_WEBHOOK`.
3. Set `FLOW_RESEARCH_ALERTS=true` to enable SSS–D research alerts.
4. Start the services:

```sh
docker compose --profile ml up -d --build
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow test-market
docker compose exec desk bybit-flow test-discord
```

Open [the dashboard](http://127.0.0.1:8000). HTTP Basic credentials are username
`research` and the configured admin token. Store credentials only in the private
environment configuration.

[Operator guide](docs/USER_RUNBOOK.md) covers configuration, startup, maintenance,
backups, and connectivity. [Contributor guide](CONTRIBUTING.md) covers development
and verification.

## Processing pipeline

Public REST and WebSocket data → source-specific normalization and recording →
closed 4H/1H/15M features → setup detection → execution confirmation and risk
checks → quality scoring and optional ML ranking → Discord and paper outcomes.

- **Sources:** Binance, Bybit, and OKX; automatic selection, fixed-source, and
  bounded multi-source modes.
- **Strategies:** liquidity sweep, trend pullback, range rejection, breakout retest.
- **Signals:** SSS–D research grades, entry zone, stop, TP1/TP2, evidence, and lifecycle.
- **Storage:** SQLite metadata; immutable compressed JSONL and Parquet recordings.
- **ML:** frozen candidate features, recorded-outcome labels, offline challenger
  training, drift monitoring, and reviewed model promotion.

A quality score describes setup evidence. It is not a win probability. Research
alerts require confirmation, fresh data, and accepted risk checks regardless of
grade. ML ranking remains separate from deterministic scoring. Current print-based
labels use cost assumptions and do not satisfy verified-cost promotion requirements.

## Documentation

| Audience | Reference |
|---|---|
| Operators | [Runbook](docs/USER_RUNBOOK.md), [operations](docs/OPERATIONS.md), [asset facts](docs/ASSET_FACTS.md) |
| Researchers | [Scoring](docs/SCORING.md), [research protocol](docs/RESEARCH.md), [ML](docs/ML_RESEARCH.md) |
| Contributors | [Architecture](docs/SELF_HOSTED_ORDERFLOW.md), [exchange adapters](docs/MULTI_EXCHANGE.md), [source definitions](docs/DATA_SOURCES.md) |
| Reviewers | [Feature status](docs/STATUS.md), [verification](docs/VERIFICATION.md) |

Exchange availability depends on the deployment network. Use the health endpoints
and diagnostic commands to establish current connectivity.

The optional [chart-event compatibility API](docs/TRADINGVIEW_SETUP.md) is disabled
by default. Exchange-native operation requires no chart subscription or public ingress.
