# Operator guide

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
| `FLOW_RESEARCH_ALERTS=true` | Enable confirmed SSS–D research cards |
| `FLOW_SSS_RESEARCH=true` | Enable SSS-only cards when broader research alerts are disabled |
| `FLOW_SCAN_ENABLED=true` | Enable public-data collection and scanning |
| `FLOW_MARKET_SOURCE=auto` | Probe Binance, Bybit, then OKX and retain a working primary |
| `FLOW_ML_ENABLED=true` | Enable decision-time ML inference when a compatible model exists |
| `FLOW_ML_FILTER_RESEARCH=false` | Keep ML acceptance from filtering research alerts |
| `FLOW_DEEP_SYMBOLS` | Concurrent deep subscription capacity; default 8, maximum 30 |

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
```

Open [the dashboard](http://127.0.0.1:8000). The Discord test is labelled as a
connection test and does not represent a trade setup.

`doctor` reports configuration, storage, database, source probes, and recorded
runtime health. `test-market` verifies REST and actual trade WebSocket reception.
A configured webhook or passing offline test does not establish external delivery.

## Collection and alerts

Eligible contracts require sufficient history and turnover. Spread qualification
requires at least 12 observed five-minute buckets and coverage checks. Execution
confirmation requires a complete closed 15-minute trade window and a fresh book.
The spread baseline additionally requires at least 12 valid five-minute samples
and 80% observation coverage. Initial liquidity warm-up therefore takes about an
hour; recent collection gaps can extend it. A healthy process alone does not
prove that a signal has enough evidence to confirm.

The Compose setup uses a five-minute delay between broad scans. Each closed
15-minute execution window receives a distinct candidate ID under rules-0.2.0,
while repeated scans of the same window remain deduplicated. Native feeds recover
per symbol; one stale coin does not reset the other coins' retained history.
Source changes and interruptions reset affected continuity.

Cards include direction, setup, quality, entry zone, stop, TP1, TP2, and risk
information. Grades do not override confirmation or freshness checks. A quiet
channel can indicate warmup, unavailable data, or no qualifying setup.

The model worker monitors labels every 15 minutes, retries unsuccessful training
daily, and runs successful challenger cycles weekly. A new installation can collect
data without a model. See [ML research](ML_RESEARCH.md) for readiness and approval.

## Stop, restart, and update

Run commands from the repository directory.

```sh
# Stop both services; preserve data.
docker compose --profile ml stop

# Resume.
docker compose --profile ml up -d

# Inspect recent logs.
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
