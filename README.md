# BybitFlow

Self-hosted, public-data crypto perpetual research and Discord alerts. No TradingView,
Pine Script, GoCharting, domain, exchange keys or live order execution is required.

**Start here: [USER_RUNBOOK.md](docs/USER_RUNBOOK.md)** — exact Windows 11 and Ubuntu/VPS
setup, Discord, environment settings, health checks, warmup, ML, maintenance and recovery.

```sh
git clone https://github.com/0xAldoLim/bybitflow.git
cd bybitflow
git switch main
git pull --ff-only origin main
# Windows PowerShell: Copy-Item .env.example .env
cp .env.example .env
# Edit .env: set a random FLOW_ADMIN_TOKEN and your local FLOW_RESEARCH_WEBHOOK.
# Opt-in SSS research: FLOW_SSS_RESEARCH=true; no probability or proven edge.
docker compose up -d --build
docker compose exec desk bybit-flow doctor
docker compose exec desk bybit-flow test-market
docker compose exec desk bybit-flow test-discord
```

Open http://127.0.0.1:8000; username `research`, password `FLOW_ADMIN_TOKEN`.
Keep webhook/password values out of chat and Git. No real webhook is bundled.

## What runs

Public exchange data → venue normalization/recording → closed 4H/1H/15M features →
four deterministic setup families → native footprint confirmation → risk gates →
separate quality/ML qualification → Discord → lifecycle and counterfactual paper labels.

`FLOW_MARKET_SOURCE=auto` tests Binance, Bybit and OKX in that order and keeps a
healthy primary. Fixed `binance`, `bybit` and `okx` modes are available. `multi`
adds independent core-symbol collectors; price/spread dispersion and aligned delta
comparison are informational, with zero unvalidated score credit. Source changes
reset continuity. TLS verification is always enabled.

Existing SMC, risk, replay, recorder and ML modules are preserved. Missing history or
stale books block dependent signals. An empty dashboard or hours/days without signals
can be correct. Trade-side footprint is never fabricated from candle direction.

## Research, not proven performance

`SSS RESEARCH · UNCALIBRATED` requires raw quality ≥95 plus every mandatory gate.
It is explicitly opt-in. Quality is not probability. A raw 98 is not a 98% win rate.
Validated grades require a qualifying approved artifact; none has been established.
Current paper labels use assumed costs and cannot alone satisfy the verified-cost
promotion requirement. No financial results, fills or probabilities are fabricated.

Optional training runs separately:

```sh
docker compose --profile ml up -d --build
docker compose exec desk bybit-flow ml status
```

The worker resolves recorded candidates every 15 minutes and attempts a challenger
weekly when data permits. Training uses logistic/LightGBM, independent calibration,
purged chronological folds, bounded threshold trials and an untouched holdout.
See the runbook before enabling inference or considering manual model approval.

## Documentation and verification

- [Operator runbook](docs/USER_RUNBOOK.md)
- [Architecture, algorithms and actual limitations](docs/SELF_HOSTED_ORDERFLOW.md)
- [Venue schemas, official references and source modes](docs/MULTI_EXCHANGE.md)
- [Current implementation status](docs/STATUS.md)
- [Executed verification, separate from live compatibility](docs/VERIFICATION.md)
- [ML research and promotion protocol](docs/ML_RESEARCH.md)
- [Research definitions](docs/RESEARCH.md) and [source audit](docs/DATA_SOURCES.md)

This development network has failed verified TLS connections to exchange endpoints.
Synthetic payload tests do not prove live compatibility. Real Discord delivery
requires your locally configured webhook and confirmation in your channel.

## Optional legacy chart integration

Preserved TV gateway, chart-research commands and Pine files are **not on the required
runtime path**. Do not buy a subscription or domain to run this application.
[Legacy integration documentation](docs/TRADINGVIEW_SETUP.md) applies only if you
deliberately enable that separate integration. Its classified footprint is not native tape.

## Local Python development

Python 3.12+; Docker is the recommended cross-platform operator path.

```sh
python -m venv .venv
.venv/bin/pip install -r requirements-dev.lock
.venv/bin/pip install --no-deps -e .
.venv/bin/pip install -r requirements-ml.lock
.venv/bin/pytest -q
.venv/bin/ruff check src tests examples
.venv/bin/bybit-flow serve
```

On native Windows use the executables under `.venv/Scripts/`.
Production updates stay on `main`, using fast-forward pulls only. Datasets, recordings,
model artifacts and failed experiments stay in the persistent data volume, outside Git.
