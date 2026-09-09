# TradingView → BybitFlow → Discord setup

This is an alerts-only workflow. You do not need an exchange API key or Discord bot token.
It does not connect TradingView's broker panel to an exchange. Complete the software smoke
tests before paying for hosting or creating production alerts. Pine scripts have been source
reviewed, **not compiled in a TradingView account here**. Account compilation is a release check.

## 1. Choose the operating mode

| Mode | Required data | Alert behavior |
|---|---|---|
| Default | None | No outbound research alerts |
| TradingView proxy research | Premium/Ultimate footprint, confirmed structure, turnover proxy, cost/risk geometry | Unsized, max 84; explicitly missing spread/depth; no equity sizing |
| Strict SSS research | Above plus fresh observed instrument specifications, normal spread and executable depth | Raw ≥95 and all gates; **SSS RESEARCH · UNCALIBRATED** |
| Validated public SSS | Not available | Locked, regardless of configuration |

Start with proxy research if Bybit is inaccessible. This gets the signal workflow operating
without pretending chart turnover proves an order can be filled. It does not need a probability
model or proof of profitability. Do not mislabel proxy cards as strict SSS. Optional Bybit
recording can later supply real tape/book/derivatives evidence where lawful access works.

## 2. TradingView account and Premium

1. Create/sign into your account at [TradingView](https://www.tradingview.com/). Verify your email.
2. Open [Subscriptions](https://www.tradingview.com/pricing/), compare monthly versus annual billing,
   then choose **Premium**, or Ultimate if your account classification requires it. Check your
   local currency, taxes, renewal price, trial conditions and cancellation date at checkout.
   This guide intentionally does not promise a fixed price or free trial.
3. Enable two-factor authentication in account security settings and save recovery codes offline.
4. Confirm your plan runs `request.footprint()`. Premium/Ultimate are required; an input toggle
   cannot make a footprint script run on a lower plan. See the
   [official footprint API documentation](https://www.tradingview.com/pine-script-docs/concepts/other-timeframes-and-data/#requestfootprint).

You are buying a chart/data feature, not an investment return. Buying Ultimate does not improve
the strategy rules. Keep billing and login credentials out of all Pine scripts and payloads.

## 3. Create your Discord server and channel

1. In Discord, use **Add a Server → Create My Own**, or use a server you administer.
2. Create a text channel named `bybitflow-research`. Initially keep it private to you and reviewers.
3. Open **Server Settings → Integrations → Webhooks → New/Create Webhook**.
4. Name it `BybitFlow Research`, select that channel, then copy the webhook URL.
5. Put that URL only in local `.env` as `FLOW_RESEARCH_WEBHOOK`. Never put it in Git, TradingView,
   screenshots, chat messages or the public domain URL. Delete/recreate the webhook if leaked.

You need permission to manage webhooks. No Discord developer application, privileged intents,
or OAuth bot invitation is required. The app suppresses mentions, including `@everyone`.
See [Discord's webhook instructions](https://support.discord.com/hc/en-us/articles/228383668-Intro-to-Webhooks).

## 4. Buy a domain and prepare a lawful hosting location

A domain is a name; it does **not** run Python or Docker. You also need an always-on computer
with public inbound HTTPS, usually a modest Linux VPS. A local PC behind carrier-grade NAT
cannot receive public alerts merely by buying a domain. Start with approximately 2 vCPU / 4 GB
RAM / 30 GB disk; this is an engineering starting budget, not a measured full-universe capacity.
Do not choose a location to circumvent exchange or residency restrictions. TV-only mode needs
no Bybit connection.

One option is [Cloudflare Registrar](https://domains.cloudflare.com/):

1. Create an account, enable 2FA, search for an available domain you own the right to use.
2. Compare **registration and renewal** prices. Prefer an ordinary, non-premium name; check
   recurring charges, contact requirements and refund rules before buying.
3. Purchase the domain yourself. Verify registrant contact details and review auto-renew.
4. In DNS, add an **A** record: name `signals`, value your server's public IPv4 address.
   Use **DNS only** for this initial setup. Do not add an AAAA record unless IPv6 is deliberately
   configured; TradingView webhooks do not currently support IPv6.
5. Wait for the record to resolve to your server. Buying elsewhere is fine: create the equivalent
   record at that domain's DNS provider.

See [domain registration](https://developers.cloudflare.com/registrar/get-started/register-domain/)
and [renewals](https://developers.cloudflare.com/registrar/account-options/renew-domains/).
No domain or subscription purchase has been made by this agent.

Install Docker Engine and its Compose plugin following [Docker's official Linux instructions](https://docs.docker.com/engine/install/).
Allow inbound TCP 80 and 443 in both provider and OS firewalls. Keep SSH restricted to your
administrative IP where practical. Do **not** open port 8000 publicly.

## 5. Configure the existing repository

Use the reviewed/merged release. During PR review, explicitly check out its feature branch.

```sh
git clone git@github.com:0xAldoLim/bybitflow.git
cd bybitflow
git switch feature/tradingview-research-gateway
cp .env.example .env
chmod 600 .env
python3 -c 'import secrets; print(secrets.token_urlsafe(48))'
```

Generate **two different** random values: one dashboard password, one integration-only key.
Edit `.env` locally; do not use a weak example value:

```dotenv
FLOW_ADMIN_TOKEN=<random-dashboard-password>
FLOW_TV_TOKEN=<different-random-integration-key-at-least-32-characters>
FLOW_GATEWAY_DOMAIN=signals.your-actual-domain.com
FLOW_TV_ENABLED=true
FLOW_TV_SYMBOLS=["BTCUSDT","ETHUSDT"]
FLOW_SCAN_ENABLED=false
FLOW_RESEARCH_ALERTS=true
FLOW_RESEARCH_WEBHOOK=https://discord.com/api/webhooks/<your-id>/<your-secret>
FLOW_TV_PROXY_RESEARCH=true
FLOW_SSS_RESEARCH=false
FLOW_TV_REQUIRE_NATIVE_CONFIRMATION=false
FLOW_DASHBOARD_URL=http://127.0.0.1:8000
```

Leave `FLOW_EQUITY` empty in proxy mode. It cannot safely size actual contracts without observed
specifications/depth. The 0.25% figure is illustrative risk, not a instruction to place an order.
To enable strict SSS research later, set `FLOW_SSS_RESEARCH=true`; use observed liquidity rather
than proxy mode. Current Bybit data is used automatically when the optional collector is healthy.
An authenticated operator can alternatively submit **actual observed** data to
`POST /api/tradingview/liquidity`; inspect `LiquidityObservation` in `tradingview.py` for the schema.
These observations expire after 60 seconds. Never enter made-up quotes/depth to unlock a tier.

```sh
docker compose -f compose.yaml -f compose.gateway.yaml up -d --build
docker compose -f compose.yaml -f compose.gateway.yaml ps
```

Caddy obtains and renews certificates when DNS and ports are correct. Its certificate state is
stored in named volumes. See [automatic HTTPS](https://caddyserver.com/docs/automatic-https).
The gateway publishes only the integration route; all other public paths return 404. Access
the dashboard through an SSH tunnel:

```sh
ssh -L 8000:127.0.0.1:8000 <your-ssh-user>@<your-server-ip>
```

Open `http://127.0.0.1:8000`; Basic username is `research`, password is `FLOW_ADMIN_TOKEN`.
Use the TradingView gateway page to inspect event states/results. Do not publish an unprotected
dashboard. Never run `docker compose config` in shared logs: its expanded output contains secrets.

## 6. Install the indicator and create alerts

1. Open TradingView Supercharts, select **BYBIT:BTCUSDT.P**, standard Candles, **15 minutes**.
2. Open Pine Editor. Paste `pine/bybitflow_indicator.pine`, save and **Add to chart**.
3. Confirm it compiles; report the exact line/error if it does not. Grey shading means footprint
   unavailable. Do not bypass it by setting fake volume values.
4. Select footprint ticks per row appropriate to the symbol. It is a fixed input, not a dynamic
   series. Too coarse gives too few adjacent levels; too fine may produce sparse imbalances.
   Inspect the Data Window for delta, stacks, PoC and value-area levels.
5. Create an alert. Condition: the indicator, **Any alert() function call**. Enable Webhook URL.
6. Set the URL to `https://signals.your-actual-domain.com/ingress/<FLOW_TV_TOKEN>`.
   This is a revocable integration capability, **not** your TradingView login or Discord secret.
   Do not add it to Pine inputs or message templates. The script generates JSON automatically.
7. Create the alert once for each approved symbol; add the same symbols to `FLOW_TV_SYMBOLS` and
   restart the app. Do not assume one chart scans every Bybit pair or that watchlist alert behavior
   has been tested. Start with BTC/ETH.
8. Recreate server-side alerts after changing script code or inputs; an existing alert retains its
   saved script configuration. Do not run strategy and indicator webhooks simultaneously.

TradingView needs 2FA, accepts webhook ports 80/443, and cancels responses taking over three
seconds. This app commits the small inbox record before acknowledging; Discord happens later.
See [webhook requirements](https://www.tradingview.com/support/solutions/43000529348-how-to-configure-webhook-alerts/).
The gateway capability authenticates possession of an integration key, not TradingView's identity.
Keep the URL private, rotate on exposure, and do not add access logging that records it.
Caddy overwrites the internal authentication header; the public FastAPI port stays closed.

## 7. Verify synthetic, then real delivery

First run the deterministic software test (no Discord credentials used, no market claim):

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.lock
.venv/bin/pip install --no-deps -e .
.venv/bin/pytest -q tests/test_tradingview.py
```

That covers HTTP auth, durable inbox, mock Discord card, invalidation and journal. It is **not**
proof that your domain, TradingView or Discord is connected. For a real test:

Start with `pine/connection_test.pine`: add it to an approved 15M Bybit chart and create an
**Any alert() function call** webhook alert using the same gateway URL. With research alerts
enabled, the next closed realtime bar sends exactly one **CONNECTION TEST · NOT A TRADE**
Discord card. Remove that alert after verification. This checks real transport without inventing
a trade or waiting for a rare setup; it does not establish that a qualified trade card was delivered.
Then verify the actual signal indicator:

1. Wait for a qualifying **closed-bar** script event; low frequency means this can take time.
2. In TradingView's alert log, inspect Webhook Status. At the gateway, confirm its event ID is
   queued/done. A 202 only proves ingestion, not Discord delivery.
3. Inspect the inbox result and signal detail. `rejected` explains a gate; `disabled`/`dry-run`
   means notifications or credentials are not configured; `cooldown` means a recent card.
4. Confirm the exact symbol/signal ID is visibly posted in `bybitflow-research`. Record the UTC
   time and Discord message ID. Only now call it real delivery.
5. Follow its invalidation/expiration update and later paper outcome. No card claims you traded.

No qualifying setup? Keep waiting or inspect lower-tier proxy mode. Do not lower live gates or
invent a signal merely to manufacture an SSS screenshot. DNS/TLS errors must be diagnosed with
verification enabled. Ingress 401 means a key/config mismatch; 422 means stale/malformed/unapproved
payload. HTTP 503 means queue capacity. Discord errors remain in the delivery ledger; ambiguous
timeouts need reconciliation rather than blind retries and duplicate cards.

## 8. Strategy Tester and reproducible research

Use `pine/bybitflow_strategy.pine` on a **separate** chart with `Send research/lifecycle alert()
events` off. `strategy.entry/exit` affect only TradingView's simulator. No broker/exchange keys
are used. Test the Reversal and Continuation family inputs separately, then long/short results.
Default tester commission is 0.055% per side; its slippage is **two ticks**, not two bps. Set the
tester properties for the instrument. Tester does not apply actual perpetual funding schedules;
the indicator's paper net-R uses a labeled funding reserve. Their fill assumptions differ.

The shared Python evaluator can replay actual saved TradingView events against an actual
15-minute chart CSV (headers `time,open,high,low,close`; UTC epoch seconds/ms or timezone-aware
ISO time). Export chart data from TradingView for the same symbol and dates:

```sh
.venv/bin/bybit-flow tv-export > data/tv-events.jsonl
.venv/bin/bybit-flow tv-research data/tv-events.jsonl data/BTCUSDT-15m.csv --symbol BTCUSDT
```

For Docker data, use `docker compose exec -T desk bybit-flow tv-export` to export the actual
container database. Put both input files in the mounted data location before running the replay
inside that container. Results save code/data hashes, assumptions, per-family/side 60/20/20
chronological partitions, a four-hour embargo, and sparse-stratum calibration abstention.
Manually reported and Pine paper outcomes do not automatically enter calibration. The inputs
are source-attested chart observations, not independently audited trade fills. No real historical
study is included without actual supplied records; Strategy Tester is one research input.

## 9. Interpretation and operations

- Potential trapped buyers: a prior 15M excursion above a structure level with at least +20%
  classified delta, followed by a close below that level. Sellers use the symmetric rule.
  This is a failed-auction heuristic, not observable inventory, identified traders or manipulation.
- PoC and 70% value area in these scripts are **per 15M bar**, not full-session/prior-week profiles.
  CVD resets at a daily boundary and after missing data; it is not a perpetual exchange CVD series.
- The execution trigger lasts 15 minutes, setup validity one hour, and paper holding horizon up
  to four hours. Setup expiration does not claim a paper position closed. Missing heartbeat
  invalidates support; outcomes can still be journaled without resurrecting the alert.
- TradingView footprint history may change if underlying intrabar data is revised. Confirmed
  HTF offsets avoid future-bar leakage but cannot eliminate provider revisions. Compare recorded
  live observations with later chart exports; do not claim perfect historical/live equivalence.
- Back up SQLite with the existing `bybit-flow backup` command and retain raw segments. Schema v2
  adds the TV inbox without deleting v1 tables. Roll back using an explicit database backup, never
  `docker compose down -v`. Review disk usage and unresolved outbox entries regularly.
  The TV inbox stops accepting new IDs at 100,000 retained events or its database storage budget;
  export and back up before operator-managed archiving. It never silently deletes evidence.
