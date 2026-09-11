# ML collection and top-30 coverage — 11 September 2026

The Windows installation enables `FLOW_ML_ENABLED=true` with
`FLOW_ML_FILTER_RESEARCH=false` and `FLOW_RESEARCH_ALERTS=true`. Discord research
cards accept SSS through D after the existing confirmation, freshness and risk
checks. They include the entry zone, stop, TP1 and TP2. ML never places orders.

ML captures decision features and resolves outcomes from real recorded trades.
There is currently no trained model. Training requires at least 200 training,
100 calibration, 100 validation and 100 holdout labels in chronological, purged,
source-separated partitions. The worker monitors outcomes every 15 minutes,
rechecks unsuccessful training daily, and fits successful challenger cycles weekly.
These counts are minimums, not a guarantee of passing validation or profitability.
Promotion remains a separate evidence-based review.

Compatible models add a separately labelled research ranking out of 100 to
Discord cards. This is not an established win probability and does not replace
the deterministic SSS–D score. Missing/degraded models cannot block research
delivery when the research filter is disabled. The ML dashboard shows monitoring,
decision counts and recording exclusions.

## Recording recovery

The unattended ML worker reads only segments published to SQLite after their
files close. It verifies gzip hashes, manifests, bounds and event ordering before
yielding any input to the label writer. Clock-damaged, missing and overlapping
intervals become explicit global gaps; outcomes crossing them are incomplete.
Original bytes are retained, not sorted or retimestamped. JSON audits are stored
under `data/ml/recording-audits`. Integrity mismatches still halt processing.
Strict explicit replay/label commands retain their rejection behavior.

Snapshot reads are bounded and materialized before labeling yields to other work,
so concurrent recorder commits cannot leave the trainer trying to upgrade a stale
SQLite WAL read snapshot into a write transaction.

## Reviewed assets

`examples/reviewed-assets-20260911.json` contains 94 source-attributed baseline
entries covering the 30 numbered assets in the [CoinGecko market-cap listing](https://www.coingecko.com/en/all-cryptocurrencies)
reviewed on 11 September 2026; unnumbered wrapped/staked duplicates are excluded.
Every entry links its underlying source. Sources were reviewed by Codex at the
user's request, not personally reviewed by the user. Live records carry current
collection/knowledge timestamps and seven-day review expiry; never backdate an
import or automatically renew it without source review.

BTC, ETH, USDT, BNB, XRP, USDC, SOL, TRX, FIGR_HELOC, ZEC, HYPE, DOGE, RAIN,
USDS, XMR, WBT, LINK, LEO, ADA, XLM, DAI, BCH, USDE, USD1, LTC, CC, UNI,
GRAM, USDG, HBAR.

Coverage is partial per asset: a baseline is not a live reserve audit, incident
clearance, complete unlock calendar or full five-category review. Missing
categories remain missing and do not receive invented evidence points.
The [official TON media page](https://ton.org/media/) confirms the Toncoin/TON
token rename to Gram/GRAM. Venue tickers are checked independently; neither
unsupported assets nor stablecoins are forced into directional perpetual setups.

The local installation raises `FLOW_DEEP_SYMBOLS` to 30 and prioritizes supported
non-stablecoin top-30 symbols (including the venue's legacy TON ticker). The wider
scanner remains enabled. Actual deep subscriptions can be fewer than 30 because
metadata, liquidity and data-coverage checks still apply. Warmup and outages can
delay signals; the application does not manufacture trades to fill a schedule.

## Start and stop (PowerShell in the repository)

Start: `docker compose --profile ml up -d`

Stop: `docker compose --profile ml stop`

Status: `docker compose --profile ml ps`

Keep Docker Desktop running, Windows awake and the internet connected. Closing
the dashboard or terminal does not stop the containers. Facts need review after
expiry. Local secrets remain in the ignored `.env`; they are never committed.

## Verification on 11 September 2026

115 tests passed, including clock jumps, overlapping/missing segments, corrupted
recording hashes, concurrent SQLite writers, all research grades, and entry/stop/
target Discord fields. Ruff lint/format, JavaScript syntax, dependency checks and
SQLite integrity passed. Desk and trainer images were rebuilt and deployed.

The live ML pass audited 14,570 committed segments and excluded two intervals;
monitoring now reports observed with zero complete outcomes. Two incomplete
labels remain excluded from training; no model or prediction was invented.
A clearly labelled Discord setup test was accepted with a stored message ID.
This establishes webhook connectivity, not delivery of a genuine trade setup.

At final verification, all configured exchange hostnames resolved to the same
address and failed normal TLS checks. Scanner health therefore remained degraded.
The desk retries automatically, but real signals and further ML data collection
require a network connection that correctly resolves and permits the exchange
REST and WebSocket endpoints. Do not disable certificate verification. Restore
access through the network administrator/provider or a permitted working network.
