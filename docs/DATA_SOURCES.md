# Source and schema audit

## Current native sources — 2026-09-10

Start with [USER_RUNBOOK.md](USER_RUNBOOK.md); venue schemas and endpoints are audited in
[MULTI_EXCHANGE.md](MULTI_EXCHANGE.md). TradingView sections below are optional legacy only.
Binance aggregate archives use
`https://data.binance.vision/data/futures/um/daily/aggTrades/{symbol}/{symbol}-aggTrades-{date}.zip`
plus `.CHECKSUM`. SHA-256, bounded ZIP contents, CSV schema, UTC millisecond dates and
monotonic execution IDs/times are verified before base-unit Parquet conversion. Provider
corrections are possible; retain hashes and collection dates. See the
[official archive definitions](https://github.com/binance/binance-public-data).
Archives supply no historical DOM, liquidations or point-in-time instrument membership.

Audit date: 2026-09-08. Official pages inspected via web browsing. Direct CLI attempts to
`api.bybit.com` and `public.bybit.com` encountered TLS certificate hostname mismatches in
the development environment; verification was NOT disabled. No successful live acquisition
or historical coverage measurement is asserted. Contract-specific availability must be
measured by the downloader and retained manifest, not inferred from the existence of a portal.

## Public REST

TradingView sources and host/Docker diagnosis were additionally audited on 2026-09-09 UTC.
See [connectivity evidence](CONNECTIVITY_AUDIT.md) and [account setup](TRADINGVIEW_SETUP.md).
The [official Pine footprint API](https://www.tradingview.com/pine-script-docs/concepts/other-timeframes-and-data/#requestfootprint)
uses intrabar price-based buy/sell classification, returns `na` when unavailable, permits one
footprint request per script and requires Premium/Ultimate. It is not exchange taker-side tape.
`ticks_per_row` is a positive simple integer; VA defaults to 70 and imbalance to 300 percent.
[Footprint/volume-row methods](https://www.tradingview.com/pine-script-docs/language/type-system/#footprints)
provide per-bar volumes, rows, delta, PoC, value-area bounds and diagonal imbalance flags.
Our scripts neither backfill actual liquidation/depth records from these values nor claim
full-session/prior-week profile coverage. Provider history revisions remain an explicit limit.

## Bybit REST details

Fixed HTTPS origin `https://api.bybit.com`. No authenticated methods. All calls use GET.

| Endpoint | Use and audited limitations |
|---|---|
| `/v5/market/time` | Compare exchange time with local UTC; fail on >2-second skew. |
| `/v5/market/instruments-info?category=linear&limit=1000&cursor=…` | Follow `nextPageCursor` until empty; default 500 is incomplete. Explicitly filter `LinearPerpetual`, `Trading`, settlement, prelisting, and precision. Trading/PendingOpen defaults are not eligibility. See [instruments](https://bybit-exchange.github.io/docs/v5/market/instrument). |
| `/v5/market/kline?category=linear&symbol=…&interval=…&end=…&limit=1000` | Reverse-ordered open-time rows; current close is unfinished. Volume is base, turnover quote for these linear contracts. Historical rows paginated backwards; only closed candles used. No universal historical start date guaranteed. See [kline](https://bybit-exchange.github.io/docs/v5/market/kline). |
| `/v5/market/tickers?category=linear` | Bid/ask, mark/index, OI values, current funding and next settlement context. Snapshot/collection times retained. Not historical quote coverage. See [tickers](https://bybit-exchange.github.io/docs/v5/market/tickers). |
| `/v5/market/orderbook?category=linear&symbol=…&limit=50` | Supporting broad-stage depth snapshot; insufficient levels means reject, never extrapolate. See [book](https://bybit-exchange.github.io/docs/v5/market/orderbook). |
| `/v5/market/open-interest?category=linear&intervalTime=1h&startTime=…&endTime=…&limit=200` | Cursor pagination; linear base units. Current docs distinguish `openInterest` (sum of sides) and `singleOpenInterest`; preserve exact returned field without silently halving. First available timestamp is instrument-dependent. See [OI](https://bybit-exchange.github.io/docs/v5/market/open-interest). |
| `/v5/market/funding/history?category=linear&startTime=…&endTime=…&limit=200` | Backward timestamp pagination; start alone is invalid. Funding interval comes from instrument metadata, never assume eight hours. See [funding](https://bybit-exchange.github.io/docs/v5/market/history-fund-rate). |
| `/v5/market/mark-price-kline`, `/index-price-kline`, `/risk-limit` | Allowlisted extension endpoints, not yet integrated into full historical liquidation accounting. |

REST rate audit: default IP ceiling is 600 requests / five seconds; this application budgets
three per second with one shared limiter. HTTP 429, 5xx and `10006` receive bounded backoff;
remaining/reset headers inform rate-limit retries. HTTP 403 aborts the scan; do not bypass
regional access restrictions. Official IP-ban guidance calls for at least ten minutes before
reconnecting; the default scan delay is fifteen minutes. [Bybit rate limits](https://bybit-exchange.github.io/docs/v5/rate-limit)

`launchTime` alone does not prove the start of continuous trading for formerly prelisted
contracts. The implementation additionally requires at least 30 contiguous completed daily
bars with positive volume. This is a conservative availability check, not proof of every
intraday trading interval. TradFi underlyings are excluded using available metadata.

## Public WebSockets

Origin `wss://stream.bybit.com/v5/public/linear`; one connection, at most 30 configured
symbols (default eight), three topics per symbol. Application heartbeat every 20 seconds.
Official subscription argument-length limit is 21,000 characters, with no current futures
argument-count cap. IP limits: 500 new connections / five minutes and 1,000 concurrent
market-data connections per market category. Rotation occurs after scans, not every tick.
[Connection documentation](https://bybit-exchange.github.io/docs/v5/ws/connect)

| Topic | Semantics |
|---|---|
| `publicTrade.SYMBOL` | `S` is taker side, `v` size, `p` price, `i` trade ID, `T` fill timestamp. Up to 1,024 prints per message; multiple messages may share a sequence. Deduplicate by trade ID, not sequence. Preserve block/RPI flags raw; block prints excluded from continuous-book confirmation. [Trades](https://bybit-exchange.github.io/docs/v5/websocket/public/trade) |
| `orderbook.50.SYMBOL` | Default 50 levels / 20ms; supported linear depths currently 1/50/200/1000. Snapshot replaces state; zero size deletes; delta size replaces rather than increments. `u=1` resets after service restart. `seq` compares ordering and is NOT documented as a consecutive per-topic counter. `cts` is matching-engine time. RPI orders are excluded. Nonmonotonic/crossed/empty books reconnect and await snapshot; silent numeric jumps cannot prove absence of gaps. [Order book](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook) |
| `allLiquidation.SYMBOL` | Actual event feed, batched every 500ms. `S=Buy` means a LONG position was liquidated; `Sell` means SHORT. `p` is bankruptcy price, so `p*v` is bankruptcy notional, not a known realized execution value. No liquidation events during a healthy subscription is distinct from missing coverage. [All liquidations](https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation) |

Event time and receipt time are distinct. Disconnect/control events are recorded. Public WS
does not supply a historical replay cursor or proof of complete delivered trades; confidence
is bounded by observed healthy connection intervals, timestamp checks and local integrity.
The recorder stops on quota/overflow/write failure. Unknown data never earns confirmation.

## Historical archives

Official [trade directory](https://public.bybit.com/trading/) contains per-symbol directories,
including [BTCUSDT](https://public.bybit.com/trading/BTCUSDT/). The downloader requests
`https://public.bybit.com/trading/{symbol}/{symbol}{YYYY-MM-DD}.csv.gz` for an explicit day.
It validates gzip CRC, required CSV fields, symbol, aggressor side, positive prices/sizes,
timestamps and a SHA-256 manifest. SHA-256 is a locally computed integrity hash, not an
exchange-signed completeness guarantee. No universal start/end coverage is asserted.

The official [historical-data portal](https://www.bybit.com/en/derivative-activity/history-data)
exists, but this audit did not establish machine-readable order-book download coverage or
verify any depth archive. Automatic depth archive import is unavailable; retain actual live
snapshots/deltas for future replay. Historical all-liquidation coverage is also unverified.
No depth, liquidation stream, or footprint is reconstructed from candle wicks.

Current instrument directories do not provide a complete delisted-contract metadata history.
Point-in-time universe snapshots start when this application records them. Earlier backtests
are explicitly single-symbol or restricted-universe research and cannot support a
survivorship-free whole-market performance claim.

## Discord and other sources

Discord execute-webhook uses `wait=true`, embeds and `allowed_mentions.parse=[]`. Durable
delivery attempts record returned message IDs. Webhook rate limits are dynamic; rejected
or ambiguous attempts are journaled without blind duplicate retries.
[Webhook API](https://docs.discord.com/developers/resources/webhook),
[rate limits](https://docs.discord.com/developers/topics/rate-limits).

Fundamental facts are manual, source-linked, collection/knowledge/effective/expiry dated;
there is no unreliable automated token-quality score. Binance integration is unavailable
in this milestone and always displayed as such. Public access does not imply unrestricted
redistribution: retain attribution and check the exchange's applicable data terms before
publishing datasets. Application source is MIT; market data is not relicensed by this repository.
