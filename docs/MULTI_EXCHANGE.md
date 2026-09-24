# Public exchange adapter audit

Audit dates: 2026-09-09–10. No exchange credentials, trading endpoints, TLS bypasses or
paid chart services are used. `exchanges.py` preserves wire responses separately
from its documented internal scanner-v1 compatibility view.

## Official sources and schema decisions

- Binance REST: `https://fapi.binance.com`: `/fapi/v1/time`, `exchangeInfo`,
  `ticker/bookTicker`, `premiumIndex`, `klines`, `depth`, `fundingRate`,
  and `/futures/data/openInterestHist`. Public instrument filters, not display
  precision, determine tick and quantity steps. Leverage brackets require account
  context: this adapter conservatively uses an explicitly illustrative 1x cap.
  OI history is limited by the exchange (not a complete historical dataset).
  [Official REST reference](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).
- Binance WS trades/liquidations use `/market`; depth uses `/public` under
  `wss://fstream.binance.com`. Unrouted legacy examples are obsolete.
  Buyer-is-maker means aggressive sell. Aggregate trades are exchange-reported
  executions aggregated by the venue, not individual fill counts.
  [Migration notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice).
- Binance depth must bridge buffered `U/u` to REST `lastUpdateId`, then require
  `pu == previous u`. Snapshot levels have absolute base quantities; zero deletes.
  [Sequence protocol](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).
- Bybit keeps the audited V5 REST adapter and public linear trade/book/allLiquidation
  streams. See [DATA_SOURCES.md](DATA_SOURCES.md). No undocumented consecutive
  sequence requirement is invented for Bybit.
- OKX: public SWAP instruments, tickers, history-candles, books, funding-rate,
  funding-rate-history and open-interest under `https://www.okx.com/api/v5`.
  `BTC-USDT-SWAP` maps to canonical `BTCUSDT` only after linear USDT metadata
  checks. `ctVal * ctMult` converts contract quantities to base units; unsupported
  quote-valued or inverse specifications are rejected. Daily bars use `1Dutc`.
  Current OI is sampled locally, never relabeled as backfilled OI history.
  [Official reference](https://www.okx.com/docs-v5/en/).
- OKX public WS is `wss://ws.okx.com:8443/ws/v5/public`. As of June 23, 2026,
  book checksum fields are fixed to zero and deprecated. Integrity must rely on
  `seqId/prevSeqId`, not comparisons against obsolete CRC examples.
  [Official changelog](https://www.okx.com/docs-v5/log_en/).

Requests are bounded and paced; rate-limit/access-denial circuits stop requests.
This is not a claim of loaded-VPS capacity or exhaustive real-feed compatibility.
Funding interval unavailable means an explicit hourly reserve assumption, not an
invented settlement schedule. Missing public limits/context remain visible.

## Reference implementation and licensing

[tiagosiebler/orderflow](https://github.com/tiagosiebler/orderflow) was inspected
for venue-separated execution aggregation and price-bucket imbalance design.
It uses a different Node/TimescaleDB stack. No code was copied or imported; the
existing Python footprint and lightweight storage are retained. The reference
repository's [MIT license](https://github.com/tiagosiebler/orderflow/blob/master/LICENSE)
was reviewed. This project does not depend on it or on edgedepth-terminal.

## Executed connectivity evidence

Host and isolated Docker probes on September 10 did not establish any healthy live
source. REST failed with certificate/connectivity errors (Bybit also timed out in
Docker); Binance/Bybit trade WS failed certificate verification. OKX's normalized
WS test could not progress past mandatory REST contract metadata, so this is not a
successful or independently completed OKX WS handshake. Host DNS returned filtering
service addresses/names; system time reported NTP synchronized. Correct-SNI hostname
verification failed. Certificate verification stayed enabled. No DNS override,
alternate geographic routing or regional-access bypass was attempted.

The official Binance historical archive was accessible. The published SHA256 matched
and 115,985 LINKUSDT aggregate execution records from January 1, 2024 were converted
to Parquet and used for actual footprint calculations. This proves historical archive
and feature processing, not current REST/WS compatibility, historical DOM or strategy
profitability. See [VERIFICATION.md](VERIFICATION.md) for reproducible checks and their scope.

Payload/sequence and adapter-to-scanner tests use synthetic HTTP transports. Run
`bybit-flow test-market --exchange binance` (or bybit/okx) on your own lawful deployment
host to establish its actual REST and normalized trade WS evidence.

## Implemented operating modes

`FLOW_MARKET_SOURCE=auto` probes Binance, Bybit, then OKX, retaining a working primary
until a failure requires requalification. `binance`, `bybit` and `okx` are fixed modes;
they never silently switch. `multi` uses the same primary selection and bounded,
separate secondary collectors for a bounded subset of up to eight selected symbols.
Primary deep capacity is configurable. This is not maximum-depth subscription
to the whole exchange. A source transition clears continuity-dependent state and is
persisted; old-venue BTC/ETH regimes and stale cross-venue comparisons cannot earn credit.

Current cross-venue outputs include aligned price dislocation, spread dispersion,
trusted effective-flow agreement, and same-window delta. New candidates may use
`cross-venue-flow-substitution-v1` when local prints are low quality and two independent
remote venues have trusted same-direction effective flow, price response and book
response in the closed decision window. Local structure, entry, spread, risk, macro
and coverage gates still apply. Tapes and contract units are never pooled. Predictive
weight is zero pending OOS testing. Cross-venue funding/OI dispersion, CVD agreement
and liquidation clusters are explicitly unavailable. Bybit liquidation events and
Binance's sampled forceOrder events retain their different semantics; OKX liquidation
collection is not implemented. Historical full-strategy replay remains Bybit-specific;
native frozen-candidate outcome labeling is source-separated for all implemented venues.
