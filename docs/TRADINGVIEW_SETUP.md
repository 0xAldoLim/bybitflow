# Chart-event compatibility API

The optional chart-event backend is retained for existing integrations and historical
research. It is disabled by default and is not required for exchange-native scanning.
Pine scripts and public reverse-proxy deployment templates are not distributed.

## Interface

`POST /webhooks/tradingview` accepts the event schema in
`src/bybit_flow/tradingview.py`. Integration access requires
`FLOW_TV_ENABLED=true`, a `FLOW_TV_TOKEN` of at least 32 characters, and a matching
`X-TV-Key` header. Dashboard authentication does not replace this integration key.

The backend validates event identity, timestamps, symbol, schema, and price bounds;
stores events in the durable inbox; and rejects duplicate conflicts. Ingress must
be deployed behind authenticated HTTPS with credentials excluded from logs.

Chart classifications are source-attested observations, not exchange taker-side
trades. Chart and native features, labels, and models remain separate. Proxy research
does not provide independently observed depth or spread and is capped below SSS.

## Historical research

`bybit-flow tv-export` exports stored inbox events.
`bybit-flow tv-research --help` describes chart-event/OHLC research.
`bybit-flow ml chart-label --help` and `ml chart-export` support separate chart
datasets. These paths do not reconstruct missing native execution data.

Compatibility tests are in `tests/test_tradingview.py`, `tests/test_tv_research.py`,
and `tests/test_ml_integration.py`.
