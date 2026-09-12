# Feature status

## Supported

- Public REST and WebSocket adapters for Binance, Bybit, and OKX.
- Automatic source selection, fixed-source operation, and bounded multi-source comparison.
- Four deterministic setup families with closed-bar features and executed-flow confirmation.
- SSS–D research alerts with entry zone, stop-loss, TP1/TP2, lifecycle, and deduplication.
- Source-attributed asset facts with knowledge, collection, and expiry timestamps.
- Bounded recording, integrity manifests, source-separated paper labels, and audited clock gaps.
- Optional ML collection, challenger training, ranking, drift checks, and guarded promotion.
- Authenticated local dashboard, diagnostics, backup, and retention planning.
- Optional chart-event API and historical chart research for compatibility.

## Limitations

Research scoring and model performance require empirical validation. Current print
labels use assumed fees, slippage, and funding reserves; they cannot satisfy the
verified-cost model promotion gate.

OKX liquidation collection, cross-venue funding/OI clustering, durable multiweek
profiles, and full Binance/OKX strategy-regeneration replay are not implemented.
Frozen-candidate outcome labeling supports each implemented native source.

ML does not automatically change strategy code, score weights, risk limits, or
model approval. Incremental replay checkpoints and automatic rollback are not implemented.

## Runtime status

Deployment health is available through `/healthz`, `/api/exchanges`, and
`/api/ml`. Repository status does not establish current network connectivity,
Discord delivery, training-data readiness, or the presence of an approved model.

See [verification](VERIFICATION.md) for test scope and [operations](OPERATIONS.md)
for diagnosis.
