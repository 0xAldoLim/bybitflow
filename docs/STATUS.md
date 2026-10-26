# Feature status

Maintenance release 2.2.1 adds prospective policy observations and bounded
operational controls. Trading policy remains v2.2. See
[Reliability & Evidence Loop](RELIABILITY_EVIDENCE_LOOP.md).

## Supported

- Public REST and WebSocket adapters for Binance, Bybit, and OKX.
- Automatic source selection, fixed-source operation, and bounded multi-source comparison.
- Four deterministic setup families with closed-bar features and executed-flow confirmation.
- V2.2 Setup Score with Evidence/Tradeability diagnostics, bounded event OFI,
  response efficiency, movement-capacity reachability and stop viability.
- Causal execution-target selection with a retained structural objective,
  delivery-time quote assessment and immutable published plans.
- Informational opportunity ranking and shared exposure; no daily quota/top-N cap.
- SSS–F research alerts with entry zone, stop-loss, TP1/TP2, lifecycle, and deduplication.
- Source-attributed asset facts with knowledge, collection, and expiry timestamps.
- Bounded recording, integrity manifests, source-separated paper labels, and audited clock gaps.
- Optional ML collection, challenger training, ranking, drift checks, and guarded promotion.
- Durable primary replay checkpoints and safe release of already-consumed ML evidence.
- Source-specific historical OHLC bootstrap labels and advisory-only challengers.
- Verified Binance aggregate-trade recovery for incomplete current-schema primary
  outcomes, retaining original decisions, complete labels and fill/cost rules.
- Current-feature monitored learning recovers original-venue price paths after
  host downtime, independently of strict print continuity.
- Authenticated local dashboard, diagnostics, backup, and retention planning.
- Separate forward exit-policy research at first decision and successful delivery.
- Historical TradingView records remain readable; new chart-event ingestion is retired.

## Limitations

Research scoring and model performance require empirical validation. Current print
labels use assumed fees, slippage, and funding reserves; they cannot satisfy the
verified-cost model promotion gate.

V2.2 constants are explicit research assumptions. Capacity quantiles and the
Setup Score are not target-hit or win probabilities. Retrospective exports with
missing candle/profile/quote-event inputs cannot establish a full v2.2 return
comparison. See [v2.2 policy details](V22_SETUP_QUALITY.md).
The [fixed selection replay](V22_VERIFICATION.md) did not improve retained proxy
returns. Forward validation remains necessary; no new score cutoff was fitted.

OKX liquidation collection, cross-venue funding/OI clustering, durable multiweek
profiles, and full Binance/OKX strategy-regeneration replay are not implemented.
Frozen-candidate outcome labeling supports each implemented native source.

ML does not automatically change strategy code, score weights, risk limits, or
model approval. Bootstrap OHLC models cannot filter signals or become champions.
Automatic model rollback is not implemented.

## Runtime status

Deployment health is available through `/healthz`, `/api/exchanges`, and
`/api/ml`. Repository status does not establish current network connectivity,
Discord delivery, training-data readiness, or the presence of an approved model.

See [verification](VERIFICATION.md) for test scope and [operations](OPERATIONS.md)
for diagnosis.
