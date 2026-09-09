# Implemented, experimental, unavailable

## TradingView feature-branch milestone (2026-09-09 UTC)

Implemented and exercised with synthetic/offline tests:

- Authenticated, bounded TradingView ingress; source time, symbol, schema, price and ID validation;
  durable SQLite v2 queue, deduplication/conflict rejection, restart recovery and queue expiry.
- Source-isolated lifecycle processing, mock Discord initial/invalidation cards, paper outcomes,
  heartbeat loss and setup expiry. The existing exchange scanner cannot process TV signals.
- Measured native seven-factor scoring replaces fixed fractions. Source-attributed fundamental
  diligence coverage and fresh cross-market observations can earn points. Quality is not probability.
- Separate, named TV score: regime 20, structure 25, classified footprint 30, risk 15, observed
  liquidity 10. Correlated structure features and trapped-participant heuristics do not add votes.
- Opt-in strict `SSS RESEARCH · UNCALIBRATED`; no validated label or predictive percentage.
- Opt-in TradingView turnover-proxy research: independent of Bybit connectivity, unsized, capped
  at 84, missing spread/depth clearly stated. Proxy data never unlocks strict SSS.
- Historical TV observation/actual-OHLC replay for reversal and continuation, separately by side,
  cost assumptions, fixed chronological partitions, embargo and sparse calibration abstention.
- Gateway status dashboard and domain/HTTPS/TradingView/Discord setup documentation.
- One-shot TradingView connection-test script and isolated non-trade notification workflow.
- Rebuilt Docker startup/auth smoke, Caddy/Compose config validation and desktop/mobile browser
  checks passed. Final suite: 54 tests. See VERIFICATION.md for exact scope and remaining checks.

Code added or changed but requiring further external/operational verification:

- Pine v6 indicator and simulation-only strategy use official footprint APIs, closed HTF offsets,
  confirmed pivots, sweep/reclaim, break/CHoCH, displacement FVG, per-bar PoC/VA, stacks and CVD.
  **No TradingView compiler/account was available.** Provider history revisions remain possible.
- Closed-bar candle caching, pending-setup refresh lane and incremental subscription rotation.
  Retained symbol state is preserved in unit tests; full live rotation/load is not tested here.
- Optional fresh Bybit observations can augment TV cards; required native confirmation fails closed.
  This host and Docker resolve Bybit to a Telkomsel filtering certificate; no bypass was attempted.

Still pending: actual TradingView compilation and real alert receipt; real Discord message
delivery; public DNS/ACME deployment; real historical datasets/studies. No paid account, domain,
VPS or Discord webhook was supplied. The synthetic delivery test does not satisfy real delivery.
Full-session/prior-week footprint profiles and exchange-native absorption cannot be inferred from
TradingView's per-bar classified volume. All lower historical milestone limitations below remain
unless explicitly superseded here. See TRADINGVIEW_SETUP.md and CONNECTIVITY_AUDIT.md.

This release is a runnable first milestone with subsequent research components. “Implemented”
means code exists and the listed behavior is locally exercised, not a validated financial edge.
See VERIFICATION.md for the actual execution record.

## Implemented foundations

- Public-only GET adapter, fixed endpoint allowlist, all instrument pages, backward closed-candle
  pagination, OI and funding pagination, conservative request budget and bounded retries.
- USDT-default / configurable USDC metadata selection. Non-perpetual, prelisting, nontrading,
  invalid precision, recent listings and known TradFi underlyings excluded.
- Thirty contiguous daily bars, seven-day median turnover, current and time-sampled normal
  spread, and actual visible depth gates. Point-in-time membership observations persist in
  both SQLite and raw replay records only once their supporting data is available.
- Broad ranking and bounded prioritized trade/book/all-liquidation subscriptions. Snapshot/delta
  reset, nonmonotonic/crossed/empty book detection, heartbeat, reconnect, separate timestamps.
- Bounded event count and byte budget; bounded tapes; compressed raw observations and typed
  Parquet, linked segment hashes, omitted-segment gap detection, manifest metadata, SQLite
  operational journal and backup command.
- Delayed confirmed pivots, internal/external swing views, structure/BOS/CHoCH flags, equal
  levels, sweep/reclaim, displacement-filtered three-bar FVGs, deterministic last-opposite-candle
  order blocks, dealing-range position, partial/complete prior-period levels and candle VWAP.
- Trade-based buy/sell base volume, notional delta, explicit delta percentage, window CVD,
  volatility/tick buckets, contiguous 70% value area, PoC, diagonal/stacked imbalance and VWAP.
- Visible depth at 5/10/25 bps, microprice, hypothetical book consumption, replenishment/depletion
  and visible flow imbalance. Depletion is not falsely labeled cancellation.
- Four distinct setup families with separate long/short branches, defined stop/zone/targets,
  15M execution triggers, persistent states and expiration/invalidation updates.
- Risk sizing by stop distance plus assumed costs, Decimal quantity rounding, minimum/maximum
  contract constraints, manual portfolio risk limits and conservative margin-stress rejection.
- Transparent specified score weights, no weight redistribution for missing evidence, separate
  probability status and rejection gates, hard public-alert lock.
- Research Discord embeds, persistent attempt ledger, deduplication, cooldowns, mention suppression,
  ambiguity tracking, secret-free configuration output and local password protection.
- Dashboard overview, watchlist filter, signal detail/footprint/CVD, market/depth/derivatives,
  source coverage, experiment display, manual paper journal, portfolio entry and settings view.
- Actual archive downloader/validator and trade aggregation; source-attributed manual fundamental
  facts with separate knowledge/effective/collection/expiry times.

## Experimental, not financially validated

- All regime thresholds, four setup families, directional rules, quality contributions, adaptive
  imbalance thresholds, defended-level absorption and initiative classification.
- Order-block and FVG algorithms are reproducible but are not evidence of institutional intent.
- OHLCV ATR baseline, fixed chronological splits, development parameter sensitivity, gross market
  comparison; event-level partial/missed fills, funding hooks and mark-margin stress simulator.
- Recorded-event replay shares features and candidate/confirmation functions, but coverage and
  context are a restricted research subset, not parity with every live gate. Reversal absorption
  uses actual historical book additions within the execution window; missing funding/mark data limits
  qualification. No synthetic historical order books are introduced.
- Weekly-cluster bootstrap and expanding-window stratified empirical probability estimates.
  Sparse strata abstain. These are research utilities, not a deployed calibrated predictive model.
- Docker/VPS configuration is supplied. See verification for build/run status.

## Unavailable or incomplete in this milestone

- Empirically validated live strategies, calibrated probabilities, SS/SSS qualification and S alerts.
  These require real independent evidence; no configuration option bypasses the lock.
- Complete delisted-contract historical metadata, survivorship-free universe before recording,
  and proved continuous-trading start time. Normal spread history starts with actual collection;
  it is not backfilled from current quotes or candle-derived estimates.
- Automatic historical depth or all-liquidation archive acquisition with verified completeness;
  automatic exact-gap recovery; exchange-signed trade completeness verification.
- Full-session/anchored/prior-week trade volume profiles and prior-week PoC across long recordings.
  Displayed footprint/PoC/CVD are explicitly 15M execution-window values. Session candle VWAP
  is available only with honest partial/complete coverage flags.
- Validated exhaustion, auction/retest persistence, HVN/LVN acceptance, book slope/concentration,
  lifetime/cancellation attribution and spoof-like persistence classifiers.
- Automatic Binance cross-exchange comparison; cross-market context currently shows BTC/ETH regimes.
- Automatic asset-specific adoption/revenue/unlock/treasury/insider/governance data collectors.
  Facts can be entered with sources; no automatic Buffett-style valuations or authoritative scores.
- Exact account-specific maintenance tiers and mark-price liquidation simulation, queue-position
  passive fills, measured slippage/latency distributions, actual portfolio correlation estimates.
- Full strategy-family out-of-sample studies, trade-level ablations, multiple-testing adjustment,
  untouched multi-regime holdout qualification or stable-regime SSS certification.
- Automatic historical archive scheduling/compaction, automatic retention deletion, PostgreSQL
  backend, multiworker deployment and atomic exactly-once Discord delivery.
- Settings edits through UI except manual portfolio input. Environment settings require restart.

These boundaries are intentionally visible. The software may emit no research cards even with
working feeds. It must not be described as institutional-grade execution or a profitable strategy.
