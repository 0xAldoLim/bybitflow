# Implemented, experimental, unavailable

This release is a runnable first milestone with subsequent research components. “Implemented”
means code exists and the listed behavior is locally exercised, not a validated financial edge.
See VERIFICATION.md for the actual execution record.

## Implemented foundations

- Public-only GET adapter, fixed endpoint allowlist, all instrument pages, backward closed-candle
  pagination, OI and funding pagination, conservative request budget and bounded retries.
- USDT-default / configurable USDC metadata selection. Non-perpetual, prelisting, nontrading,
  invalid precision, recent listings and known TradFi underlyings excluded.
- Thirty contiguous daily bars, seven-day median turnover, current spread and actual visible
  depth gates. Point-in-time membership observations persist from collection onward.
- Broad ranking and bounded prioritized trade/book/all-liquidation subscriptions. Snapshot/delta
  reset, nonmonotonic/crossed/empty book detection, heartbeat, reconnect, separate timestamps.
- Bounded event count and byte budget; bounded tapes; compressed raw observations and typed
  Parquet, segment hashes, manifest metadata, SQLite operational journal and backup command.
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
  replay abstains without matched historical replenishment; missing funding/mark data limits
  qualification. No synthetic historical order books are introduced.
- Weekly-cluster bootstrap and expanding-window stratified empirical probability estimates.
  Sparse strata abstain. These are research utilities, not a deployed calibrated predictive model.
- Docker/VPS configuration is supplied. See verification for build/run status.

## Unavailable or incomplete in this milestone

- Empirically validated live strategies, calibrated probabilities, SS/SSS qualification and S alerts.
  These require real independent evidence; no configuration option bypasses the lock.
- Complete delisted-contract historical metadata, survivorship-free universe before recording,
  proved continuous-trading start time, and a historical rolling “normal spread” estimator.
  Current spread is explicitly a current observation, not normal/historical liquidity.
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
