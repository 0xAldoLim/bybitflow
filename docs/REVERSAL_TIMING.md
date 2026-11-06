# Staged reversal timing

Version 2.2.2 adds `exhaustion-reclaim-v1` to new `trade-validity-v22` range
rejections and liquidity sweeps in SHORT_INTRADAY, CORE_INTRADAY and SWING.
Continuation families and older persisted candidates keep their existing rules.
The original family confirmation runs independently; either valid mechanism may
confirm a new candidate, subject to all other qualification and delivery checks.

## Evidence sequence

The existing ten-second confirmation loop inspects a bounded retained 75-second
slice from the original venue. There is no additional scanner, stream, database,
recording pipeline, timer or training worker.

1. **Pressure, 45 seconds:** the frozen level was known before the context began;
   actual trades extend beyond it. Opposing signed notional is at least 10% of
   gross with persistence at least 0.6, consistent with the existing opposing-flow
   definition.
2. **Failed extension, 15 seconds:** price makes no further opposing extreme and
   its displacement is non-adverse. The full 60-second context must show actual
   defended absorption, or opposing executed-notional intensity must decline
   relative to the first 45 seconds. Falling activity alone is insufficient.
3. **Reclaim and turn, a separate 15 seconds:** price advances in the proposed
   direction and the final five seconds of observed prints remain beyond the
   frozen level. Existing trusted raw-flow fallback requirements apply: directional
   delta ≥15%, positive directional CVD slope, persistence ≥0.6 and initiative or
   defended absorption. The existing footprint defines initiative; it is not a
   new volume threshold. Touch-event OFI must be ready, directional and persistent
   (≥0.6) in this same trigger window. Trade delta and queue-event OFI stay distinct.

Both context and trigger must have flow trust ≥0.6. Phase boundaries require
observed trades near both ends; missing quiet periods are not fabricated. The
existing event-OFI readiness rules and continuous book epoch apply. Tape or book
resets require fresh warm-up. Wrong-source, future and incomplete evidence cannot
confirm. A fresh adverse liquidation-cascade classification blocks this route;
missing liquidation data supplies no positive exhaustion credit.

These window lengths and numerical definitions are predeclared research
assumptions. They were not fitted to historical losses and are not validated
profitable parameters.

## Scoring, execution and continuity

When only the staged route passes, its trusted 15-second trigger supplies the
existing mechanism/price-response assessment. Raw 60-second flow remains unchanged
in the decision. The two routes do not add scores together. The existing v21.1
control continues to evaluate the original full context and structural objective.
`confirmation.baseline_passed`, `sequence_passed`, `sequence_state` and compact
`reversal_sequence` measurements explain the difference on the same candidate.
They are observations, not a separately simulated strategy return.

Execution targets already consider causal range/value references, costs and
reachability. This release keeps that selection, structural stops, entry deadlines,
liquidity/stress, market alignment, macro pauses, V8 checks, score options, original
holding horizons, notification deduplication and durable Discord delivery rules.
Absorption without a reclaim and supportive flow never authorizes this route.
Deferred delivery refreshes its sequence separately from the frozen decision;
stale confirmation cannot authorize a delayed initial card.

Existing active plans are never retagged, moved, replaced or terminated by this
policy. A swing's entry timing does not shorten its holding lifecycle. Sequence
state uses existing candidate evidence; no migration or historical rewrite is
needed. Policy-tagged strategy versions remain subject to normal ML compatibility.
The feature schema remains `candidate-v20`; no projection, training, promotion or
model-authority change is included.

## Literature and validation

This operationalizes the supplied synthesis's P1 failed-break/reclaim and P5
aggression-versus-response ideas, within its P6 liquidity safeguards. The existing
[research ledger](v2/INSILICO_RESEARCH_LEDGER.md) records the MarketDelta,
auction/acceptance and O’Hara source review and its coverage limits. Absorption,
exhaustion, churn and continuing forced flow are distinct; no public footprint
identifies participant intent or remaining capital.

The [Cont–Kukanov–Stoikov study](https://arxiv.org/abs/1011.6402) concerns short-run
book-event imbalance/price impact in equities, not proven future crypto returns.
Forward evaluation should compare the preserved baseline and staged admissions
chronologically after costs, including rejected/expired candidates, entry delay,
missed opportunities, MAE/MFE and independent trading periods. The mechanism tests
verify causality and fail-closed behavior; they establish no win-rate improvement.
