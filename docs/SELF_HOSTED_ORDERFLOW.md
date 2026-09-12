# Architecture

## Native data and continuity

VenueAPI exposes an explicitly internal scanner-v1 compatibility view. Actual Binance
and OKX wire responses are recorded separately, never claimed to be Bybit responses.
OKX contract quantity × ctVal × ctMult becomes base quantity before risk or Book use.
To express an illustrative OKX quantity in contracts, divide its base quantity by the
recorded multiplier. Unsupported inverse/quote-valued specifications are rejected.

Binance uses REST snapshot plus buffered U/u bridge, then pu continuity. OKX requires
snapshot plus matching prevSeqId and rejects resets; obsolete checksums are not used.
Bybit retains audited V5 snapshot/delta semantics. Prices and quantities use Decimal.
New native connection IDs, separate event/receipt times, raw source, schema and quality
are retained. Auto source changes reset tape/book/context and invalidate affected plans.
Fixed modes never silently switch. Failover is conservative, not zero-gap or zero-latency.
Raw envelope schema remains v1; additive normalized observation schema is explicitly v2
in each new row and segment manifest. Old segments are not rewritten. Unknown generic
observation exchange remains null, not assumed Bybit. Binance funding freshness uses the
premium-index observation time, not a newer book-ticker time.

Deep capacity defaults to eight primary symbols. Core/pending collection can begin before
the broad scan finishes. Retained symbols keep their connections; recently alerted setups
remain subscription candidates through the paper holding deadline. Capacity overflow means
some pending candidates cannot qualify; missing coverage remains explicit. One immutable
first-covered execution decision is scored per setup ID, avoiding feature/label mismatch.

Multi mode collects at most the first two configured core symbols on each secondary venue.
Venue tapes stay separate. Current comparisons: price dislocation, spread dispersion and
same-window delta sign. They earn zero predictive score credit without OOS evidence.
Funding/OI dispersion, liquidation clusters and independent session-CVD comparison remain
unavailable rather than silently approximated.

## Footprint and profiles

All calculations use actual exchange-reported executions. Binance records are aggregates;
trade count/average size are not interchangeable with another venue's individual-fill count.

- Delta is buy minus sell base; percentage divides by total buy plus sell base. Notional
  delta sums signed price × base size. CVD is explicitly window-scoped.
- Price buckets are tick-aligned max(tick, ATR/100 rounded down to tick). Diagonal buy
  compares against sells one bucket lower, and vice versa. Denominators below 10% of
  median bucket volume do not qualify; adaptive ratio stays within 3–6. Stacks require adjacency.
- PoC is maximum base-volume bucket, low-price ties first. Contiguous value area expands
  toward the larger adjacent observed bucket until 70% volume is covered, low ties first.
  Empty price gaps contain no fabricated volume.
- HVN: observed bucket volume ≥1.5× median; LVN: positive volume ≤0.5× median.
- Absorption requires opposing aggressive notional, small displacement and time/level-matched
  passive additions. Delta alone never qualifies it.
- Potential trapped buyers: delta ≥20%, upward excursion ≥0.3 ATR, then last print below
  both first print and VWAP. Sellers mirror this. Actual inventory remains unknown.
- Unfinished auction heuristic: both buy/sell at an extreme bucket exceed denominator
  protection. Large execution: size ≥5× window mean, not a whale identity.

Views support last closed 1/5/15/60-minute, rolling 4H, UTC 8H session, day/prior day,
week/prior week only when continuously retained actual prints cover the entire window.
Bounded tapes often cannot retain long periods on busy symbols. Missing windows display
unavailable. Persistent multiweek aggregation is not claimed.

DOM includes 5/10/25 bps depth, microprice, spread, impact, additions/depletion, largest-level
concentration and (depth25-depth5)/20 slope. Depletion combines fills/cancels, not attributed
cancellation. Five-second frames are compressed in recording segments; up to 720 per symbol
are kept in memory, latest 120 via API. UI shows sampled liquidity history, not yet a graphical
heatmap or validated wall/intent detector.

## Derivatives and research boundaries

Bybit all-liquidation prices are bankruptcy prices. Binance forceOrder is sampled, not a
complete liquidation census; last-fill quantity × reported average price is approximate
notional. OKX liquidation stream is not implemented. OKX current OI is sampled locally,
not mislabeled as backfilled history. Unknown settlement intervals use an explicit hourly
funding reserve assumption; no account-specific leverage brackets are invented.

Feature schema v4 adds trapped-participant and mean-execution-size fields. Old snapshots
and models remain preserved. Exports separate schema and venue methodology. Current labels
use assumed costs, exclude gaps/unresolved outcomes and cannot approve validated SSS.
The earlier full-strategy replay is still Bybit-specific and explicitly refuses Binance/OKX
input. Source-tagged native recordings can label frozen candidates through `ml label`.
No historical depth or point-in-time universe is fabricated from trade archives.

Remaining extensions include full native strategy-regeneration replay parity, broader
feature ablations/optimizer dimensions, verified settlement-cost labels, automatic promotion,
durable multiweek profiles and statistically validated auction/wall features. These are not
described as completed merely because their raw inputs can be recorded.
