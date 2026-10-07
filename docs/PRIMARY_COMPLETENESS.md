# Primary outcome completeness

Use this audit to explain missing `candidate-v10` / `prints-v1` outcomes. It does
not change strategy decisions, scores, Discord qualification, frozen plans,
training minimums, or the maturity epoch. Historical labels remain immutable.

## Continuity contract

The existing labeler uses receipt order for availability and venue trade time
for paper fills. Receipt times must be nondecreasing, including within segments;
equal times are valid. Worker preflight verifies hashes, manifests, counts and
bounds. Overlapping or clock-damaged segments are excluded. Adjacent segments
must reference the prior segment's identifier and hash. A normal chained
250 ms rotation is valid.

Trades more than `FLOW_TRADE_STALE_MS` apart lose coverage (default 15,000 ms;
equality is accepted). Subscription acknowledgements establish membership;
removal, disconnect, incomplete envelopes and source changes invalidate affected
coverage. The first retained print can bootstrap a previously unseen market
after retention, without retroactively qualifying an earlier decision. Complete
book-only recovery notices do not invalidate executed trades.

A decision activates when its timestamp is at or before the next envelope's
receipt. That envelope must be within the stale limit and the market subscribed.
Paper fills accept unique non-BT prints from decision + 500 ms through the earlier
of the frozen entry deadline or decision + 60,500 ms. Entry-zone bounds are
inclusive. Exits use TP1, stop or the frozen maximum holding horizon; exits beyond
that horizon are incomplete. The materializer allows 60 seconds after the horizon
before closing an unresolved observation. Accepted venue clock lead is at most
1,000 ms during normalization; event time is not substituted for receipt time.

## Audit commands

Run from the cloned repository directory with the ML profile running:

```sh
docker compose exec trainer bybit-flow ml audit-completeness --limit 50
docker compose exec trainer bybit-flow ml audit-completeness --limit 200 --json
docker compose exec trainer bybit-flow ml audit-completeness --days 7 --limit 10000 --json
docker compose exec trainer bybit-flow ml audit-completeness --days 30 --limit 10000 --json
docker compose exec trainer bybit-flow ml audit-completeness --source binance --horizon CORE_INTRADAY --json
```

The default selects the latest 200 unique economic identities. Repeated snapshots
are listed separately, not counted as independent outcomes. Daily/weekly cohorts
use UTC decision dates. Source, horizon and pre/post-epoch summaries are separate.
Completion is complete **stored** outcomes divided by mature decisions with known
eligible coverage. Open horizons and unknown historical eligibility are excluded
from that denominator and reported explicitly. A reconstructed result does not
become training data.

Raw decoding is limited to candidate windows and shared across candidates, with
hard limits of 1,500 segments, 3 million envelopes and 384 MB of compressed data
per invocation. Budget exhaustion, safely pruned files and unverifiable evidence
remain unknown; they do not prove either a false label or retention loss. Narrow
the source, horizon, date or candidate limit for another audit. Integrity failures
cannot produce a complete reconstructed result. No raw payload is copied into
SQLite. The audit function performs no writes; the CLI saves only its compact
summary after reading. `ml status` reads that cache without decoding recordings.
Its audit timestamp, scope and budget describe how current the result is.
Previously verified clock damage can also be attributed from content-addressed
worker audit files when their segment hashes and bounds match the retained
manifests. That evidence does not reconstruct trades from deleted files.

Operational status requires at least 30 mature eligible observations: 80% or
better is HEALTHY, 60–80% WATCH, below 60% DEGRADED. Smaller samples are
INSUFFICIENT_COHORT. These are diagnostics, not trading gates. Long-horizon
limitations are reported only when sufficiently sized short/core cohorts are
healthy and swing/extended cohorts degraded.

## Prospective eligibility and recorder evidence

New decision snapshots include `primary_ml_eligibility` outside the feature
allowlist. It requires an acknowledged subscription and at least one fresh valid
**committed** print at or before decision time. No multi-minute warm-up is imposed:
the existing continuity contract has no such duration. Publication lag can make
a decision diagnostically ineligible. That does not suppress its trading setup or
its internal monitoring. Ineligible decisions are not counted as later recorder
failures. Old snapshot payloads are not modified; their eligibility is derived
only when historical evidence can establish it.

New recorder envelopes retain `observed_receipt_ms` when a proven one-millisecond
wall-clock reversal requires a nondecreasing admission timestamp. The adjustment
is explicit and counted in recorder health. Venue event times and original clock
readings remain available. Larger reversals still fail strict replay; historical
recordings are never reordered or rewritten. Recorder drop controls identify the
affected venue, symbol, interval and dropped-event count. Global counters alone
are not used to attribute a candidate failure.

Retention loss requires committed evidence to have been deleted while unresolved
and absent from a valid durable checkpoint. A tombstone after an immutable label
does not establish loss. Invalid checkpoints fail closed in existing retention
protection, retaining the original unresolved interval. Audits preserve labels,
models, plans and delivery receipts. There is no automatic historical repair.

## Measured baseline: 7 October 2026

The read-only audit at 03:51 UTC covered decisions from 1 October 01:01 UTC
through 7 October 00:46 UTC. This is dated evidence, not a claim about future
runtime health. All current-schema decisions were Binance candidates; Bybit and
OKX had no current-schema decisions in these cohorts.

| Cohort | Unique | Duplicate snapshots | Mature | Not yet mature | Complete |
|---|---:|---:|---:|---:|---:|
| Latest 50 unique | 50 | 25 | 44 | 6 | 0 |
| Latest 200 available | 68 | 29 | 62 | 6 | 1 |
| Last 7 days | 68 | 29 | 62 | 6 | 1 |
| Last 30 days | 68 | 29 | 62 | 6 | 1 |

Among the 68 unique candidates, 41 had confirmed clock/ordering damage, one had
subscription readiness missing, one had a complete outcome, and 25 remained
unknown. Technical duplicates are separate from those 68. No false-gap label,
retention loss, or checkpoint inconsistency was proven in the available cohort.
The runtime's 11,352 cumulative queue drops could not be attributed to individual
candidates merely from that counter.

Historical eligibility was known for only four candidates: one eligible and
three ineligible; 64 remained unknown. Thus the mature-eligible completion rate
was 1/1, which is **INSUFFICIENT_COHORT**, not evidence of a healthy pipeline.
The six unmatured horizons already had incomplete labels; they were not six
clean positions simply waiting for exits. Only one unique decision was after
the preserved maturity epoch, and it had clock-damaged evidence.

| Horizon | Unique | Mature | Complete |
|---|---:|---:|---:|
| Short intraday | 3 | 3 | 0 |
| Core intraday | 52 | 51 | 1 |
| Swing | 10 | 8 | 0 |
| Extended swing | 3 | 0 | 0 |

No symbol had ten mature candidates with **known eligible** coverage, so the
problem-symbol threshold produced no ranked entries. The audit decoded 283
segments / 2,150,040 envelopes / 383,812,312 compressed bytes and reached its
byte budget. The unknowns must not be reclassified as real gaps or false labels
without more evidence.

The newest ARBUSDT interval independently contained 2,308 trade envelopes,
no inter-print gap above 15 seconds, and a valid 99-segment chain. Eight segments
nevertheless had a recorded one-millisecond receipt reversal. Strict replay
correctly excluded those historical bytes. The repair changes future recorder
admission timestamps with explicit original-clock metadata; it does not make
those old labels trainable. Book-only recovery notices were already handled
correctly and required no labeler change. Training minimums remain 500 usable
source-specific outcomes with the existing causal partitions and four-hour embargo.
