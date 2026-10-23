# BybitFlow v2.2: Setup Score and trade construction

This release uses **RESEARCH-GROUNDED ASSUMPTIONS**, not validated alpha.
**Setup Score is NOT a calibrated win probability.** Formulas were not fitted
to the retained loss list. Grades and configured alert options are unchanged.
There is no daily quota, top-N delivery cap or ranking-based correlation veto.

## Policy and historical boundaries

New opportunities receive `trade-validity-v22` and
`bybitflow-v22-setup-1`. Earlier v21/v21.1 opportunities retain their validity
checks, prices, score rubric and original lifecycle. Deployment cannot replace,
terminate or re-cluster an already active setup's monitoring.

ML remains `candidate-v20`. New evidence is not automatically added to its
feature allowlist. Existing snapshots, labels, models and maturity are retained.
Source-specific minimums remain **500 complete primary outcomes and 500 complete
sequences**, with chronological partitions, purge, embargo, independent holdout,
compatibility and abstention. Bootstrap/monitored challengers remain advisory;
there is no automatic promotion, threshold fitting or score retuning.

## One public score, two internal engines

```text
E = .30 Location + .25 Structure + .30 Mechanism + .15 Context
T = .30 FollowThrough + .30 TargetReachability + .20 StopGeometry + .20 Execution

base = .55 E + .45 T
raw = .75 base + .25 min(E, T)
SetupScore = round(min(raw, EvidenceConfidenceCap), 1)
```

Components are 0–100. Unknown components have score `None`, earn no
favorable credit, and retain an explicit possible-score upper bound. Fixed
weights are not redistributed. Unknown context is not classified as adverse.
Required causal structure, covered native trades, trusted response, accepted
risk and a fresh acceptable quote are necessary for a credible assessment.
OFI/profile/capacity/path readiness each adds five points to an 80-point
confidence ceiling, bounded by flow trust.

Location uses family-specific range/value geometry. Structure preserves the
v21.1 reclaim, acceptance, retest and trend checks. Context groups available
factor/spot/perpetual/breadth observations without inventing directional views.

Delta, CVD, stacks, runs and aggressive volume estimate **one aggression
concept**, not six conviction votes. Mechanism and FollowThrough intentionally
share response observations across the two engines. They are not independent
statistical confirmations.

## Quote-event OFI

The touch calculation follows the queue-event definition in
[Cont, Kukanov and Stoikov](https://arxiv.org/abs/1011.6402):

```text
e = I(new_bid >= old_bid) * new_bid_quantity
  - I(new_bid <= old_bid) * old_bid_quantity
  - I(new_ask <= old_ask) * new_ask_quantity
  + I(new_ask >= old_ask) * old_ask_quantity
```

It differs from static OBI and trade delta. Quote depletion combines executions
and cancellations; public quotes cannot identify which occurred. The paper
concerns contemporaneous price impact in US stocks, not profitable crypto
thresholds or proven future prediction.

`event-ofi-v1` reads 1/5/15/30/60 completed-second windows from the existing
bounded 120-second bucket deque. Touch OFI uses base quantity. The 5/10/25-bps
variants are signed observed changes at fixed prices within the current-mid
band, in quote notional; they are not the touch estimator. Each uses its own
depth normalization, direction, persistence and half-window acceleration.

Readiness requires observed events in at least 75% of window seconds, plus five
events for 15/30/60-second windows. These are coverage assumptions, not raw
cross-asset pressure cutoffs. Quiet seconds are not fabricated. Reset,
snapshot/reconnect, epoch changes, backwards clocks and quote gaps over five
seconds break new-policy continuity. Future-mutated buckets cannot be read as
earlier evidence. States: READY, WARMING, STALE, GAPPED, UNAVAILABLE.
Historical `ofi-v3` interpretation remains available for older policies.

## Impact efficiency

`impact-efficiency-v1` relates aggression to signed bps/execution-ATR
displacement, persistence, depth consumption, replenishment, OFI, spread and
trust. Positive ATR response x uses `100*x/(x+.15)`. Price response and
persistence mix 70/30 with a further weakest-response adjustment, multiplied by
trusted directional aggression (fully supported at 20% delta) and trust.
Ready opposing OFI reduces the pathway by 25%. Replenishment greater than
depletion reduces it smoothly, with at most a 40% reduction. Missing OFI stays
UNKNOWN rather than supplying an opposing vote.

Reversal families can instead qualify through defended absorption plus structural
response. The mechanism takes the **maximum** valid directional/absorption path,
not their sum. Dramatic volume without displacement cannot produce high
follow-through merely through multiple correlated transformations.

Trusted aggression of at least 10% with 0.05 execution ATR of adverse response,
or two distinct post-trigger failed-progress windows with replenishment, can
contradict the mechanism. Existing v21.1 protections also remain enforced.
Weak/zero progress alone reduces credit rather than banning every reversal.
These normalized constants are transparent initial assumptions, not fitted
values.

## Movement capacity and targets

`horizon-movement-capacity-v1` uses at most 512 completed setup bars.
Every pseudo-start requires its own prior 14-bar true-range mean and a full
subsequent horizon completed before the current decision. Invalid/gapped windows
are excluded. Fractional horizons interpolate adjacent fully historical bar-window
MFEs; this smooth approximation is explicitly recorded and does not reconstruct
an intrabar path. Every eligible pseudo-start in the bounded history is used.
At least 20 valid pseudo-starts are needed; overlapping windows are not
independent trades.

Historical q50/q75/q90 favorable movement in start-time ATR units scales to the
current setup ATR:

| Distance versus mature capacity | State | Initial score |
|---|---|---:|
| At/below q75 | REACHABLE | 90 |
| Above q75 through q90 | STRETCH | 40 |
| Above q90 | LOW_REACHABILITY | 10 |
| Insufficient/invalid history | UNKNOWN | Unknown |

These are not probabilities of hitting a target before a stop. Existing
volume-weighted path density and obstacle strength remain in use. Tick-equivalent
POC/HVN/VAH/VAL/AVWAP/reference aliases do not count independently.
HEAVY_FRICTION caps target credit at 35; an intervening level alone does not veto.

`execution-target-v1` preserves `structural_tp1` and considers at most 24
causal targets: the original objective, frozen intermediate structure/profile
levels, and reserved q75/q90 capacity candidates. Prefer a reachable original
structural objective; otherwise use mature reachability, weighted friction and
after-cost RR. Every choice must respect direction, tick geometry and configured
minimum net RR (default 2). No target beyond mature q90 qualifies. Unknown
capacity does not invent a rejection.

Backward levels are discarded before bounding the candidate set. A first target
cannot cross the plan's existing second target; target ordering remains valid.

No economically viable choice produces TARGET_GEOMETRY_UNTRADEABLE. New TP1 is
the selected `execution_tp1`; the structural objective remains visible.
Targets are not mechanically shortened and never change after publication.

## Stops and execution

`stop-viability-v1` assesses the supplied stop against setup/execution ATR,
normal noise, adverse-wick q75/q80/q90, spread, impact, cost/R and remaining RR.
**It never widens a stop or changes account sizing/leverage.**

Stop/noise below 1 is NOISE_CONFLICT; 1 to below 1.5 is TIGHT_BUT_VALID;
at least 1.5 is HEALTHY_STOP. Credit uses `100*r/(r+.75)`, with
NOISE_CONFLICT capped at 35. Existing maximum structural widths remain
1.5/2.25/3 setup ATR for short/core/swing. Noise-safe room that would violate
these limits or configured RR is UNTRADEABLE_GEOMETRY. Existing stop-width,
coverage, liquidity-stress and entry safeguards remain mandatory.

Use best ask for LONG and best bid for SHORT. Costs preserve existing reserves:
spread is embedded in executable prices, with only residual impact beyond the
quote added when necessary. Widened spread, insufficient depth, excessive impact,
crossed stops/targets, expired windows, chased/adverse-zone entries and degraded
RR can block publication. Execution credit reflects current RR, cost/R and
relative-spread deterioration. No new fitted score cutoff was introduced.

## Frozen delivery and monitoring

The first decision/ML snapshot remains immutable. Immediately before an initial
Discord dispatch, refresh native flow/confirmation, book/OFI, remaining capacity
and execution evidence. `delivery_execution_snapshot` freezes the observed
bid/ask, executable price, quote age, spread/depth/impact, gross/net RR, entry
distance, zone, reachability, stop viability, OFI, response, confirmation and
Setup Score. A final synchronous check follows client setup; a further guard
catches staleness during persistence. Store the full snapshot and exact card
before dispatch.

Edge-age evidence retains the strongest response observed while this candidate
is scored and the last supportive OFI observation. It does not relabel the latest
window as the strongest, or invent an unobserved historical peak.

After successful delivery, plan/score/evidence remain frozen. Current health is
separate under `coverage.v22_current`. Two distinct adverse minute windows
can yield a deduplicated SETUP DETERIORATING notice; it is not an account-position
claim or a replacement plan. Original lifecycle and terminal checks remain
authoritative. Every lifecycle notice requires that exact setup's sent initial
and message ID. Unseen children cannot generate orphan notices.

An uncertain HTTP result is never automatically resent. A v2.2 rejection before
**any HTTP dispatch** can be reconsidered inside the original window only when
durable state proves no HTTP attempt occurred. Provisional rejection scoring
does not latch the confirmation window closed.

## Ranking and customer presentation

Rank all valid opportunities by Setup Score, then Tradeability, follow-through,
reachability and remaining net RR. Shared factor exposure is informational;
neither lower rank nor correlation adds an alert cap. Actual configured account
risk limits remain separate safeguards.

True duplicate recognizers share one visible opportunity when
source/symbol/direction/horizon, event and executable entry/stop/target match.
Each internal ID retains its lifecycle. Identical already-visible older plans
are recognized without rewriting their identity or monitoring.

Discord shows one Setup Score/grade, executable and structural targets,
flow/geometry/context, strengths, limiting factors, timing and invalidation.
Dashboard detail exposes frozen delivery evidence, current health and separate
exit research. Advisory model ranks are not a second headline probability.

## Forward research and limitations

New v2.2 opportunities have separate first-decision and, only when actually
delivered, post-acknowledgement delivery research stages. Delivery-stage proxy
entry cannot precede acknowledgement; quote-observation time is retained
separately. These stages never replace primary ML labels.

Fixed variants share entry assumptions, original risk denominator and assumed
costs: execution TP1, structural TP1, +0.50R, +0.75R, +1R, 50% at +0.75R plus
structural remainder, and time decay. Time decay exits at/after minimum hold
when favorable excursion remains below +0.25R. These are research outcomes,
not live exit instructions.

Record +0.25/+0.50/+0.75/+1R milestones, target times, stop ordering,
pre-stop horizon MFE/MAE bounds, net R and time exits. Full-candle touches are
hypothetical, not verified fills. Partial decision candles, same-candle ordering
ambiguity and gaps cannot manufacture complete outcomes. Durable cursors allow
research to resume after downtime.

Fixed OFI/reachability/stop score-and-gate ablations refer to the same frozen
execution plan, not invented alternative target paths. The v21.1 control uses
the original structural objective. Workers share one original-venue candle
response per market across old swing and new research jobs, with oldest-due
bounded scheduling. Worker I/O bounds are not trading quotas. No new raw feed
or unbounded event buffer is added; the Decimal/Tape memory work remains intact.

Historical exports may lack original candle sequences, raw profile bins or
multi-window quote-event state. Replay must mark those inputs UNKNOWN.
Filtering outcomes at their original targets is not a simulation of new targets.
Report matched cohorts, unsupported records, fidelity and descriptive differences.
Do not call retrospective results validated forward profitability or repeatedly
retune constants against them.
