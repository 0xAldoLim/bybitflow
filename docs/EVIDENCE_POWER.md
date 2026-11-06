# Evidence power — 2.2.3

**Research only. No profitability claim. No production change.** Every row
described here is `research_only=True` and `production_ml=False`. Nothing
changes a Setup Score, gate, delivery decision, Discord card, lifecycle, risk or
cost formula, ML feature (`candidate-v20`), ML minimum, or promotion authority.
A fixed fixture renders the same score, gates, plan and Discord payload,
byte for byte, as 2.2.2 (`tests/fixtures/production_v222_golden.json`).

The release adds paired shadow measurements and statistics, so forward data can
answer five questions without pooling across source, policy or provenance.

## Measurements

| Question | Measurement | Pairing |
|---|---|---|
| How fast does setup value decay after delivery? | `delay60`, `delay180`, `delay600` stages (job suffix `:delayN`) | Same opportunity and exit variant as the `delivery` stage |
| Does flow confirmation add value? | `structure_only_control`; structural time and planned price on confirmed opportunities | Cohort comparison; confirmation entry cost is paired |
| Are score/policy differences credible? | Day-resampled, clustered intervals; Holm-adjusted p-values | Variant minus `v22_actual` on one opportunity |
| How sensitive are outcomes to fills and costs? | `decision_trade_through` stage; 1.5×/2× cost; ambiguity bounds | Trade-through minus touch on one frozen plan |
| Do repeated attempts underperform? | `attempt_ordinal`, `pullback_ordinal`, `first_attempt_only` variant | Cohorts and a shadow filter |

### Delay ladder

Entry is the open of the first complete 1m bar starting at or after the durable
sent-initial timestamp plus the delay, moved against the trader by half the
delivery-snapshot spread. The delivery-snapshot cost is added. Stop, targets
(including R targets fixed from the delivery entry), deadlines and the original
risk denominator stay frozen. `delayed_entry_risk` and `net_r_delayed_entry_risk`
are also stored. Because entry is at the open, the whole entry bar follows the entry.

A stop or execution-target crossing between delivery and the delayed entry, an
open beyond either level, or an expired entry window gives `MISSED_INVALIDATED`,
`MISSED_TARGET_PASSED` or `MISSED_EXPIRED`. These rows are kept and count as
**explicit zero exposure**, never as losses. A crossing inside the minute of
acknowledgement cannot be ordered: `UNKNOWN_PRE_ENTRY_ORDER`. A missing delivery
quote gives `UNKNOWN_SPREAD`. Delays are configured with `FLOW_FORWARD_DELAY_SECONDS`.

### Structure-only control

A v2.2 candidate whose only gates are executed-flow confirmation failures
(`executed order flow did not confirm family trigger`, `UNCONFIRMED_LIQUIDITY_SWEEP`,
`LOW_INFORMATION_EXECUTED_FLOW`) is enrolled once per candidate identity. Its
decision is that structural assessment (`v22.available_ms`), with the frozen
plan, a touch entry and the same causal path engine. It never enters the outbox,
Discord or primary ML. `eventual_flow_status` (`LATER_FLOW_CONFIRMED`,
`NOT_FLOW_CONFIRMED`, `UNRESOLVED`) is an **ex-post cohort label** resolved at
report time. It is never a decision input.

Flow-confirmed opportunities also store `structural_decision_ms`,
`structural_planned_entry` and `confirmation_cost_r`, which is the signed executable
price at confirmation minus the planned entry, in original R.

### Fill and cost bounds

`decision_trade_through` enters only when a bar touches the planned entry *and*
trades through it: `low < entry − tick` for LONG, `high > entry + tick` for SHORT.
Exactly one tick beyond does not qualify. Touch is the optimistic fill and
trade-through the conservative one, reported as separate paired variants.

Stop exits use a **fixed** slippage assumption inside the stored `cost_per_base`.
No depth-based stop-exit model enters any outcome. As a diagnostic only, each
opportunity stores hypothetical notional divided by exit-side visible depth
within 5/10/25 bps of mid **at decision**. That is not depth at stop time.

New labels store `gross_r` per exit variant. Cost stress recomputes net R at
1.5× and 2× the stored cost from stored gross R only. Older labels without
`gross_r` report `UNKNOWN`. Ambiguous same-bar orderings are shown twice, side by side:
conservative (stop first) and optimistic (target first). They are never merged.

### Attempt numbering

At creation, `attempt_ordinal` counts earlier candidates of any status with the same
source, symbol, family, direction and tick-rounded frozen reference level, inside
a configurable lookback (`FLOW_FORWARD_ATTEMPT_WINDOW_HOURS`, default 72).
`prior_attempt_outcomes_known` lists only research outcomes already resolved
before this decision. For `trend_pullback`, `pullback_ordinal` counts since the
frozen trend-qualification time, or `UNKNOWN` without one. The status is `EXACT`
only when the whole window lies after the evidence-power epoch and no admission
deferral gap overlaps it. Otherwise the status is `LOWER_BOUND`. Rows are immutable,
so a later candidate or outcome cannot change an earlier ordinal. These values
are research evidence and forward-report cohorts, never ML features.
`first_attempt_only` shadow-rejects known repeats and is `NOT_COMPARABLE`
when a first attempt cannot be proven.

## Statistics (`forward_stats.py`)

- **Resampling:** whole UTC days with replacement, B = 2000 by default, with a fixed
  seed recorded in the output.
- **Clusters:** source, direction, shared-exposure group, and a 60-minute window
  measured from each cluster's first member. Raw N, cluster N and day N are
  printed. The primary interval and p-value use cluster means.
- **p-values:** a two-sided percentile-bootstrap sign test against zero, bounded
  below by 1/(B+1). **Holm** adjusts across every comparison in the section,
  and comparisons without a p-value count as p = 1. The total is printed.
- **Score:** Spearman rank correlation of frozen Setup Score with net R, per
  source and provenance, with a day-resampled interval.
- **Floor:** below `MIN_COHORT = 30` complete observations a row says
  `INSUFFICIENT SAMPLE` and prints no interval, p-value or MDE.

### Sample-size arithmetic

The minimum detectable paired difference at two-sided α = 0.05 and 80% power is

```text
MDE = (z0.975 + z0.80) · σ / √n_clusters ≈ 2.80 · σ / √n_clusters
```

| σ of cluster-mean differences | n = 30 | n = 100 | n = 400 |
|---:|---:|---:|---:|
| 0.5 R | 0.26 R | 0.14 R | 0.07 R |
| 1.0 R | 0.51 R | 0.28 R | 0.14 R |

Detecting a 0.1 R difference at σ = 1 R needs about (2.80/0.1)² ≈ 784
independent clusters, not alerts. Correlated alerts add little information.

## Pre-registration

```sh
docker compose exec desk bybit-flow policy-preregister prereg.json
```

The command requires `hypothesis`, `metric`, `comparison`, `acceptance_threshold`,
`evaluation_window` (`start_ms` < `end_ms`) and a non-empty `attempted_variants`
list. The record is canonical JSON, hashed with SHA-256, stored with `created_ms`
and protected by update/delete triggers. `policy-review` records may cite
`prereg_id`. A review without one, or whose `forward_start` precedes the
pre-registration, is flagged `NOT_PREREGISTERED`. Neither status grants
production authority.

## Bounds and operation

New work runs in the existing forward-research admission class. Delayed and
trade-through stages enroll in the research loop **before** any path advance,
so a parent cannot finish first. Each parent is decided once. A cap leaves an
explicit `NOT_ENROLLED` marker (`HOURLY_CAP`, `STORAGE_BOUND`, `PRE_EPOCH`,
`TICK_UNKNOWN`) instead of a later, outcome-aware retry. The defaults are
conservative: `FLOW_FORWARD_POWER_MAX_PER_HOUR=60`, `FLOW_FORWARD_CONTROL_MAX_PER_HOUR=20`
and `FLOW_FORWARD_POWER_MAX_ROWS=200000` per table. Shadow failures increment
`evidence_power_errors` and cannot roll back a production save.

Migration **012** (`evidence-power-research`) is append-only. It adds
`evidence_power_paths`, `structure_controls`, `candidate_attempts` and
`policy_preregistrations` with immutability triggers. No earlier migration,
receipt, plan, label or score is rewritten or backfilled. A new epoch,
`forward_evidence_power_epoch_v1`, starts at the first startup with this release.

## Limits

OHLC paths are lower bounds, not fills. Day resampling assumes independence
across UTC days, which regime persistence can violate. Clusters cannot remove
dependence across groups. Small cohorts stay insufficient. Holm controls the
family-wise error only over the comparisons printed. Pre-registration records
intent; it cannot prove that no other analyses were run. Nothing here establishes
an edge or authorizes a production change.
