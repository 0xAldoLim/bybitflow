# Release notes

## 2.2.1 — Reliability & Evidence Loop

- Prospective, source/provenance-separated policy observations, fixed ablations,
  guarded score/cohort reports and manual research review records.
- Six scanner responsibility services with compatibility delegates and algorithm
  equivalence checks; production policy formulas and lifecycle remain unchanged.
- Ordered migration receipts, SQLite/loop pressure measurements, bounded local
  time series, passive WAL checkpoint diagnostics and storage growth forecasts.
- Background-only workload admission, common recovery status and a decision-first
  dashboard with immutable plans separated from current health.
- Deployment separated from ML maintenance; current-volume image rollback,
  redacted provenance, isolated Docker backup/restore CI and security checks.

Source release only; deployment is a separate operator action. No profitability,
new trading authority, ML maturity reduction, signal quota or deletion policy is
introduced. See [operating details](RELIABILITY_EVIDENCE_LOOP.md).


Implementation notes by release. These describe source behavior,
not the health or deployed version of any particular installation. Use
`docker compose exec desk bybit-flow version` for version metadata and
runtime diagnostics for current health.

## 2.2.0 — setup quality and trade construction

New opportunities use `trade-validity-v22` and `bybitflow-v22-setup-1`.
One Setup Score combines Evidence and Tradeability, with a bottleneck and
confidence cap. Bounded quote-event OFI, price-response assessment, historical
horizon movement capacity, execution targets and stop viability expose why a
trade may work and what limits it. Mandatory validity and configured grade
options remain separate; no daily quota or top-N delivery restriction is added.

Initial Discord delivery rechecks the executable quote and freezes the exact
assessment. Current deterioration is separate from the published score.
Old v21/v21.1 setups continue under their original semantics and lifecycle.
ML stays `candidate-v20` with unchanged source-specific 500-outcome/sequence
minimums and authority. Fixed forward exit research remains outside primary ML.
These are research-grounded assumptions, not validated profitability.
See [v2.2 definitions and limitations](V22_SETUP_QUALITY.md).
The [verification record](V22_VERIFICATION.md) reports an unfavorable partial
selection replay and missing historical inputs; this release makes no measured
profitability claim.

## 2.1.x — evidence, entry quality and research measurements

New setups use `candidate-v20` and the `bybitflow-v21-evidence-1` score profile:
location, confirmed mechanism, after-cost trade offer, and an evidence-confidence
cap. This is a versioned research rubric, not a measured improvement in returns.
Existing setups keep their original plans, scores, deadlines, source, and lifecycle.
Historical learning records and models remain available under their stored schema.
See the [score guide](SCORING.md) and [v1 comparison](v2/BYBITFLOW_V1_TO_V2_GAP_ANALYSIS.md).

Version 2.1 introduced frozen pre-trigger references, two-close breakout acceptance,
range reclaim checks, persistent context for pullbacks, executable entry-quality
checks and relative liquidity stress with recovery hysteresis. Target-path
friction uses observed volume concentration instead of simply counting levels.
The score weights remain 30% location, 45% mechanism and 25% trade offer.
Existing v2 plans retain their original `bybitflow-v2-evidence-1` interpretation.
Rejected new-policy candidates remain in research storage for hypothetical
outcome comparisons; they do not produce Discord setup cards.

Version 2.1.1 assigns `trade-validity-v21.1` only to new opportunities. Entry
checks distinguish chasing, adverse zone breaks, crossed stops/targets, expired
windows, stale quotes, unknown/invalid costs and deteriorated net reward:risk.
References freeze before the event; range reclaims may complete within three
setup bars, and accepted breakouts may retest within four. These are predeclared
research hypotheses whose forward effectiveness is unknown. Existing v21 plans
keep their original validity semantics and lifecycle.

Version 2.1.2 adds prospective research measurements: the first fresh executable
ask for a LONG or bid for a SHORT is frozen once, then compared with later
execution estimates under the existing cost model. Relative volume compares
completed bars with previous comparable UTC sessions, including 15-minute
setups. A bounded journal observes duplicate and potentially distinct later
structural events without generating another plan or Discord card. Scores,
strategy thresholds and `trade-validity-v21.1` production rules remain unchanged.
These measurements are **UNPROVEN / FORWARD TESTING**.

Recording cleanup verifies archive health, protects actual active/unresolved
consumers and leases, and can replace mixed packs with verified retained members.
Permanent snapshots, labels, exported datasets, models and Discord receipts are
preserved. Canonical per-source training counts, sequence counts and eligible
identity digests are checked before and after cleanup. Safely releasing raw tape
after a final label does not remove the derived training candidate.
Cleanup freezes both causal time and the existing snapshot/label insertion
boundaries, so historical labels arriving during cleanup do not trigger a false
data-loss alarm. Missing original records still stop cleanup. The recorder waits
for independent maintenance at the storage limit and retries its pending batch;
archive verification does not run inside its bounded writer process.

Inspect or plan cleanup from the repository directory:

```bat
docker compose exec desk bybit-flow storage status
docker compose exec desk bybit-flow storage cleanup --dry-run --force
```

`storage cleanup --force` executes that eligibility policy even below the usual
85% pressure trigger; it never overrides active, unresolved, leased or unknown
protection. Automatic cleanup retains the existing recent-data floor and targets
65% usage when sufficient eligible raw data exists. Permanent records may keep
usage above that target. Archive health and compact tombstones explain deletions;
`ml/maintenance` contains pre/post eligibility and artifact verification reports.

Pullback ordinal, completed-bar trigger, ID/NR4 compression, comparable-session
volume and spread/depth stress variants are diagnostics, not additional score
weights. The joint liquidity production rule remains unchanged. Rejected
opportunities are counted uniquely in the audit, with separate sole/shared reason
groups and leave-one-filter-out diagnostics. Shared groups must not be added as
independent avoided losses. Cost stress and candle-close markouts are hypothetical
research measurements; unsupported execution or extrema remain unknown. See the
[hypothesis ledger](V211_HYPOTHESES.md).

Inspect delivered outcomes and filter counterfactuals with:

```bat
docker compose exec desk bybit-flow research trade-quality --days 30
docker compose exec desk bybit-flow research trade-quality --days 30 --source binance --horizon CORE_INTRADAY --json
```

The bounded audit separates prints and monitored OHLC outcomes, reports missing
measurements as unknown, and does not treat candle touches as verified account
fills. [The v2.1 audit](V21_RESEARCH_AUDIT.md) describes evidence, limits,
policy definitions and changes. These rules have no demonstrated post-change edge.

## Primary outcome recovery

Binance aggregate-trade recovery can reconstruct incomplete current-schema
primary outcomes under the original fill, participation and cost rules. It
preserves original decisions and labels, saves replay progress, and uses a
bounded official-archive cache. Recovered outcomes join the primary pool with
explicit provenance only after the complete trade sequence is verified.
See [ML recovery](ML_RESEARCH.md#primary-trade-history-recovery).
