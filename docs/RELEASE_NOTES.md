# Release notes

## 2.4.4 — storage cleanup that keeps up with recording

- Cleanup rebuilt its protection list for every segment, re-reading every
  unresolved decision each time. Measured on production it removed about four
  segments a minute, the same rate new ones are recorded, so the budget filled
  regardless. The list is now rebuilt at most every 30 seconds. Each segment is
  still re-classified and exclusively leased before removal.

## 2.4.3 — recorded-readings study, hypothesis tags, readable failures

- A study of every stored decision found no recorded order-flow, book or
  derivatives reading that predicts outcomes under a held-out test. See
  [recorded readings](audits/2026-11-28-recorded-readings.md).
- Three readings that lean consistently are recorded on each delivered alert as
  pre-registered tags and reported by `bybit-flow scorecard`. They never affect
  delivery.
- Scanner failure logs now carry the failure message, bounded to one line with
  credential-bearing URLs removed. Previously only the exception type was
  logged, which made outages undiagnosable from logs.

## 2.4.2 — stop delivering the wrong-sided rule

- Core intraday trend pullbacks are no longer sent to Discord
  (`FLOW_ALERT_BLOCKED_RULES`, default `["CORE_INTRADAY|trend_pullback"]`). The
  published rule reached +1R before −1R in 47.4%, 46.0% and 45.9% of setups on
  three independent symbol sets. Setups are still generated, tracked and shown
  on the dashboard; follow-ups for alerts already delivered are unaffected.
- Three further acceptance results are recorded: the swing trend pullback did
  not replicate on 48 fresh symbols, nothing was accepted on 40 mid-caps, and
  fading the intraday trend pullback with resting orders was rejected. 59 rules
  tried, none accepted. See [acceptance](ACCEPTANCE.md).
- The candle downloader backs off on exchange rate limits, since the live
  scanner shares the same address.

## 2.4.1 — second acceptance round

- Twenty-eight further rules, declared in code before they were run: four exits
  on swing continuation setups, resting-order entries for core intraday and
  swing, funding extremes, and cross-sectional 7-day momentum.
- 58 rules tried in total on 92,174 replayed setups. None is ACCEPTED. One
  passed the corrected significance threshold in the test period and reversed
  in the held-out quarter. See [acceptance](ACCEPTANCE.md).
- Acceptance variants may carry their own stop distance and cost, so entry and
  fee models are compared under one protocol.

## 2.4.0 — acceptance test and outcome-based grades

- `bybit-flow acceptance run` replays the production entry rules on public
  candles and writes a verdict per rule: ACCEPTED only with a significant
  directional edge and a positive, cost-adjusted plan in the test period, both
  confirmed on a held-out final quarter. `bybit-flow acceptance status` prints
  the current verdicts. See [acceptance](ACCEPTANCE.md).
- Every new alert states its rule's proven record and an outcome grade measured
  from replayed setups like it. Both are stored with the alert, and
  `bybit-flow scorecard` reports delivered results by outcome grade.
- `FLOW_ACCEPTANCE_GATE` is `report` by default: alerts are annotated, not
  blocked. `enforce` delivers only ACCEPTED rules. `FLOW_GRADE_SOURCE=outcome`
  makes `FLOW_MIN_ALERT_TIER` use the outcome grade instead of the Setup Score.
- No rule is ACCEPTED at this release. Enforcing the gate today would deliver
  nothing; that is the measured state, not a malfunction.

## 2.3.1 — database slimming and the scoring review

- `bybit-flow storage slim` removes permanent rows that nothing reads:
  generation-stage ML snapshots and origin copies of never-delivered, finished
  candidates. Decision snapshots, labels, models, signals and Discord receipts
  are untouched. `--dry-run` reports what would go.
- `bybit-flow storage rebuild --output PATH` writes a compact, verified copy of
  the database. Swapping it in is a manual step with the application stopped.
- Generation-stage snapshots are no longer written
  (`FLOW_ML_GENERATION_SNAPSHOTS=false`). They were about 80 KB per candidate
  and had no reader.
- A replay harness runs the production entry rules over a year of public
  candles. The [scoring review](audits/2026-11-27-scoring-review.md) reports the
  result and what to change.

## 2.3.0 — alerts no longer depend on recording; delivery follows the grade

Based on the [independent audit of 2026-11-27](audits/2026-11-27-independent-audit.md).

- **Raw recording is best effort.** A full storage budget, queue overflow or
  writer error drops the affected raw rows as explicit coverage gaps. Scanning,
  lifecycle tracking and Discord delivery continue. `FLOW_RECORDING_REQUIRED=true`
  restores the previous behaviour, where a recording failure stops the scanner.
- **An interrupted cleanup no longer blocks storage forever.** A run that lost
  its claim and has no usable pre-run audit is replaced by a fresh, fully audited
  run. A readable audit that shows changed frozen evidence still blocks.
- **Delivery follows the Setup Score grade.** `FLOW_MIN_ALERT_TIER` (default `C`)
  is the lowest grade sent to Discord. Previously every gate-passing setup was
  sent, including grades D, E and F. Follow-ups for already delivered setups are
  unaffected. Set `F` for the previous behaviour.
- **`bybit-flow scorecard`** reports targets, stops and expiries of delivered
  alerts by grade, family and horizon, with a break-even comparison.
- **`scripts/start.ps1` repairs the Docker Desktop stale-socket crash** that
  kept the stack offline after unclean shutdowns.
- Score weights, gates, stops, targets and model authority are unchanged. The
  audit found no measurable edge in delivered alerts; see its recommendations.

## 2.2.6 — verified raw recording retirement

- Bounded maintenance can retire raw recordings for finalized non-trainable
  outcomes or intervals with no committed consumer. Durable outcomes, active
  plans, and complete-label recordings remain protected.
- Retirement has its own pre- and post-run verification, tombstones, and failure
  state. Historical broad cleanup failures remain in the original audit.

## 2.2.5 — storage outlook

- The dashboard and doctor show cached permanent and raw storage categories,
  retention eligibility, and estimated time to budget thresholds.
- Permanent-data pressure warns before recording stalls. A failed cleanup audit
  remains visible when the recorder has space and continues normally.

## 2.2.4 — storage reliability

- A failed retention run can be retried after its frozen SQLite rows, eligible
  identities and immutable ML artifacts have been verified. The failed run stays
  in the audit history. Legacy audits without frozen row fingerprints require
  independent evidence before cleanup can resume.
- The dashboard groups the effects of recording storage pressure into one
  incident. Discovery waits for recording capacity and retries automatically.
- The application continues to observe the configured storage budget and existing
  evidence protections. Trading and model authority policies are unchanged.

## Operational readiness repair

- Capacity upkeep can audit checksum-damaged recordings as explicit gaps so one
  bad segment cannot indefinitely block later evidence processing. Strict replay
  still rejects checksum failures; affected evidence is never treated as complete.
- ML readiness stages distinguish collection, training, compatibility and
  production validation. Setup details show evidence coverage without rescoring.
- Maintenance failures surface directly on the dashboard.
- Optional existing-container login startup, isolated fill-journal review and
  SQLite backup-copy integrity verification. No additional collection pipeline.

See [operating readiness](OPERATING_READINESS.md). Strategy, scoring, Discord
qualification and model promotion rules remain unchanged.

## 2.2.3 — evidence power (research only)

- Delayed-entry stages `delay60`, `delay180` and `delay600` after the durable
  sent-initial timestamp, paired with the delivery stage on every exit variant.
  Missed entries (`MISSED_INVALIDATED`, `MISSED_TARGET_PASSED`, `MISSED_EXPIRED`)
  are kept as explicit zero exposure.
- A `structure_only_control` for v2.2 candidates that failed only flow
  confirmation, enrolled once per identity at its structural decision time.
  Confirmed opportunities record structural time, planned price and entry cost.
- `forward_stats`: whole-UTC-day resampling with a recorded seed, clustered
  effective N, Holm-adjusted paired differences with a printed comparison
  count, Score/net-R Spearman, conservative and optimistic ambiguity bounds,
  1.5×/2× cost stress from stored gross R, and minimum detectable differences.
- A trade-through conservative fill bound alongside touch. Stop-exit slippage
  is labeled a fixed assumption, and a decision-time depth diagnostic is research only.
- Causal `attempt_ordinal`/`pullback_ordinal` as forward-report cohorts, and a
  `first_attempt_only` shadow variant.
- `bybit-flow policy-preregister` stores immutable, hashed pre-registrations.
  `policy-review` flags reviews `NOT_PREREGISTERED`.

Research only, with no profitability claim and no production change: score,
gates, delivery, Discord content, lifecycle, risk/costs, ML schema
`candidate-v20`, 500/500 minimums and authority are unchanged. A fixed fixture is
byte-identical to 2.2.2. Append-only migration 012 is bounded by hourly caps and
row limits under forward-research admission. See [evidence power](EVIDENCE_POWER.md).

## 2.2.2 — staged exhaustion and reclaim timing

- New intraday and swing range-rejection/liquidity-sweep candidates can confirm
  through a non-overlapping pressure/decay/trigger sequence. The existing
  confirmation route remains available in the same scanner.
- Reuses retained tape, defended footprint calculations and bounded quote-event
  OFI. Resets, incomplete observations, untrusted churn and continuing adverse
  liquidation cascades cannot establish the new sequence.
- Mechanism scoring uses the actual confirmed trigger without adding duplicate
  delta/CVD votes. Raw context, baseline results and policy version are retained.
- Deferred initial delivery rechecks the sequence without changing the original
  decision snapshot. Existing active plans and lifecycle monitoring are preserved.

No new service, ML training, compatibility relaxation, migration or data rewrite
is introduced. Execution costs, risk, liquidity, macro/factor and V8 gates remain
in force. See [definitions and validation limits](REVERSAL_TIMING.md).

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
