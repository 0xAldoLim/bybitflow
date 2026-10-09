# Evidence scores and qualification

Quality is a deterministic evidence score, not a calibrated win probability or
a daily percentile rank. The implemented `scoring.tier` bands are:

| Grade | Score |
|---|---|
| SSS | 95–100 |
| SS | 90 to below 95 |
| S | 85 to below 90 |
| A | 75 to below 85 |
| B | 65 to below 75 |
| C | 50 to below 65 |
| D | 35 to below 50 |
| E | 20 to below 35 |
| F | Below 20 |

With `FLOW_RESEARCH_ALERTS=true`, confirmed research setups across these bands
can be delivered. `FLOW_SSS_RESEARCH=true` alone is the narrower SSS-only opt-in.
Mandatory structure, liquidity, coverage, freshness, entry, risk, and applicable
production gates still apply. A rejected setup is not made eligible by its score;
the scorer marks its raw tier F and final qualification REJECTED.

## Version 2 rubric

New `candidate-v20` plans use `bybitflow-v2-evidence-1`:

| Dimension | Weighted contribution | Evidence |
|---|---:|---|
| Location | 30% | Original structural reference, family-specific range geometry, covered volume-value location |
| Mechanism | 45% | Trusted executed aggression and actual price response, or defended absorption for reversal families; explicit independent-venue substitution |
| Trade offer | 25% | Accepted after-cost RR, costs relative to stop distance, stop/noise geometry, and distinct POC/HVN/AVWAP barriers to original TP1 |

```text
weighted = .30 × location + .45 × mechanism + .25 × trade_offer
raw = .80 × weighted + .20 × min(location, mechanism, trade_offer)
quality = min(raw, evidence_confidence)
```

Dimensions and confidence are on a 0–100 scale. The weakest dimension limits
quality; favorable context cannot erase an unsupported mechanism. Confidence
requires original-venue coverage, valid causal structure, accepted risk, and flow
confirmation. Optional profile and closed-event readiness can support confidence;
missing evidence cannot add points or redistribute weights. Size and event
direction share one modifier because both describe the same prints. Unknown or
opposing observations receive no favorable modifier.

The existing scanner families, beta/residual context, assessed asset facts,
funding/OI observations, execution costs, and V7/V8 gates remain in use. Their
old flat category points are not a second v2 score. Large prints do not identify
participants, and sampled liquidation acceleration alone never establishes a fade.
All coefficients are implementation inferences; predictive advantage is unvalidated.
See the [research ledger](v2/INSILICO_RESEARCH_LEDGER.md).

Pre-cutover active plans retain every original price level, score, tier, deadline,
and stored rubric through their original lifecycle. Historical scores remain
comparable only within their stored score profile.

## Historical native evidence rubric

The v1 seven category weights total 100. Each fraction is clipped
to its category bounds; missing evidence earns no invented points or redistribution.

| Category | Maximum | Implemented evidence |
|---|---:|---|
| Regime | 15 | Closed context-bar directional efficiency relative to 0.5 |
| Structure | 20 | Family structure and stop distance relative to setup ATR, refined by range location and available auction evidence |
| Executed flow | 25 | Family-specific continuation or reversal evidence, with versioned flow trust, persistence, and short-intraday book refinements |
| Derivatives | 10 | Available OI/funding, directional crowding, and versioned freshness, basis, and longer-horizon funding adjustments |
| Execution/risk | 15 | Accepted cost-adjusted reward/risk relative to 2.5R, refined by measured stop/noise geometry |
| Fundamentals | 10 | Current sourced quality/risk assessments for new hardened plans; coverage remains separate |
| Cross-market | 5 | Directional beta-adjusted residual evidence and factor stability for hardened plans, with capped fallback when unavailable |

Continuation flow starts from the weaker of directional stacked imbalance and
delta strength. Reversal flow can use defended opposing notional and observed
absorption. Plans with `flow-score-v2` can instead use a passed, supportive
directional executed-flow pathway. These are alternative evidence paths, not
additive duplicate conviction. Flow-quality versions reduce untrusted local
credit; configured cross-venue substitution uses independent trusted flow and
price response rather than adding another category.

Short intraday places more weight within the flow category on sustained book/flow
timing; longer horizons account for accumulated funding. Sweep, BOS, wick, FVG,
and trapped-participant descriptions from the same event are not independent
votes. A negative funding rate is not evidence of a profitable long.

Current fundamental scoring separates source coverage from an explicit point-in-time
quality/risk assessment. Unassessed facts supply no favorable quality, and severe
adverse evidence reduces credit. Known major events can reject a setup independently
of score. Historical `native-evidence-2` plans retain their coverage-based rubric;
`native-evidence-3` introduced assessed fundamentals/residual factors, and
`native-evidence-4` adds the alternative confirmed directional flow basis. The
stored plan version and `score_profile` identify which rules were used. Existing
plans and historical scores are not rewritten.

## Separate scores and model authority

Opportunity priority ranks discovery candidates and is separate from quality.
V8 production-gate readiness/rejections are also separate from the quality score;
a supportive feature cannot override a mandatory rejection.

ML can rank compatible research candidates and explain its output. Its ranking
does not replace quality, grade, entry, stop, or targets. With
`FLOW_ML_FILTER_RESEARCH=false`, abstention does not block otherwise eligible
alerts. Bootstrap OHLC-proxy models can never filter delivery or become champions.
Validated probability requires registry admission and independent cohort evidence;
current assumed-cost labels cannot satisfy verified-cost promotion checks.
There is no automatic strategy, score-weight, or model promotion.

Historical `tv-evidence-1` chart attestations remain audit records under their own
rubric. TradingView ingestion and new chart labels are retired; those scores are
not interchangeable with native scores or pooled into current calibration.

Inspect a setup's `score_components`, `score_reasons`, and `score_profile` to see
the earned evidence. High-quality claims and empirical calibration need independent
real outcomes; software tests do not establish predictive edge. See
[ML research](ML_RESEARCH.md) and the [operator guide](USER_RUNBOOK.md).
