# v2.2 implementation and verification record

Status: **V22_IMPLEMENTED_WITH_WARNINGS**. Source implementation is complete;
the fixed retrospective selection replay does **not** establish improved economic
performance. Full target-construction performance is unknown because the export
does not contain the required predecision candles, quote-event buckets or profile
volume bins. No rule was retuned after viewing this result.

Starting main: `47e5dad0c73196b5a63996c9fad3c68a9e7ae34e`.
The ending revision is the commit containing this report. Version: 2.2.0.
No live deployment or service restart was performed.

## Implementation

The new-candidate policy is `trade-validity-v22`, with
`bybitflow-v22-setup-1` and explicit component policies:

- `event-ofi-v1`: manually verified touch queue-event arithmetic, distinct
  5/10/25 bps changes, depth normalization and 1/5/15/30/60 second windows.
  Bounded second buckets exclude resets/gaps/epoch discontinuities, including
  the first aggregate after a gap exactly on a second boundary.
- `impact-efficiency-v1`: one executed-aggression concept, evaluated against
  displacement, persistence, OFI, depth and replenishment. Reversals can use
  defended absorption with structural response; alternative pathways use their
  maximum rather than additive correlated votes.
- `horizon-movement-capacity-v1` and `target-reachability-v1`: completed
  prior pseudo-start windows, each with its own known ATR, bounded history and
  minimum samples; explicit REACHABLE/STRETCH/LOW_REACHABILITY/UNKNOWN states.
- `execution-target-v1`: preserve the structural objective and select a causal
  viable first target with configured after-cost RR and valid target ordering.
- `stop-viability-v1`: assess existing stop/noise, wick, ATR and economic
  geometry. Stops, account sizing and leverage are not widened or changed.
- `setup-score-v22`: one public Setup Score with supporting diagnostics,
  confidence limits, strengths and weaknesses.
- `delivery-execution-v22`: current original-venue executable bid/ask,
  renewed confirmation, costs, RR, spread/depth/impact and immutable delivery
  snapshot. Staleness is checked again after durable accounting.
- `setup-ranking-v22` and `setup-health-v22`: all valid independent
  opportunities remain deliverable; shared exposure is informational. Current
  deterioration is separate from the frozen published plan and score.
- `setup-exit-research-v22`: separate first-decision and actually delivered
  stages; fixed exit variants, milestones and cautious ordering/coverage labels.

The score is:

```text
E = .30 Location + .25 Structure + .30 Mechanism + .15 Context
T = .30 FollowThrough + .30 TargetReachability + .20 StopGeometry + .20 Execution
base = .55 E + .45 T
raw = .75 base + .25 min(E, T)
SetupScore = round(min(raw, EvidenceConfidenceCap), 1)
```

These are **RESEARCH-GROUNDED ASSUMPTIONS**, not calibrated win probabilities.
See [the policy specification](V22_SETUP_QUALITY.md) for constants and limitations.

Discord has one score/grade, executable entry, stop, execution/structural target,
remaining RR, flow/target/stop/execution context, strengths, limits, timing and
invalidation. Dashboard detail exposes the frozen snapshot and separate research.

## Preserved behavior

| Requirement | Result |
|---|---|
| Daily quota, top-N cap or artificial setup maximum introduced | **NO** |
| Historical v21/v21.1 scores, plans, gates or evidence rewritten | **NO** |
| Existing active lifecycle replaced, paused or terminated by clustering | **NO** |
| ML schema changed | **NO**, remains candidate-v20 |
| ML minimums changed | **NO**, 500 complete primary outcomes/source and 500 sequences/source |
| Venue isolation, chronology, purge/embargo, holdout or abstention weakened | **NO** |
| Retraining, promotion or automatic retuning performed | **NO** |
| Recording/storage policy changed or historical evidence deleted | **NO** |
| Live recorder interrupted or deployment changed | **NO** |

Published lifecycle updates still require that exact setup's durable sent initial
and message ID. True duplicates retain independent internal monitoring. No
unseen duplicate child can generate an orphan lifecycle notice. Existing
historical fingerprints remain unchanged.

## Tests and checks

Focused tests cover manual OFI sequences and continuity; successful versus
ineffective aggression and defended reversal absorption; causal capacity and
target/stop geometry; current delivery price/RR/spread/depth/freshness; twelve
independent correlated setups all delivered; old/new true duplicates; immutable
published plans; native flow calculation without cross-thread SQLite access;
deduplicated deterioration; provisional confirmation recovery; separate
post-acknowledgement research; ambiguous entry/exit candles; the existing SSS-only
opt-in after final scoring; unchanged v21.1 generation geometry; and identical
existing candle markouts/primary proxy semantics for new-policy plans.

Normal verification: `python scripts/verify.py`, including Ruff, Python
formatting, JavaScript syntax, diff whitespace and the complete pytest suite.
Final local result: **863 passed, 4 skipped**. All four skips are existing
optional Torch tests because Torch is not installed. Three existing dependency
deprecation warnings remain. Ruff, formatting, JavaScript syntax and whitespace
checks passed. No CI or deployment result is implied by local verification.

## Fixed retrospective comparison

Input export as of 2026-10-23; candidate-v20 public one-minute OHLC proxy labels,
with assumed costs and no verified account fills. Feature scoring ran before
outcome labels were joined. Inputs and component source files are hashed in the
private JSON output. No thresholds or weights were fitted to the result.

Reproduce with the existing fixed export:

```bat
python examples/compare_v22.py --archive-dir "<loss-review-export-folder>" --output-dir "<private-output-folder>"
```

Private exports and row-level outputs are excluded from the repository.

The full v21.1 reference remains **52 outcomes: 27 losses, 25 positives,
-2.144493R**. It is not directly comparable to a smaller selected cohort.

Of the 91 published current-schema proxy outcomes, 49 have all critical frozen
feature timestamps available by the initial acknowledgement and complete entry
coverage proof. The other 42 cannot be replayed under that rule: 39 older-policy
records lack required execution diagnostics, and three newer-policy records have
missing or post-delivery critical timestamps. Unknown records are not losses.

### Selection-only matched newer-policy observations

| Measure | Original matched proxy cohort | Retained by fixed v2.2 checks |
|---|---:|---:|
| Setups | 49 | 44 |
| Losses | 24 | 23 |
| Positive outcomes | 25 | 21 |
| Sum of original proxy net R | +1.139345R | -2.475129R |
| Mean original proxy net R | +0.023252R | -0.056253R |
| Stops | 17 (34.69%) | 17 (38.64%) |
| Time exits | 30 (61.22%) | 25 (56.82%) |
| Mean MFE lower bound | 0.987755R (49) | 0.970920R (44) |
| Mean adverse-excursion magnitude lower bound | 0.639140R (49) | 0.665417R (44) |

One losing label and four positive labels were excluded. The difference in
retained original proxy R is **-3.614474R**. Reasons: two stale quotes, two stops
beyond structural width, one entry impact above its cost allowance. These
protective rules were not relaxed to improve the retrospective result.

Twenty-four of these 49 proxy entries precede the card acknowledgement. Therefore
they cannot support a customer-entry performance claim.

### Post-card proxy-entry subset

| Measure | Original matched subset | Retained by fixed v2.2 checks |
|---|---:|---:|
| Setups | 25 | 24 |
| Losses / positives | 14 / 11 | 14 / 10 |
| Sum of original proxy net R | -1.804860R | -1.842744R |
| Mean original proxy net R | -0.072194R | -0.076781R |
| Stops | 9 (36.00%) | 9 (37.50%) |
| Time exits | 15 (60.00%) | 14 (58.33%) |
| Mean MFE lower bound | 0.866892R (25) | 0.887839R (24) |
| Mean adverse-excursion magnitude lower bound | 0.693151R (25) | 0.704932R (24) |

Zero losing labels and one positive label were excluded; retained original proxy
R changes by **-0.037884R**. This is a selection-only association. It does not
resimulate the new observed executable entry, retargeting or actual fills.
Actual losses avoided and causal P&L improvement remain **UNKNOWN**.

### Score and reachability diagnostics

In the 49 matched records, median old scores were 56.45 for losses and 55.30 for
positives; new partial-evidence scores are 48.95 and 49.70 respectively. Missing
evidence earns no supporting credit and remains explicitly unknown.

In the post-card subset the new medians are 48.75 for losses and 49.70 for
positives. Fixed score bands do **not** show economic monotonicity: the five
20–40 observations average +0.308893R, while the twenty 40–60 observations average
-0.167466R. There are no observations above 60 in this incomplete replay.
No higher score cutoff was introduced.

Target reachability and weighted path are **UNKNOWN in all 49** records because
the required underlying histories/bins were omitted from the fixed export. New
multi-window OFI is unavailable in all 49. Stop classifications are 41 healthy,
four tight-but-valid, two noise-conflicted and two structurally too wide.

Forward research will record full v2.2 evidence and fixed ablations prospectively.
There is no forward v2.2 deployment sample from this source-only task. This
retrospective result does **not** validate improved profitability.

## Change map

New logic: `impact_efficiency.py`, `target_reachability.py`,
`stop_viability.py`, `setup_common.py`, `quality_v22.py`,
`setup_scoring.py`, `setup_runtime.py`, `setup_freeze.py`,
`setup_ranking.py`, `setup_presentation.py`, `setup_research.py`.

Integration: scanner, strategy, horizons, risk, quality dispatch, existing OFI
buckets, identity, notifications, storage, V8 runtime/gates, API, dashboard and
source version. The existing optional `ml/monitored.py` candle markouts also
apply to new v2.2 plans without changing primary label calculations.
Existing Decimal sharing, Tape ID indexing and bounded liquidity
frames remain intact.

Tests: `test_setup_v22.py`, `test_v22_ofi.py`,
`test_v22_delivery.py` and `test_v22_research.py`.
Replay: `examples/compare_v22.py`.
Documentation: README, release notes, scoring, status, the policy specification
and this verification record.
