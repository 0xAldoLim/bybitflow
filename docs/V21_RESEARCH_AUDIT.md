# v2.1 / v2.1.1 / v2.1.2 research and implementation audit

## v2.1.2 measurement and retention policy

New opportunities carry `research-observations-v212`; old decisions are not
retroactively annotated. `initial-executable-v1` freezes the first fresh same-venue
book quote and existing modeled cost reserves, using ask for LONG and bid for
SHORT. Spread embedded in that price is not charged again. Later
`execution-deterioration-v1` observations record gross/net RR change, price move
in known ATR units, spread/cost change and elapsed time. Missing inputs remain
unknown. These observations add research cohorts, not gates or score weights.

`comparable-session-volume-v1` uses prior completed same-source, symbol,
timeframe and UTC bucket observations: minimum 3, target 10, maximum 20 samples
over at most 30 calendar days. Daily bars use earlier completed days. Available
cached candles are reused; an optional longer fetch uses the existing candle
adapter and REST limiter, bounded to 1,000 bars per market/timeframe every six hours.
That fetch never replaces the production candle cache. Auxiliary
stored reference keys are bounded to 128. Snapshot evidence remains immutable
even when auxiliary history is evicted. Relative volume never rejects a setup.

`structural-suppression-v1` compares frozen references and actual sequence
identities. The existing event search and production deduplication remain
authoritative. A bounded diagnostic search may observe a distinct later sequence
and record `POTENTIAL_DISTINCT_EVENT`; it does not create a plan or notification.
The journal retains at most 1,000 observations and aggregates repeated identities.

The earlier broad-scan exception count came from market selection/admission,
where exceptions were counted without persisted details. New scans record
operation, market, timeframe/range, exception type, bounded redacted message,
cause and traceback fingerprint. Forensic decode/chain failures use the same
durable mechanism. Internal errors are `AUDIT_SOFTWARE_FAULT`, not inferred
market-data gaps. Old discarded exception details cannot be reconstructed.

`archive_health` is additive durable metadata. Only proven corruption can justify
corrupt retirement; permission and unknown software failures fail closed.
Cleanup protects exact active/unresolved intervals, recent tape, writers and
leases. Replacements verify retained byte hashes/manifests, fsync and close,
atomically remap retained members and write tombstones/deletion intent, then
remove the old archive. A crash on either side of the mapping commit preserves
a readable version. Logical `through_ms` is unchanged by non-prefix pruning.

A renewable ten-minute run claim serializes scheduled and manual destructive
cleanup before eligibility capture. Expired or replaced ownership stops further
deletion. Each run persists its own result report alongside pre/post inventory,
so concurrent triggers cannot split freed-byte counters or verification state.
Live broad selection identified three listings with fewer than the existing
60 completed candles required by horizon features. They remain rejected with
`CANDLE_HISTORY_INSUFFICIENT`; the history requirement is unchanged.

Cleanup validates canonical per-source trainability and sequence readiness,
eligible identity digests, registered model hashes and existing dataset/model
file hashes. A mismatch stops further deletion and persists a failed check.
See [hypotheses](V211_HYPOTHESES.md) and [ML retention](ML_RESEARCH.md).

## v2.1.1 maintenance policy (12 October 2026)

Starting HEAD: `817744efb5022a2188797b4774ff7c50ecc890c0`. The remaining
v2.1 observations below are historical baseline evidence, not v2.1.1 results.
New opportunities use `trade-validity-v21.1` and `entry-quality-v2`; old v21
semantics live unchanged in `quality_v21.py`. Candidate-v20, score weights,
source-specific model requirements, causal partitions/embargo, lifecycle and
initial Discord visibility rules are retained. The deployment must preserve
original plans and snapshot bytes, rather than migrate them into the new policy.

Production qualification now separates directional zone relation, trigger/plan
expiry, original/executable geometry, known/unknown/invalid costs, net RR and
existing quote freshness. Tick comparisons use Decimal instrument ticks with
ROUND_HALF_UP; no ATR entry tolerance is introduced. Missing optional quote
metadata is not a new veto. Stop/target crossings are geometry failures and do
not become chase failures. Frozen costs and current execution estimates remain
separate. See [cost accounting](SCORING.md#separate-scores-and-model-authority).

Range references use 20 completed horizon setup bars before the first excursion.
A reclaim can complete on that bar or within three following completed setup
bars. Two consecutive outside closes before reclaim produce
`RANGE_ACCEPTANCE_OUTSIDE`, separately from `FAILED_RECLAIM`. Breakouts freeze
the reference before the first acceptance close, require two outside closes and
a held retest within four subsequent completed setup bars. The first held/failed
retest ends that event; later retests cannot create another candidate from it.
The structural ID is based on event start and frozen reference, not scan minute
or policy name. The existing active-event guard also protects upgrades from
duplicate cards.

`ACCEPTED_NO_RETEST` at expiry is stored as a rejected research plan, never an
automatic chasing entry. Its outcome measures the hypothetical offer at research
decision time, not a retrospectively invented entry at first acceptance. Other
production gates still apply. Trend qualification uses the existing classifier
on at most 20 causal context prefixes, requiring two consecutive trend readings;
touches are counted in at most 60 setup bars after that current continuous
qualification. A left-censored trend is explicitly bounded, not an unbounded
first-pullback claim. Later pullbacks are not banned. The previous-bar completed
break is diagnostic only.

ID_NR4 requires both an inside completed setup bar and the smallest range among
four; INSIDE_ONLY and NR4_ONLY remain distinct, with expansion recorded separately.
Relative volume uses only available past same-UTC-slot setup bars (at most 80,
minimum three). It is unknown without comparable history. CORE_INTRADAY alone
records the optional previous-24-hour, next-six-hour frozen-range interpretation
from already cached 1H bars. This adds metadata, not another scanner or card.

The joint liquidity production rule and its three-bad/two-good persistence are
unchanged. Spread-only, depth-only, joint and either variants are descriptive
labels on the same observation/outcome, with unknown maturation and gap-reset
metadata. Existing flow quality supplies aggression magnitude, signed notional,
displacement, absorption and efficiency; there is no second flow engine.

Audit output includes `research_cohorts`, `filter_counterfactuals`,
`failure_taxonomy` and `execution_stress`, separately for prints and monitored
OHLC labels. Entry dimensions, policy, reclaim/retest delay, no-retest state,
pullback ordinal, compression, reference efficiency/width and liquidity variants
are grouped across accepted/rejected decisions. Canonical rejected opportunities
are counted once in `unique`; reason groups separately report containing, sole
and shared outcomes. Shared avoided losers/missed winners overlap and must not
be summed. Leave-one-filter-out counts only sole recorded reasons as uniquely
changing a decision. It is not verified alternate execution.

Each failure category is identified as actively detectable, research-only,
obsolete for v21.1, or not yet implemented. Coarse legacy CHASED_ENTRY does not
prove chasing. Unimplemented retrospective causes are explicitly labeled rather
than interpreted as zero failures. Cumulative original-R drawdown is additive
research P&L, not equity drawdown.

New-policy monitored workers reuse their already-fetched candles for +1 and +3
execution-bar and +1 setup-bar planned/entry markouts. Values are candle-close
proxies, not future midprices, executable fills or account fills. Missing
checkpoints remain unknown. Enclosed post-entry full bars provide observed
MFE/MAE lower bounds; entry/exit-bar ordering is unknown, so full OHLC extrema
remain null. Prints retain their observed MFE/MAE in original planned R.
Cost stress is `net R − (multiplier − 1) × frozen cost / original risk` for
1.0/1.5/2.0, with no added spread or optimistic delayed-fill reconstruction.
Delayed execution is UNKNOWN. Historical labels are not rewritten.

All changes are predeclared experiments; there are no completed v21.1 forward
outcomes before deployment and no claimed improvement in profitability. See
[H1–H8](V211_HYPOTHESES.md). Operational observation demonstrates health only.

Initial implementation verification: `scripts/verify.py` exited 0 with 606 tests passed and
four optional skips, plus lint, format, JavaScript syntax and diff checks. The
first full run exposed a legacy candidate-history guard regression; it was
restricted to v21.1 and the targeted legacy/new-policy tests passed before the
successful complete recheck. The final focused integration group passed 115
tests; the later legacy/new-policy group passed 46. A 50-run, 120-bar isolated
benchmark measured median 9.14 ms for v21 and 28.68 ms for v21.1. This bounded
local computation adds no REST calls; it is not a live throughput or edge claim.

Live observation then exposed a current-quote timestamp mismatch: book receipts
arriving during asynchronous flow work could be later than its original check
time and incorrectly appear stale. V21.1 now measures executable quote freshness
at the current execution-check time, separately from the frozen completed flow
window. The existing freshness limit is unchanged. A regression covers a receipt
after the worker's original timestamp and confirms old v21 dispatch keeps its
stored semantics. This follow-up repair receives fresh full verification and
exact-commit CI before deployment; historical decisions are not relabeled.
The follow-up's local full verification exited 0: 608 passed, four optional skips;
72 targeted execution-clock, legacy-policy and scanner-recovery checks passed.

Baseline: `c8a480eefb40fb056286306cb033b034d65b6ffc`, inspected 11 October 2026.
The operator supplied the synthesis directly. Its source-page references are
reported provenance, not a claim this repair reread the underlying library.
Illustrative 24H/6H ranges and optimized trading parameters are not adopted.

## Baseline evidence

The read-only runtime export contained 237 candidate-v20 decisions, 44 sent
initial cards and 188 technical duplicate exclusions. Exactly one prints-v1
outcome was complete: a loss of -1.293R. There were 36 complete monitored OHLC
outcomes across all decisions, of which 35 belonged to delivered cards with a
message ID. The latter had 11 positive and 24 negative returns, mean -0.466R,
median -0.565R and profit factor 0.284 (sum positive net R / absolute sum negative
net R). There were 15 stops, 20 time exits and no targets in that delivered
cohort. Costs averaged 24.55% of original risk; the largest losing streak was
eight. These are hypothetical public-data outcomes with assumed costs, not
the operator's account fills. Only two calendar days are represented.

SHORT_INTRADAY, CORE_INTRADAY, SWING and EXTENDED_SWING are reported separately
by the audit command, including empty horizons. Swing had three delivered
decisions and no complete monitored outcomes; extended swing had none.
No horizon has adequate independent evidence to establish an edge.
MFE/MAE are unavailable in historical monitored labels and remain null; the
single complete prints outcome had MFE 0.237R and MAE -1.132R.

## Research-to-code matrix before repair

| Recommendation | Baseline status | Existing implementation / gap |
|---|---|---|
| Market condition before family | PARTIALLY_IMPLEMENTED | `features.candle_features` regimes and `strategy.candidates`; no explicit discovery/normalization classifier; MA touch and one-close breakout definitions are weak |
| Costs relative to stop | FULLY_IMPLEMENTED | `risk.evaluate_risk`, `v2_evidence.trade_offer`; fees, funding, slippage, net RR and impact already exist; cost dominance is scored but not explicitly rejected |
| Frozen pre-trigger levels | PARTIALLY_IMPLEMENTED | Causal pivot availability and fixed plan levels exist; range context can roll independently; breakout trigger records the generic pivot rather than the qualifying range boundary |
| Aggression with price response | IMPLEMENTED_BUT_NOT_AUTHORITATIVE | `v2_evidence.mechanism` penalizes opposite displacement; `production.flow_support` can still qualify supportive delta/CVD using a stack/run |
| Executable liquidity | PARTIALLY_IMPLEMENTED | `Book.impact`, fixed-bps depth and spread history work; combined same-symbol percentile state and recovery hysteresis missing |
| Horizon matches signal life | PARTIALLY_IMPLEMENTED | Horizon stops, holding deadlines and long-horizon thesis-health protection exist; general entry-quality explanation and setup validity are incomplete |
| Correlated exposure budget | PARTIALLY_IMPLEMENTED | Manual portfolio risk cap and V8 exposure context exist; no account connection; notification deduplication does not prove portfolio exposure |
| Simple rules before complexity | PARTIALLY_IMPLEMENTED | Causal partitions, unchanged ML sample gates and immutable decisions exist; unified bounded trade-quality audit and v2.1 filter counterfactuals missing |

## Causal losing-trade diagnostics

The frozen evidence flags weak location, aggression without favorable response,
opposing absorption, costs large versus stop and target friction in subsets of
losers. Several losers remain UNKNOWN. These associations do not establish
causation. Score ordering is not monotonic in this small sample: C-grade proxy
outcomes outperform B-grade outcomes. No score-weight search is justified.
The audit reports feature readiness and adverse V8 states for each losing
decision, without treating unavailable optional features as adverse.

## Predeclared repair scope

Reuse the existing scanner, four families, live feed, profiles, risk model,
labels and ML partitions. Add versioned structural validity and entry-quality
evidence for new opportunities. Keep candidate-v20 and original score weights.
Use completed setup-timeframe bars for fixed boundaries, two-close acceptance
and a retest, measurable reclaim and trend persistence. Measure later pullback
attempts rather than banning them. Liquidity uses prior same-symbol samples and
hysteresis; no outcome-selected cutoff. Cost sensitivity at 0.5/0.75/1.0R is
diagnostic; the hard logical dominance boundary is costs >= original risk.
Skipped new-filter decisions retain frozen hypothetical plans for the existing
outcome workers, with no Discord production card.

Pairs, options, millisecond market making, parameter fitting, automatic model
promotion and historical relabeling are outside this repair. Forward samples
must measure both avoided losers and missed winners. Operational observation
after deployment cannot establish improved trading performance.

## Delivered monitored outcomes by cohort

These are the pre-change observations. Prints outcomes are reported separately
above. The small, overlapping sample is INSUFFICIENT in every cohort.

| Horizon | Delivered | Complete | Positive / negative | Mean net R | Profit factor |
|---|---:|---:|---:|---:|---:|
| SHORT_INTRADAY | 4 | 3 | 2 / 1 | -0.085 | 0.805 |
| CORE_INTRADAY | 37 | 32 | 9 / 23 | -0.501 | 0.253 |
| SWING | 3 | 0 | 0 / 0 | unknown | unknown |
| EXTENDED_SWING | 0 | 0 | 0 / 0 | unknown | unknown |

| Family | Complete | Positive / negative | Mean net R | Profit factor |
|---|---:|---:|---:|---:|
| Liquidity sweep | 23 | 6 / 17 | -0.535 | 0.214 |
| Range rejection | 8 | 3 / 5 | -0.366 | 0.505 |
| Trend pullback | 4 | 2 / 2 | -0.268 | 0.115 |
| Breakout retest | 0 | 0 / 0 | unknown | unknown |

Range: 11 complete, mean -0.431R, PF 0.427. Downtrend: 23 complete,
mean -0.446R, PF 0.223. Uptrend: one complete, -1.309R. All 35 observations
come from Binance; there is no comparison cohort for Bybit or OKX.
Score bins 0–20 / 20–40 / 40–60 / 60–80 respectively have 1 / 10 / 13 / 11
complete outcomes and mean -0.067 / -0.918 / +0.023 / -0.668R. There is no
monotonic evidence that the old deterministic score predicts realized returns.
The component-interaction audit separates horizon, family and regime rather
than treating these pooled bins as a calibration target.

## Retained components

| Component | Audit result and action |
|---|---|
| Net RR, impact, fees and horizon funding | FULLY_IMPLEMENTED; reuse `evaluate_risk`; new entry check uses its frozen cost allowance instead of another hardcoded fee estimate |
| Stop/noise and horizon stops | FULLY_IMPLEMENTED; retain existing structural/noise policies, no automatic widening |
| Flow trust, absorption, response | PARTIALLY_IMPLEMENTED; retain measurement, add a veto only for trusted aggression with opposite displacement or opposing absorption |
| OFI and liquidation sequence | FULLY_IMPLEMENTED within stated source-readiness limits; no second measurement engine or universal veto |
| Native profile refresh | FULLY_IMPLEMENTED; retain the c8a480e refresh before first candidate-v20 scoring |
| Auction/profile context | PARTIALLY_IMPLEMENTED; retain measurements, add frozen setup references and volume-weighted path diagnostics |
| BTC/ETH factors and spot/perp | PARTIALLY_IMPLEMENTED; existing horizon-aware alignment retained; one-minute optional context is not promoted to a swing veto |
| Correlated risk | PARTIALLY_IMPLEMENTED; manual account snapshot remains necessary; no inferred positions from Discord messages |
| Location/mechanism/trade offer | PARTIALLY_IMPLEMENTED; retain weights, bottleneck and confidence cap; version only the path component interpretation |
| Candidate-v20, causal partitions and model gates | FULLY_IMPLEMENTED; retained; no reduced sample requirements or automatic model promotion |
| Original lifecycle and visibility | FULLY_IMPLEMENTED; old plan policy tag never added, original deadline/levels preserved; exact initial-delivery rule retained |

## New-policy definitions

`trade-validity-v21` is attached only during new candidate generation. Original
plans do not acquire this tag. Structural event identity no longer changes each
scan minute for this policy, and a policy upgrade cannot recreate the same active
source/symbol/side/family/horizon/trigger event.

References use the existing horizon's 20 completed setup bars. Breakouts freeze
the window before two acceptance closes and the retest. Reclaims require an
actual excursion through the frozen boundary and a close back inside; a mere
nearby candle is insufficient. Breakout qualification and persisted trigger now
use the same boundary. Trend pullbacks require persistent context under the
existing HH/HL, slope and efficiency classifier. Separated MA touches in a
bounded 20-bar segment and the last 20 distinct attempts are recorded, not banned.
The attempt index is **not** claimed to identify the first pullback in an
unbounded trend history.

Entry quality uses executable bid/ask, original zone, frozen cost assumptions,
configured minimum net RR, original expiry and horizon ATR. It never moves the
stop or target to accommodate chasing. Strong trusted aggression against the
observed price response cannot be rescued by a high aggregate score. Weak or
zero displacement remains a score limitation; it is not automatically a veto.
The explicit cost-dominance boundary is cost/original-risk >= 1.0. Audit
sensitivity bins at 0.5/0.75/1.0 are descriptive and never choose that boundary.

Liquidity sampling reuses the existing profile worker and book: one sample per
minute, at most 360 samples, same source/symbol/session, no added REST calls.
After 20 past samples, jointly >=95th-percentile spread and <=10th-percentile
10-bps depth constitutes a thin observation. Three consecutive thin minutes
establish stress; two non-thin minutes clear it. The baseline is frozen during
the episode. A coverage gap resets persistence; stale evidence cannot veto a
new entry. Impact percentile and book reset delta are diagnostics, while the
existing absolute impact/depth gates remain mandatory. Percentiles are
predeclared research settings, not validated performance improvements.

The path score uses the fraction of native executed volume between entry and
TP1 plus the strongest observed obstacle. Three tiny nodes do not automatically
outweigh one large node. Missing volume remains unknown and receives no path
credit. Visible opposing depth is transient diagnostic context, not a target
probability or an additional fitted weight.

## Deferred rules and honest limits

- No new options/pairs engine, strategy-weight search, universal BTC veto or
  one-minute spot/perp veto for swing. Adequate comparative evidence is missing.
- No stop-width optimization: historical monitored labels lack MFE/MAE and one
  complete prints outcome cannot establish a better stop.
- Compression/discovery/normalization are descriptive states within the existing
  families, not a newly validated compression strategy.
- Long-horizon thesis health already distinguishes entry timing from persistent
  structural failure. Existing swing protection against one-minute noise stays.
- New filters have zero completed forward counterfactuals before deployment:
  avoided losers 0, missed winners 0, sample 0. This means unknown effectiveness,
  not zero opportunity cost. Every rejected candidate with an observable costed
  decision uses the existing prints/monitored label workers. Historical decisions
  are not retroactively rewritten as if the new filters had been deployed.
- Models retain existing compatibility and evidence requirements. A changed score
  profile is not evidence that an old model is calibrated for new decisions.

The audit command reads at most 5,000 decisions or 32 MiB of snapshot payloads,
reports truncation, and separates label policies. Routine status reads a cached
summary refreshed by the existing 15-minute background worker. It never replays
raw tape in the dashboard request path.

## Operational verification findings

The baseline recorder was stalled at the 30 GB limit and its service was
unhealthy. A SQLite WAL checkpoint recovered journal space without deleting
records; restarting the existing deployment restored healthy recording and
fresh streams for active setups. This is a temporary recovery, not a claim
that unlimited permanent history fits within a fixed budget.

Recovery logs also exposed an independent ML-worker crash: `directory_bytes`
could raise `FileNotFoundError` when concurrent retention removed a listed
manifest before `stat`. The traversal now tolerates only vanished paths;
permission and other I/O errors remain visible. Regression tests exercise both.

An isolated 120-bar candidate-generation benchmark measured median 3.35 ms for
the old policy and 9.01 ms for v2.1 (50 runs each). This is not a live scanner
throughput benchmark. New checks add no REST requests or stream subscriptions.
The baseline live scan took 300 seconds while source/spread history rewarmed;
compare warm runtime observations rather than attributing cold REST latency to
the small local validity calculations.

Local full verification: 558 passed, four optional skips. Offline research
passed. The isolated 20-second recorder soak wrote all 8,225 offered events with
zero drops (391 events/second achieved against 500 requested on this host).
The subsequent retention-race fix receives targeted tests and complete
Python 3.12/3.13 verification in exact-commit CI before deployment.

Live verification found large footprint arrays consumed the audit's byte bound
before all local decisions were included. The read query now projects out unused
footprint/profile arrays and ML vectors while retaining causal diagnostic fields.
Original stored snapshots remain byte-for-byte unchanged. Truncation is still
reported if the compact decision bound is reached.

The live audit also projects out duplicated feature metadata that no audit
calculation consumes. An oversized-metadata regression verifies the decision's
causal signal evidence remains readable under the byte limit and the original
stored snapshot is unchanged.
