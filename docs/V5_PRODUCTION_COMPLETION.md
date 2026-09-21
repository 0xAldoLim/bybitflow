# Targeted production completion

## Scope and versions

This pass builds on `d730c46` without replacing the scanner, recorder, storage,
notification transport, or lifecycle engine. New native candidates carry
`production-v2` and explicit policy identifiers in `evidence.production_policies`.
Existing candidates retain their prior policy, plan, score, and deadlines.

The production policy is deterministic and active.
It is not claimed to be statistically optimal or guaranteed profitable.
Future research may justify a new version, but does not gate this implementation.

## Intraday confirmation

For SHORT_INTRADAY and CORE_INTRADAY liquidity sweeps and range rejections,
`intraday-confirmation-v2` requires the original family event plus a closed
response through its structural level. Candle color alone cannot create a setup.
Executed flow is classified as supportive, neutral, or opposing. Clearly opposing
flow cannot be overcome by profile context or market alignment.

With at least 20 prior same-symbol, venue, horizon, and session windows, ordinary
reversals need two supportive participation categories at percentile 0.60 or
higher. Categories are activity (trade count/intensity), size (volume/directional
aggressive notional), and imbalance magnitude. Correlated metrics within a category
count only once. Missing observations remain missing.

Before baseline maturity, raw fallback requires directional delta at least 15%,
persistence at least 0.60, directional CVD slope, and initiative flow or absorption
with favorable response. The chosen PERCENTILE or RAW_FALLBACK mode is persisted.
Weak candidates remain pending and can re-evaluate fresh closed flow windows until
the original entry deadline. Failed attempts no longer freeze a score and strand
the candidate. A successful decision is frozen normally. Entry windows are not
extended. Score category weights are unchanged.

## Market context and contrarian trades

`market-alignment-v2` separates aligned, neutral, contrarian, idiosyncratic, and
uncertain relationships. A stronger burden applies only with positive BTC beta,
correlation at least 0.50, beta instability at most 0.50, and a directional BTC
regime with efficiency at least 0.35. Material BTC/ETH or timeframe disagreement
makes the relationship uncertain rather than imposing a directional ban.

SHORT_INTRADAY considers execution/setup; CORE_INTRADAY considers setup/context/
execution. Swing uses its 1D/4H/1H frames, not 5m or 15m noise. All factor fits use
prior closed bars and preserve factor-fit and observation timestamps.

A valid opposite-direction intraday trade requires directional residual greater
than max(0.001, half the absolute expected BTC factor move), delta at least 20%,
persistence at least 0.75, directional CVD slope, and initiative, stacked imbalance,
absorption with favorable response, a directional aggressor run, or CVD acceleration.
Mature participation needs two categories at percentile 0.70 or higher. Immature
baselines use the stronger raw-flow requirement. Passing candidates receive
IDIOSYNCRATIC_DIVERGENCE and continue through ordinary structure, execution, cost,
and risk checks. Failure means CONTRARIAN_EVIDENCE_INSUFFICIENT, not a ban on shorts
in a bullish market or longs in a bearish one.

## New Swing stops and targets

`swing-stop-v2` applies only during generation of new SWING plans. The anchor is the
actual sweep/deviation, pullback, or failed-retest extreme together with confirmed
setup-timeframe structure. A complete causal executed profile can contribute an
adverse excess/rejection extreme within 0.75 setup ATR of that anchor and overlapping
the setup interval. This bounded same-thesis rule avoids choosing an unrelated
faraway auction level. POC or a value-area edge is not mechanically used as a stop.

The execution-noise estimate is the nearest-rank 80th percentile of the last 32
closed execution-bar ranges. Buffer = max(0.25 setup ATR, 0.75 noise), capped at
0.75 setup ATR. Stops round outward to the instrument tick. Anchor provenance,
noise window, sample count, buffer, ATR distance, basis-point distance, and
profile reference are stored. The final tick-rounded distance must not exceed
3 setup ATR. Over-wide stops are rejected as SWING_STRUCTURAL_STOP_TOO_WIDE;
they are never tightened back into the liquidity extreme.

`structural-levels-v3` computes targets after stop construction. It retains causal
POC/VAH/VAL/HVN/LVN, opposing swings and established ranges, and adds actual profile
excess/rejection prices. Suitable structural objectives precede measured 3R/4R
fallbacks. Risk, fees, slippage, execution assessment and net R:R use the resulting
plan through the existing risk engine. Missing profiles use structure plus noise,
without disabling Swing. Published stops never move. A liquidity grab can fit
inside a new buffered plan; crossing its published stop still ends that plan.

`executed-profile-v2` records excess/rejection flags and their actual observed
prices, acceptance, POC/VAH/VAL shifts, overlap and UP/DOWN/FLAT/UNAVAILABLE migration.
Incomplete coverage cannot supply levels. Rolling profiles may overlap and are
not treated as independent samples.

## Optional ML and continuity

No champion or research-sample minimum gates these deterministic policies. For
new-policy candidates ML filters are advisory even if the old optional research
filter setting is enabled. Optional inference failure does not block a valid
plan. Null evidence timestamps are marked missing instead of raising a snapshot
exception. Automatic model promotion and statistical policy optimization retain
their independent validation requirements. Old-policy candidates retain their
existing behavior.

The prior additive backup remains intact. This patch performs no schema migration,
history deletion, volume recreation, re-clustering, or existing-plan transformation.
Before/after record hashes and active immutable fields are checked separately from
normal lifecycle transitions. The private environment and its storage limit are
unchanged.

## Verification

Verification results are recorded below after targeted tests, the full regression
suite, runtime checks, continuity comparison, and the integrated live soak finish.
The soak checks operational health, not profitability or a guaranteed signal cadence.

### Test and continuity results

- Targeted confirmation, market alignment, Swing levels, scanner and lifecycle
  suite: 94 passed.
- Full regression suite: 284 passed, no failures or skips, four third-party
  deprecation warnings; elapsed 128.81 seconds.
- Ruff lint and formatting, JavaScript syntax, and Git whitespace checks passed.
- Actual Discord connection and labeled synthetic card returned `sent`. The
  synthetic card passed risk checks and created zero signal and ML rows.
- Public REST and genuine trade WebSocket probes passed for Bybit, Binance and
  OKX. Exchange clock differences were under half a second at the probe.
- All 16 captured pre-deployment active plans retained their immutable fields;
  their states subsequently reached their original entry expiry.
- Hash comparisons found zero missing/changed records among 30,203 terminal
  signals, 30,219 origins, 52,849 snapshots, 22,507 ML labels and 7,308 research labels.
- Storage was normal at 19.19 GB of the unchanged 30 GB budget, with no stale leases.
- Live persisted candidates exercised both PERCENTILE and RAW_FALLBACK under
  intraday-confirmation-v2. Old records were not converted to the new policy.

Complete profile construction requires continuous retained trades: the existing
30-minute intraday and four-hour Swing windows must warm after a restart. The
profile worker can be healthy while a profile remains unavailable; stop and target
fallbacks continue to work. No real signal is forced to satisfy a runtime test.

## Score audit and related setup cards

A read-only audit of 111 delivered initial alerts found no arithmetic mismatch:
each displayed quality score equals the rounded sum of its stored category points.
The older native-evidence-2 cohort (45 alerts) averaged 69.24; native-evidence-3
(66 alerts) averaged 62.34. These are different cohorts, not a controlled policy
comparison. Lower structure, fundamentals and order-flow contributions explain
much of the difference; no trade outcomes were used to tune this repair.

The audit also found 16 delivered setups with zero order-flow points. The v2
confirmation policy can accept directional reversal flow, but the old reversal
scorer only awarded defended absorption. New candidates now carry flow-score-v2
and native-evidence-4. A passed supportive confirmation can earn directional
flow credit using the minimum of directional delta / 50, persistence / 0.75 and
positive directional CVD, bounded to 0–1. That pathway and defended absorption
are alternatives: use the stronger, never add them together. Existing horizon,
persistence and depth modifiers remain. The category remains capped at 25 points;
all seven weights, tier boundaries, missing-data treatment and historical scores
are unchanged. The policy identifier and selected flow scoring basis are stored.

Dedicated setup-confirmation, conflicting-horizon and primary-thesis-update
layouts are replaced with the ordinary NEW SETUP card, using bracketed relationship
labels: [Setup confirmation], [Conflicting horizon], or [Stronger setup]. The card
shows updated new-plan details and the previous setup's direction, horizon, score,
entry/zone, stop, targets and original deadline. Conflicting directions are explicit.
The prior setup remains monitored; no plan is cancelled, moved or rescored by a
presentation change. Existing Discord history is preserved.

### Final score/card follow-up verification

The follow-up scoring, clustering/card and production tests passed (56 tests).
The final full suite passed 289 tests with no skips or failures and four
third-party deprecation warnings in 142.71 seconds. Static checks passed again.
A clearly labeled TEST ONLY conflicting-horizon card was delivered successfully;
it created zero signal rows and zero ML rows. This verifies the revised layout
through the existing Discord transport, without generating a real trade.

The first integrated live soak lasted 662 seconds: confirmation, recorder and
profile workers remained healthy; queue samples returned to zero, with no dropped
events or recorder retries. The scan completed with 528 discovered instruments
and 30 deep selections, recording one isolated per-market error without stopping
the scan. A second soak verifies the final scoring/card deployment separately.
