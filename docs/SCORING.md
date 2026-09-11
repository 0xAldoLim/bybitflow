# Evidence scores and qualification

Research grades span the full 0–100 range: SSS >=95, SS >=90, S >=85,
A >=75, B >=65, C >=50, D >=0. D includes the former E/F score bands;
numeric component weights and mandatory rejection gates are unchanged.
With `FLOW_RESEARCH_ALERTS=true`, confirmed setups across all grades can send
uncalibrated research cards. `FLOW_SSS_RESEARCH=true` alone remains the narrower
SSS-only opt-in. These grades are evidence scores, not win probabilities.
Discord cards show the grade, entry zone, stop, TP1 and TP2. Lower-grade cards
remain subject to confirmation, freshness, risk, cooldown and deduplication.

`native-evidence-2` retains the original weights. The former fixed fractions capped quality at
83. Current contributions change with observations and have direct unit tests, including a
fully populated synthetic 100-point case. This proves software reachability, not the market
frequency or effectiveness of high scores.

| Native component | Maximum | Measured rubric |
|---|---:|---|
| Regime | 15 | Confirmed 4H directional efficiency / 0.5, clipped |
| Structure | 20 | Confirmed family structure, invalidation distance relative to 2 ATR |
| Executed flow | 25 | Continuation: delta /25% and directional stack /4; reversal: delta /50% and matched defended/opposing notional /.25, requiring actual absorption |
| Derivatives | 10 | Actual OI change and funding available; same-direction funding crowding penalty |
| Execution/risk | 15 | Accepted risk calculation; cost-adjusted planned RR /2.5 |
| Fundamentals | 10 | Sourced, current coverage of purpose, value accrual, dilution, security, governance |
| Cross-market | 5 | Fraction of available configured external BTC/ETH regime slots aligned; excludes self |

Fundamental points measure due-diligence **coverage**, not a token valuation, absence of risks,
or permission to override a price setup. Major known events are rejection gates. Missing
categories earn nothing. A directionally negative funding rate is not proof of profitable longs;
the rubric only penalizes specified crowding. These initial thresholds remain experimental.

`tv-evidence-1` is explicitly a different, source-specific rubric:

| Component | Maximum | Measured rubric |
|---|---:|---|
| Regime | 20 | Weaker of directional EMA slope /0.3 ATR and efficiency /0.5 |
| Structure | 25 | Recomputed family sweep/reclaim or continuation and execution-level break |
| TV classified footprint | 30 | Weaker of directional delta /25% and stack /4 |
| Risk geometry/cost | 15 | Accepted cost/risk gates, net RR /2.5 |
| Observed liquidity | 10 | Instrument age/status, turnover, spread history, quote freshness and executable notional |

TV values are authenticated source **attestations**, not independently re-downloaded chart
history. Raw row stacks originate in Pine; the gateway checks ranges/direction and the aggregate
observations but cannot independently prove the supplied row calculation. The inbox retains
the original observations for comparison with recordings/exports.

All fractions are clipped to [0,1]. FVG, sweep, wick, CHoCH and trapped-participant observations
from one event are not accumulated as independent conviction votes. No missing native tape,
derivatives, fundamentals or Binance field is presented as observed in the TV score. Scores
from these two profiles are not interchangeable or eligible to share calibration bins.

Strict TV SSS research needs raw >=95, all gates, and explicit `FLOW_SSS_RESEARCH=true`.
The label always includes RESEARCH and UNCALIBRATED. Turnover-proxy mode lacks actual spread,
depth and contract sizing: capped at 84, never strict SSS. Default notification toggles are off.
Validated public SSS remains locked in `Notifier.send_public`; there is no model artifact that
unlocks it. A score is never assigned to `probability`.

Native strategies now support the same explicit `FLOW_SSS_RESEARCH=true` opt-in: raw ≥95,
all structure/flow/liquidity/freshness/entry/risk gates, no rejections, and an accepted risk
calculation. The result is `SSS RESEARCH · UNCALIBRATED`, not a validated probability.
Missing fundamentals still earn zero; a default installation without verified facts may
never reach 95. Cross-market means fresh same-venue BTC/ETH regimes, not multi-venue
agreement. Experimental multi-venue comparisons earn **zero** predictive credit.

The ML layer is separate from this measured rubric: it can explain/rank/filter research
candidates, but cannot replace hard gates or convert probability to quality. Registry
approval and live cohort evidence are required for validated labels; current assumed-cost
labels cannot pass approval. See USER_RUNBOOK.md for the actual requirements. Ablations,
regime stability and empirical probability calibration require real independent data;
none is manufactured by software tests.
