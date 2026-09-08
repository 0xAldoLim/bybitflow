# Research protocol and definitions

## Causal availability

Live and recorded replay use `features.candle_features`, `strategy.candidates`, `strategy.confirm`,
`orderflow.footprint` and `Book`. Only closed bars enter features. A 3-left/3-right pivot becomes
available at the confirming candle's close; trigger levels must be known before the trigger bar.
Internal pivots use 2-left/2-right. REST observations in replay become available on local receipt,
not retroactively at candle start. Manual facts require collection and knowledge times <= asof.
Future-dated catalysts can be known beforehand, but retrospectively entered facts cannot.

The live pipeline requires 4H support, a distinct 1H setup, a covered 15M executed-flow window,
fresh visible book, accepted costs/size and no known blocking event. Current footprint scores
are heuristic quality components, not statistical independence or probabilities.

## Explicit setup families

| Family | Long rule | Short rule | Execution trigger |
|---|---|---|---|
| Liquidity sweep | Known low breached and reclaimed in supportive/range 4H | Known high breached and rejected | Opposing aggressive volume, small displacement, matched same-level passive replenishment, directional 15M close |
| Trend pullback | Supportive 4H; 1H low touches MA20 and close recovers within 0.7 ATR | Mirrored declining rule | Directional initiative, displacement >0.15 ATR and at least three stacked imbalances |
| Range rejection | 4H/1H range; rejection of known lower boundary | Rejection of upper boundary | Same defended-level execution test as sweep, separately labeled family |
| Breakout retest | Prior close above previous 20-bar high, current retest and acceptance | Prior close below low, retest and rejection | Directional initiative plus stacked imbalances |

These are four independent hypotheses with versioned labels. Simultaneous price-derived
features are grouped in one structure contribution, not treated as independent votes.
Range targets use the opposite boundary; trending plans use labeled 3R/4R measured projections.
Targets are not asserted to be resting institutional liquidity. Stops use the sweep/range
extreme with a 0.15 ATR buffer. Entry-zone width is 0.3 ATR around the setup close.

Current diagonal imbalance uses buy at price / sell one bucket lower and sell at price /
buy one bucket higher. Denominators below 10% of median bucket volume do not qualify.
Ratio threshold starts at three and rises with ATR/price, capped at six. This is an
experimental adaptive rule, not a fitted optimal threshold. Value area expands contiguously
from maximum base-volume PoC, selecting the larger neighboring volume; lower-price ties win.
Zero-volume gaps inside the returned interval contain no invented trades.

Absorption requires price-level/time-matched visible additions and opposite-side prints;
delta alone cannot qualify it. Public data cannot distinguish hidden liquidity or intent.
Book history is bounded, so missing replenishment evidence means abstention, not assumed absence.

## Outcomes and fills

The paper simulator models a hypothetical aggressive entry after 500ms, capped at 1% of
observed print size, with a 60-second fill window. It never treats a touched limit price as
a fill. Exits consume print participation too; gaps use observed adverse prices. One whole
position exits at TP1, stop, or a four-hour time stop. TP2 is informational in this version.
Funding hooks take actual settlement rate and contemporaneous mark. Missing replay funding
or recording discontinuities prevent complete-cost qualification. Exact liquidation depends
on account, collateral, margin mode, risk tiers, mark behavior and fees; only a stress
approximation is implemented. The OHLCV baseline separately uses pessimistic stop-first bars.

## Qualification prerequisites (fixed before evaluating a model)

No candidate model is deployed in this release. The following are necessary conditions for a
future reviewed deployment artifact, not claims that sample counts alone prove reliability:

- Independent chronological predictions, four-hour purging plus embargo, symbol/regime/liquidity/
  family/direction strata and no overlap of unresolved labels with training.
- At least 200 resolved out-of-sample trades and 26 UTC-week clusters for a base candidate
  probability estimate; otherwise abstain. Independent uncertainty is bounded by cluster count.
- At least 52 weekly clusters for SS consideration and 78 for SSS, plus relevant regime coverage,
  positive lower 95% cluster-bootstrap net-EV bound, stable costs and acceptable tail drawdowns.
- Calibration/reliability evaluation and held-out score-bin probabilities, compared with
  uncalibrated and market-only baselines; sample adequacy must account for concentration and
  correlation beyond a week. A count does not guarantee acceptable uncertainty.
- Family-specific long/short results, untouched holdout, documented parameter trials, turnover,
  adverse selection, funding, drawdown and tail losses. No selection for win rate alone.
- SMC/order-flow/derivatives/fundamental ablations on the same timestamps/universe and cost
  assumptions. Missing actual features prevent that ablation, not candle proxies.
- SSS additionally requires raw quality >=95, all core coverage, no single-regime dependence,
  and independent review. The current scoring/coverage cap cannot reach it.

Implemented tools: clustered descriptive statistics, expanding-window score-bin estimation,
Brier evaluation, fixed 60/20/20 OHLCV partitions and development-only ATR sensitivity. Full
strategy validation, evidence-artifact approval, fold generation with multi-symbol overlapping
labels, and model deployment are unfinished. Raw quality 95 never means 95% probability.

## Reproduction and experiment controls

Every CLI research/replay run writes a new JSON experiment with parameters, input hashes,
source-code hash, available commit ID and outcomes. Do not delete failed experiments to
improve reported results. Reserve holdout files before development; repeated holdout access
requires a new untouched period. A current instrument list does not supply historical eligibility.

The `examples/offline_research.py` exercise uses unmistakably synthetic fixtures solely to
verify the research machinery. Its outputs must never be included in strategy validation.
