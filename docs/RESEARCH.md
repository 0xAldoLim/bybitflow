# Research protocol

## Causal availability

Live and recorded replay use `features.candle_features`, `strategy.candidates`, `strategy.confirm`,
`orderflow.footprint` and `Book`. Only closed bars enter features. A 3-left/3-right pivot becomes
available at the confirming candle's close; trigger levels must be known before the trigger bar.
Internal pivots use 2-left/2-right. REST observations in replay become available on local receipt,
not retroactively at candle start. Manual facts require collection and knowledge times <= asof.
Future-dated catalysts can be known beforehand, but retrospectively entered facts cannot.

Universe membership is recorded only after all supporting observations arrive. Replay requires
that historical membership and independently reconstructs the normal-spread estimator from
timestamped quotes; an absent or stale record abstains. Five-minute time buckets prevent an
active period from receiving extra weight merely because it produced more updates. A missing
linked recording segment resets tape/book coverage and invalidates interrupted outcomes.

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

## Qualification

Quality scoring, ML ranking, and validated probabilities have separate admission
criteria. See [scoring](SCORING.md) for deterministic grades and [ML research](ML_RESEARCH.md)
for the implemented model and live-cohort requirements. Current print labels use
assumed costs and cannot satisfy verified-cost promotion.

## Reproduction and experiment controls

Every CLI research/replay run writes a new JSON experiment with parameters, input hashes,
source-code hash, available commit ID and outcomes. Do not delete failed experiments to
improve reported results. Reserve holdout files before development; repeated holdout access
requires a new untouched period. A current instrument list does not supply historical eligibility.

The `examples/offline_research.py` exercise uses unmistakably synthetic fixtures solely to
verify the research machinery. Its outputs must never be included in strategy validation.
