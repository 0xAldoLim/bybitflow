# Do the recorded order-flow readings predict outcomes? 2026-11-28

Candle replays cannot test the part of the system the supplied literature is
mostly about: executed flow, the order book, and derivatives positioning. Those
readings exist only where the live scanner recorded them. This study uses every
decision the scanner ever stored.

Script: `analysis/audit/decision_features.py`, with its readings and pass
criteria committed (`34566a0`) before any outcome was computed.

## Verdict

**No recorded reading passes.** Eighty readings were declared and 76 had enough
data to test, on 5,820 distinct decisions across 154 symbols. None separates winners from losers strongly enough
in the test period and again in the held-out period. A model given all of them
at once finds one group that might be break-even and cannot be told from zero.

The readings do lean in a consistent direction, and it is the one the
literature describes. That is worth tracking forward. It is not yet a reason to
change what is sent.

## The data, and a problem found in it

| | |
|---|---:|
| Decision snapshots exported | 28,852 |
| Repeats of the same event within the hour | 22,558 |
| Stop closer than the round-trip cost, or otherwise unusable | 474 |
| Distinct decisions analysed | 5,820 |
| Symbol-days | 1,663 |

78% of the stored decisions were repeats. One structural event was stored up to
448 times, and one symbol on one day accounts for 1,619 rows. The duplication
ran at 21 to 23 copies per event in the first half of September and has been
about 1.1 since October, so it is a past defect and not a live one. It matters
for two reasons: any statistic computed on the raw table is dominated by a
handful of days, and the ML was trained on rows that were mostly copies of each
other.

Baseline on the distinct decisions: +1R before −1R in 50.9% (±1.8), and the
published plan nets −0.14R per decision after costs.

## Protocol

- Outcome: entering at the next 5-minute open after the decision, does price
  reach +1R before −1R, with R the distance to the frozen stop. A bar touching
  both counts as the stop.
- Readings with a direction are expressed relative to the trade, so "with the
  trade" means the same for longs and shorts.
- First 75% of decisions by time is the test sample, the last 25% is held out.
- A reading passes only if its top third and bottom third differ in the test
  sample with |z| ≥ 3.42 (Bonferroni for 80 readings, errors clustered by
  symbol-day), and the held-out sample agrees in sign with |z| ≥ 2.

The de-duplication, the minimum stop and a clip on net R were added after the
first run exposed the repeats. The readings, thresholds and split were not
changed.

## Results

Strongest readings, none of which passes:

| Reading | Lowest third | Highest third | z test | z held out |
|---|---:|---:|---:|---:|
| 4h slope with the trade | 47.2% | 55.5% | +3.0 | +1.6 |
| Stop distance relative to noise | 48.2% | 57.3% | +2.8 | +1.4 |
| Perpetual basis with the trade | 48.4% | 57.2% | +2.7 | +2.1 |
| Flow delta with the trade | 57.2% | 50.1% | −2.7 | −1.5 |
| Absorption on the trade's side | 51.7% | 61.5% | +2.6 | n/a |
| **Setup Score** | 55.5% | 48.4% | −2.5 | +0.5 |
| Flow delta persistence with the trade | 55.3% | 48.0% | −2.4 | −3.3 |
| Stacked imbalance with the trade | 55.2% | 48.5% | −2.3 | −1.3 |
| Book imbalance within 25 bps, with the trade | 48.2% | 54.5% | +2.1 | +1.3 |

Readings that did nothing: book imbalance at the touch, 5 bps and 10 bps;
initiative with the trade; the order-flow, regime and execution score
components; risk accepted; no gates raised; hour of day.

Model check, all readings together, walk-forward on 2,910 later decisions:

| Predicted group | +1R first | Net per trade | Lower bound |
|---|---:|---:|---:|
| Worst tenth | 49.1% | −0.33R | −0.55R |
| Middle | 42% to 50% | −0.17R to −0.36R | all negative |
| Best tenth | 53.3% | +0.14R | −0.07R |

## Reading it against the literature

The pattern is coherent, which is more than the Setup Score managed:

- **Aggressive flow already running with the trade is a bad sign.** Flow delta,
  its persistence and stacked imbalance in the trade's direction all point the
  wrong way, in both periods. This is the chase: by the time buyers are visibly
  lifting, the entry is late. It is also how the scanner confirms continuation
  setups today (`initiative` plus three stacked levels with the trade).
- **Absorption on the trade's side is a good sign.** Passive defence of a level
  is what MarketDelta and the Market Profile guides describe as responsive
  activity. It is the one flow reading that leans the right way.
- **Depth beats the touch.** Book imbalance matters at 25 bps and not at the
  best quote, consistent with touch quotes being noise at this horizon.
- **The Setup Score leans the wrong way** in the test period: the highest-scored
  third did worse than the lowest. It rewards flow with the trade, which is the
  reading that hurts.

None of this is proven. 2.6 to 3.0 standard errors on one of 80 readings is what
chance produces, which is why the bar is 3.42 and why nothing was changed.

## What was done with it

Three readings are fixed as forward hypotheses and tagged on every delivered
alert from release 2.4.3 (`forward_hypotheses` in `acceptance.py`). They never
affect delivery:

| Tag | True when |
|---|---|
| `absorption_with_trade` | The flow window shows defended absorption on the trade's side |
| `flow_not_chasing` | Flow delta in the trade's direction is zero or negative |
| `higher_timeframe_with_trade` | The context-timeframe slope points with the trade |

`bybit-flow scorecard` reports delivered outcomes for each tag and its
opposite. If a tag separates real alerts over the coming months, that is
evidence from data no rule was fitted to, and it becomes a candidate gate to
put through the acceptance protocol. If it does not, it is closed.

## Reproduce

```
docker exec -i bybitflow-desk-1 python - < analysis/audit/export_decisions.py > analysis/audit/decisions.jsonl
python analysis/audit/decision_features.py
```

Candles for the labelled symbols are fetched with `acceptance.download` into
`analysis/backtest/candles-decisions`.
