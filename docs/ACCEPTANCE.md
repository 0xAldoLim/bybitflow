# Acceptance: is any rule proven profitable?

`bybit-flow acceptance run` answers this from public data, and
`bybit-flow acceptance status` prints the current answer. The verdicts ship with
the code (`acceptance_report.json`, `outcome_model.json`), so the running system
states on every alert what its rule has demonstrated.

## Current answer (2026-11-27)

**No rule is proven profitable.** 69,967 replayed setups, 12 liquid perpetuals,
30 rules tried, costs 15 bps round trip.

| Rules | Period | Setups | Best result | Verdict |
|---|---|---:|---|---|
| Short intraday, 4 families and their mirrors | 120 days | 9,308 | −0.36R per trade | all REJECTED |
| Core intraday, 4 families and their mirrors | 1 year | 13,515 | −0.13R per trade | all REJECTED |
| Swing, 4 families and their mirrors | 3 years | 8,068 | +0.02R, lower bound −0.15R | all REJECTED |
| Momentum baselines (24h, 72h, 168h) and reversals | 3 years | 39,076 | −0.01R per trade | all REJECTED |

Closest to passing: swing trend pullback, +0.01R in the test period and +0.14R
in the held-out quarter on 694 and 452 setups. That is indistinguishable from
zero. On one year of data the swing horizon looked slightly positive; with three
years it does not. That is why the sample was extended before believing it.

Outcome grades at this release: short intraday F, core intraday E to F, swing D
to C. The grade model does rank setups out of sample (best tenth −0.02R, worst
tenth −0.71R), but what it ranks is cost: wider stops lose less to fees. No
group of setups has a positive expectation.

## What ACCEPTED means

Fixed in `src/bybit_flow/acceptance.py` before results were seen:

- Entry at the next 5-minute open after the setup bar closes. R is the distance
  to the published stop. A bar that touches both barriers counts as the stop.
- Costs are the default taker fee and slippage on both sides.
- First 75% of the period is the test sample. The last 25% is held out.
- Test sample: "+1R before −1R" must beat 50% with a z-score corrected for the
  number of rules tried (2.94 for 30 rules), **and** the traded plan must net a
  positive R whose day-clustered lower 95% bound is above zero.
- Held-out sample: still above 50%, and still a positive net R.
- At least 200 test and 50 held-out setups, or the verdict is
  INSUFFICIENT_SAMPLE.

This is a structure-only replay. Order-flow confirmation and later gates need
recorded ticks and are not replayed. The 367 alerts actually delivered did worse
than the structure alone (42.8% against about 50%), so the filters are not
hiding an edge.

## How it reaches an alert

| Setting | Default | Effect |
|---|---|---|
| `FLOW_ACCEPTANCE_GATE` | `report` | `report`: each alert carries its rule's record. `enforce`: only ACCEPTED rules are delivered. `off`: ignored. |
| `FLOW_GRADE_SOURCE` | `setup_score` | `outcome`: `FLOW_MIN_ALERT_TIER` compares the outcome grade instead of the Setup Score. |

With today's verdicts, `enforce` delivers nothing, and `outcome` with a minimum
of C delivers only swing trend pullbacks and breakout retests. Both are correct
readings of the evidence.

Each delivered alert stores its rule record and outcome grade.
`bybit-flow scorecard` then reports real delivered results by outcome grade, so
the grades themselves are checked against what happens.

## Outcome grades

| Grade | Meaning, per trade after costs |
|---|---|
| SSS, SS, S | Lower 95% bound at least +0.50R, +0.40R, +0.30R |
| A, B | Lower bound at least +0.15R, +0.05R |
| C | Expected at least zero, not proven |
| D, E, F | Expected at least −0.15R, at least −0.30R, worse |

The top five grades require a *proven* lower bound. A good-looking average on a
small sample is a C.

## Getting to a profitable rule

The test is now cheap, so the work is proposing rules and letting it reject
them. What the evidence suggests trying, each declared before it is run:

1. Trend pullbacks at swing horizon with the daily trend, with targets taken
   from the measured excursion rather than a fixed 3R. It is the only family not
   clearly negative.
2. Lower cost per trade. Intraday rules lose mostly to the 15 bps: a maker entry
   or a venue with lower fees changes the arithmetic more than any filter.
3. Rules that do not use these four structures at all, such as funding or
   open-interest extremes, or cross-sectional strength across symbols.

Do not loosen the criteria, drop the hold-out, or keep adding variants until one
passes. With 30 rules tried, one false pass in twenty is the expected rate of an
uncorrected test, which is why the threshold is corrected.

## Run it

```
bybit-flow acceptance run        # downloads candles, replays every rule, writes the verdicts (about 90 minutes)
bybit-flow acceptance status     # prints the current verdicts
```

Commit the two regenerated JSON files with any change to an entry rule.
