# Acceptance: is any rule proven profitable?

`bybit-flow acceptance run` answers this from public data, and
`bybit-flow acceptance status` prints the current answer. The verdicts ship with
the code (`acceptance_report.json`, `outcome_model.json`), so the running system
states on every alert what its rule has demonstrated.

## Current answer (2026-11-27, after two rounds)

**No rule is proven profitable.** 92,174 replayed setups, 12 liquid perpetuals,
58 rules tried: 50 rejected, 8 with too few setups to judge. A 59th rule,
declared later and tested on fresh symbols, was also rejected. A replication of
the one lead on 48 further symbols (22,068 setups) and a test on 40 mid-cap
symbols (51,919 setups) also accepted nothing.

### Round 1: the scanner's rules and plain momentum

| Rules | Period | Setups | Best result per trade | Verdict |
|---|---|---:|---|---|
| Short intraday, 4 families and their mirrors | 120 days | 9,308 | −0.36R | all rejected |
| Core intraday, 4 families and their mirrors | 1 year | 14,044 | −0.12R | all rejected |
| Swing, 4 families and their mirrors | 3 years | 7,861 | +0.02R, lower bound −0.14R | none accepted |
| Momentum over 24h, 72h, 168h and reversals | 3 years | 39,076 | −0.00R | all rejected |

### Round 2: the three ideas, declared before they were run

| Idea | Setups | Result | Verdict |
|---|---:|---|---|
| Swing continuation with 1R, 1.5R, 2R targets or time exit only | 5,528 | Best: 2R target, +0.00R, lower bound −0.14R | none accepted |
| Resting-order entry, core intraday | 22,329 | Loss shrinks from about −0.31R to −0.21R on sweeps; best family −0.17R | all rejected |
| Resting-order entry, swing | 12,090 | Trend pullback +0.04R test, +0.15R held out, lower bound −0.12R | none accepted |
| Funding extreme, fade the crowd | 15,369 | 48.2% at 1:1 (z −3.8): fading is wrong | rejected |
| Funding extreme, follow the crowd | 15,369 | 51.7% in test (z +3.8), then **47.3% held out** | rejected |
| Cross-sectional 7-day momentum and reversal | 6,516 | 50.4%, −0.03R | rejected |

What round 2 established:

- **Cost is not the whole problem intraday.** Paying maker fees instead of taker
  fees recovers about 0.1R per trade. The rules still lose 0.17R to 0.28R, and
  the hit rate falls, because a resting order is filled more often when price is
  moving against it.
- **Changing the target does not create an edge.** Four exits on the same swing
  entries land between −0.07R and +0.00R.
- **The hold-out did its job.** Following extreme funding passed the corrected
  significance threshold in the test period and reversed in the held-out
  quarter. Without the hold-out it would have been reported as a discovery.
- **Swing trend pullback was the only lead, and it is now closed.** It was
  positive in both periods on the original 12 symbols and never distinguishable
  from zero. The replication below settled it.

Outcome grades at this release: short intraday F, core intraday E to F, swing D
to C. The grade model ranks setups out of sample (best tenth +0.00R, worst
tenth −0.71R), but what it ranks is cost: wider stops lose less to fees. No
group of setups has a positive expectation.

## Replication of the one lead: closed

Declared before it was run (commit `fc48518`): replay the swing rules,
unchanged, on the 48 highest-volume USDT perpetuals listed for at least three
years, excluding the 12 symbols above (`REPLICATION_SYMBOLS` in
`acceptance.py`). None of these symbols was used to form a rule or to notice the
lead. Same protocol, same criteria, and the significance bar stays at the level
set by all 58 rules.

Result: 22,068 setups on 48 symbols, 2024-01-28 to 2026-11-26. **Nothing
accepted, and the lead did not replicate.**

| Swing trend pullback | Test setups | +1R before −1R | Net per trade | Held out |
|---|---:|---:|---:|---:|
| As published | 2,095 | 47.1% (z −2.6) | −0.03R, lower bound −0.16R | +0.18R |
| Limit entry at the close | 2,075 | 46.9% (z −2.8) | −0.02R | +0.19R |
| 2R target | 2,095 | 47.1% | −0.03R | +0.18R |
| Time exit only | 2,095 | 47.1% | −0.05R | +0.17R |

On three times the original sample the test-period result is slightly negative,
and the directional hit rate is below 50%. The held-out quarter is positive
again, as it was on the original 12 symbols, which says the most recent quarter
suited trend pullbacks everywhere, not that the rule has an edge: a rule that is
negative for two years and positive for one quarter is a regime, and the mirror
of the same rule shows the opposite pattern (+0.05R test, −0.29R held out).

The other swing families on the fresh symbols: liquidity sweep 50.0% on 8,034
setups, range rejection 49.7% on 5,915, breakout retest 47.0% on 500.

Caveat fixed in advance: requiring three years of listing keeps only survivors,
which flatters any long-biased result. It did not help.

```
bybit-flow acceptance replicate --candles analysis/backtest/candles-replication
```

## Mid-cap perpetuals: nothing accepted, one directional finding

Declared before it was run (commit `fe9ddb5`): the same swing and core intraday
rules, unchanged, on 40 mid-cap USDT perpetuals (`MIDCAP_SYMBOLS` in
`acceptance.py`): ranked 101 to 250 by 24h quote volume, listed at least one
year, never used above.

Costs were declared higher. The set's median half-spread measured 2.06 bps
against 0.52 bps for the large caps, so slippage is 4 bps per side instead of 2:
19 bps round trip for market orders, and 9.5 bps on any stop or time exit of a
resting-order variant. Same protocol, same criteria, same significance bar.

Result: 51,919 setups on 40 symbols, 2024-02-13 to 2026-11-27, no replay
errors. **Nothing accepted.** 32 rules rejected, 8 with too few setups.

| Rule | Test setups | +1R before −1R | Net per trade | Held out |
|---|---:|---:|---:|---:|
| Core intraday, all four families as published | 34,129 | 46.0% to 50.4% | −0.12R to −0.20R | all negative |
| Core intraday trend pullback, **mirrored** | 3,939 | **54.0% (z +5.0)** | −0.06R | −0.08R |
| Swing liquidity sweep, as published | 2,326 | **53.3% (z +3.2)** | +0.02R, lower bound −0.06R | −0.02R |
| Swing liquidity sweep, limit entry 0.25R better | 1,395 | 52.5% | +0.07R, lower bound −0.06R | −0.03R |
| Swing trend pullback, as published | 626 | 51.5% | −0.02R | −0.01R |

Two rules cleared the corrected significance bar for *direction* and still
failed, because direction is only half the criterion:

- **Swing liquidity sweeps on mid-caps** point the right way 53.3% of the time.
  After 19 bps that is worth +0.02R with a lower bound below zero, and the
  held-out quarter is slightly negative. Real tilt, not a profit.
- **Intraday trend pullbacks are wrong more often than right, everywhere.** The
  published rule reaches +1R first 46.0% of the time on mid-caps (z −5.0), 46.6%
  on the large caps and 45% with a limit entry. Fading it is right 54% of the
  time, which is the strongest directional result in this whole exercise. It
  still loses 0.06R per trade at market-order costs.

The second point is a finding about the scanner as it runs today: the core
intraday trend pullback is the one family with evidence that it is
systematically on the wrong side, not merely uninformative.

Caveats fixed in advance: these symbols have one to three years of history, so
samples are smaller; current volume rank selects coins that are liquid today,
which is a mild look-ahead; and candle replay cannot see the thinner order books
that make real fills worse than modelled.

```
bybit-flow acceptance replicate --universe midcap --candles analysis/backtest/candles-midcap
```

## Fading the intraday trend pullback: rejected

Declared before it was run (commit `b5ace93`): one rule, `fade_maker_1r`. When
the core intraday trend pullback fires, trade against it with a resting limit at
the setup-bar close. Stop one planned risk unit away, target one unit away as a
resting limit, 4h hold. Both limits must be traded through, not merely touched.
Entry and target pay the 2 bps maker fee; a stop or time exit pays taker fee and
slippage. It is the 59th rule, and the significance bar rose to 3.14.

The verdict was taken on the 48 replication symbols, whose core intraday data
had never been replayed. The other two sets had revealed the trigger, so they
are context only.

| Set | Published rule, +1R first | Fade at market, +1R first | Fade with resting orders | Net per trade | Held out |
|---|---:|---:|---:|---:|---:|
| **48 fresh symbols (verdict)** | 47.4% (z −3.8) | 52.6% (z +3.8) | 51.1% (z +1.6) on 5,042 | **−0.07R**, lower bound −0.12R | −0.06R |
| 40 mid-caps (context) | 46.0% (z −5.0) | 54.0% | 52.5% on 3,790 | −0.04R | −0.05R |
| 12 large caps (context) | 45.9% (z −3.0) | 54.0% | 53.3% on 1,281 | −0.09R | −0.02R |

**Rejected.** Two things are now established, and they point in different
directions:

- **The published core intraday trend pullback is wrong-sided.** 47.4% on
  symbols it had never seen, after 46.0% and 45.9% on the sets that suggested
  it. Three independent universes, 10,519 test setups, the same sign each time.
  This replicated.
- **The mistake cannot be harvested by fading it.** A resting sell is filled
  when price is rising through it and a resting buy when price is falling, so
  the orders that fill are the worse ones: the hit rate drops from 52.6% to
  51.1%. On the fresh symbols 96% of orders filled, 54% of trades ran out the 4h
  clock without reaching either level, and the average cost was 8 bps on a
  194 bps stop. The edge is smaller than the cost of collecting it.

What follows for the running scanner is not a new trade but a removal: this
family carries evidence of being worse than a coin flip, which no other family
does.

```
bybit-flow acceptance replicate --horizons CORE_INTRADAY --families trend_pullback --extra-rules 1 --candles analysis/backtest/candles-replication
```

## What ACCEPTED means

Fixed in `src/bybit_flow/acceptance.py` before results were seen:

- Entry at the next 5-minute open after the setup bar closes, or a resting limit
  that must be traded through. R is the distance to the published stop. A bar
  that touches both barriers counts as the stop.
- Costs are the default taker fee and slippage on both sides. Resting-order
  variants pay a 2 bps maker fee on entry and on target exits, and full taker
  cost on stops and time exits.
- First 75% of the period is the test sample. The last 25% is held out.
- Test sample: "+1R before −1R" must beat 50% with a z-score corrected for
  every rule ever tried (3.13 for 58 rules), **and** the traded plan must net a
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

With today's verdicts, `enforce` delivers nothing. Both settings are correct
readings of the evidence; neither is a malfunction.

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

## Where this leaves the search

Fifty-eight rules built from candle structure, momentum, funding and relative
strength have been tested on up to three years of data. The honest conclusion is
that public candle and funding data on the largest perpetuals does not contain
an edge these rule shapes can reach after retail costs. That is consistent with
these being the most efficiently priced crypto markets.

What remains worth doing, in order:

1. ~~Resolve the one lead with more data.~~ Done above: swing trend pullback
   did not replicate on 48 fresh symbols. There is no open lead among the
   candle-structure rules.
2. ~~Trade less liquid markets.~~ Done above: 40 mid-caps, nothing accepted.
   The one question it raised, fading the core intraday trend pullback with
   resting orders, was declared, tested on fresh symbols and rejected.
3. **Use information that is not in candles.** Liquidation clusters, order-book
   state at decision time and spot-perpetual basis are what the supplied
   microstructure literature actually describes. They need recorded data, which
   the recorder can now collect without ever blocking alerts.

Do not loosen the criteria, drop the hold-out, or keep adding variants until one
passes. Each new rule raises the bar for all of them, by design.

## Run it

```
bybit-flow acceptance run        # downloads candles and funding, replays every rule, writes the verdicts (about 90 minutes)
bybit-flow acceptance status     # prints the current verdicts
```

Commit the two regenerated JSON files with any change to an entry rule.
