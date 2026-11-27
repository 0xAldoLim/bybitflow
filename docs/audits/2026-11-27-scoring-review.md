# Scoring and trade-selection review, 2026-11-27

Question: are the score and its metrics good enough to pick trades, and if not,
what should change. This reviews the score against (a) the literature supplied
to the project (`docs/v2/INSILICO_RESEARCH_LEDGER.md`, `docs/RESEARCH_REFERENCES.md`
and the source library they index) and (b) three measurements made for this
review. Scripts are in `analysis/audit/` and `analysis/backtest/`.

## Short answer

No. The score is a careful translation of sound ideas into numbers nobody
tested. It measures how much a setup *resembles the textbook description*, not
how often setups like it work. On every measurement available, resemblance and
outcome are unrelated. The fix is not better weights. It is to test the entry
first, then let outcomes set the weights.

## Three measurements

### 1. Does anything the scanner records predict the outcome?

For the 367 delivered alerts with price paths, every score component and 30
other recorded inputs were ranked against "+1R before −1R" and the 4h return
(`analysis/audit/feature_stats.py`).

| Input | Rank correlation with win | z |
|---|---:|---:|
| Order-book imbalance within 25 bps, trade direction | +0.13 | +2.3 |
| Mechanism score (v2/v2.1) | +0.20 | +2.1 |
| Setup Score | +0.09 | +1.6 |
| Location score (v2/v2.1) | −0.13 | −1.4 |
| Order-flow score (native rubric) | +0.03 | +0.5 |
| Flow delta % in trade direction | −0.01 | −0.2 |
| Structure score, regime score, execution score | about 0 | under 0.5 |
| With / against the 4h trend | −0.07 | −1.2 |
| Prior 1h, 4h, 24h, 72h return in trade direction | 0.02 to 0.06 | under 1.2 |

Forty inputs were tested, so two results above |z| = 2 are what chance alone
produces. The input the score weights most heavily, executed-flow confirmation,
has no relationship to the outcome at all. Higher location scores did slightly
worse.

### 2. Do the entry rules work before any filtering?

The production function `strategy.candidates` (the same one the scanner calls,
policy `trade-validity-v22`) was run at every closed setup bar for one year on
twelve liquid perpetuals, with no flow confirmation, score or gate
(`analysis/backtest/run.py`). Entry is the next 5-minute open; ties count as the
stop; costs are the project's own 15 bps round trip.

| Horizon | Candidates | +1R before −1R | +2R before −1R | Net per trade |
|---|---:|---:|---:|---:|
| Core intraday (4h / 1h / 15m) | 13,706 | 49.2% (z −2.0) | 32.3% vs 33.3% | −0.27R ± 0.04 |
| Swing (1d / 4h / 1h) | 2,695 | 51.5% (z +1.5) | 36.6% vs 33.3% (z +3.6) | −0.07R to 0.00R ± 0.1 |

Core intraday by family: liquidity sweep 49.3%, range rejection 49.9%, breakout
retest 50.5%, trend pullback 45.5%. Signed return after 1h, 4h and 24h: +0.0,
−1.7 and −11.5 bps. Stable across all four quarters of the year.

Swing: 53.1% in the right direction after 24h, mean +23.6 bps. The published
plan (about 3R target, 48h hold) nets +0.01R per trade, which is break-even.

So the intraday structure rules carry no directional information on 13,706
samples. There is nothing there for a score to rank.

### 3. Do the filters add what the structure lacks?

| Stage | Sample | +1R before −1R |
|---|---:|---:|
| Structure only, core intraday | 13,706 | 49.2% |
| Delivered (flow-confirmed, gated, scored), core intraday | 225 | 41.3% |
| Delivered, short intraday | 61 | 36.1% |
| Structure only, swing | 2,695 | 51.5% |
| Delivered, swing | 54 | 57.4% |

Flow confirmation, eleven policy versions and the V8 gates took a coin flip and
delivered something measurably worse intraday. The likely mechanism is in
section "Why", item 3: confirmation waits for aggressive flow in the trade's
direction, so the alert fires after the favourable move, at a worse price.

## Why, against the supplied literature

1. **The sources give concepts, the weights were invented.** The ledger says so
   itself: "The proposed score weights and numeric state thresholds are
   implementation inferences; no archive author validated them on this
   program's outcomes." Dalton and the CBOT guide describe how to *read* an
   auction. They do not say that 0.30 Location + 0.45 Mechanism + 0.25 Offer
   ranks crypto perpetual setups. A score built this way is an opinion with
   decimals.

2. **The supplied validation literature was cited and not applied to the
   entry.** Pardo (*Evaluation and Optimization of Trading Strategies*) and
   Bandy (*Quantitative Trading Systems*), both in the library, put one step
   before everything else: show the raw rule has positive expectancy out of
   sample, then add filters one at a time and keep only those that improve it.
   The project applied chronological splits and holdouts to the ML layer and
   never ran the base test on the four rules. Section 2 above is that test. It
   takes fifteen minutes on public candles.

3. **Order flow was used at the wrong horizon.** Hasbrouck and O'Hara, and the
   order-flow-imbalance work that followed ([Cont, Kukanov and Stoikov](https://arxiv.org/abs/1011.6402);
   [Cont, Cucuringu and Zhang](https://arxiv.org/abs/2112.13213v3)), find that
   signed flow explains price change *over the same short interval* and that its
   predictive content "decays rapidly". Here a 60-second flow window, with a
   median traded notional of about $50,000, confirms a trade meant to last one
   to four hours, and the alert then reaches a person on Discord. By the time
   anyone can act, the information in that minute of flow has been spent. The
   measured correlation of flow delta with outcome is −0.01.

4. **Clock-time flow windows ignore the Volume Clock paper in the library.**
   Easley, López de Prado and O'Hara argue for sampling by volume, not time,
   because a minute at 03:00 UTC and a minute at the US open are different
   amounts of information. 62% of delivered alerts fired before 07:00
   UTC, in thin hours, on windows of a few tens of thousands of dollars. A
   38% buy delta on $6,000 of trades is noise, and the confirmation rule cannot
   tell.

5. **The reversal families fade moves the library says persist.** *Liquidity
   Cascades* (Newfound) describes forced flow reinforcing a move. 94% of
   delivered alerts were liquidity sweeps or range rejections, which bet on the
   opposite. External evidence on crypto is mixed by horizon and venue: intraday
   time-series momentum is reported for Bitcoin ([Shen, Urquhart and Wang](https://centaur.reading.ac.uk/100181)),
   reversal in other samples ([short-term reversal and liquidity in crypto](https://ideas.repec.org/a/eee/finana/v78y2021ics1057521921002349.html)).
   That disagreement is the point: which one holds here is an empirical
   question, and the system assumed an answer.

6. **Stops are inside the noise, and the stop paper was deferred.** Graziano's
   *Optimal Trading Stops* is in the library and listed under "deferred". The
   median delivered stop was 110 bps, about 0.9 of one hour's ATR, on trades
   held one to four hours. With zero drift, any stop and target give the same
   expectancy, which is what the barrier tests show. Stop geometry only matters
   once an entry has drift.

7. **Costs dominate intraday and not swing.** Harris (*Trading and Exchanges*)
   is cited for costs. 15 bps round trip is 0.19R of the median intraday
   structure stop (80 bps) and 0.08R of the median swing stop (197 bps). An
   intraday rule needs about 60% at 1:1 just to cover costs. Swing needs 54%.

8. **The score has no feedback loop.** Nothing in the scoring path reads an
   outcome. Eleven versions were shipped and each was "unvalidated" by its own
   documentation. A score that is never compared with results cannot improve.

## Are the metrics themselves wrong?

Mostly they are reasonable measurements pointed at the wrong question.

| Metric | Verdict |
|---|---|
| Net reward:risk ≥ 2 gate | Sound as cost control. It forces ~3R targets that are reached 6% of the time intraday, so 57% of alerts expire. A reward:risk number is not an edge. |
| Location (profile / range position) | Reasonable concept. Measured relationship is zero to negative. Unproven. |
| Mechanism (flow, absorption) | Right idea, wrong horizon and sample size. See 3 and 4. |
| Stop / noise ratio, target reachability | Good diagnostics. They describe the plan, not the direction. |
| Regime, derivatives, fundamentals, cross-market | No measurable relationship. Fundamentals scored zero for most alerts. |
| Evidence-confidence cap and weakest-dimension penalty | Sensible if the dimensions were predictive. They combine non-predictive inputs conservatively. |
| Grade bands SSS to F | Fine as a presentation. No S or better was ever produced, so the top of the scale is unused, and the bands map to no measured difference except below C being worst. |

## What to change, in order

1. **Send swing only until intraday earns its place.** It is the one horizon at
   or above break-even on both the year-long structure test and the delivered
   sample. This cuts alert volume by about 85% and removes the segment that
   measurably loses. One setting. (Not applied here: it changes what you
   receive, so it is your call.)

2. **Make the barrier test the acceptance gate for every rule.** A family or
   policy is delivered only if "+1R before −1R" clears 50% with z > 2 and the
   net result is positive on a year of candles *and* on the most recent quarter
   held out. `analysis/backtest/run.py` does this today. It should run in CI as
   a report, and before any new scoring version.

3. **Search for an entry with drift before touching the score.** Concretely,
   using the same harness: swing-horizon versions of each family; continuation
   instead of fade after a sweep (the mirror of the current rule, since the
   delivered fade lost at 42.8%); trend pullback only with the daily trend;
   breakouts with a volume-clock participation filter. Each is a hypothesis.
   Most will fail. The harness makes failing cheap.

4. **Keep the grade, change where it comes from.** Keep SSS to F as the
   interface you designed. Replace the hand weights with a small, regularised
   model fitted on the backtest's thousands of labelled candidates (a logistic
   regression on five to eight inputs), validated walk-forward, and define the
   grade from its calibrated expected R: for example S = lower confidence bound
   above +0.3R, A above +0.15R, C about zero, below C negative. Then "A" means
   something that can be checked every month with `bybit-flow scorecard`.

5. **Fix flow measurement or drop it from the score.** If kept: volume-clock
   windows with a minimum notional (for example 0.5% of the symbol's daily
   turnover), measured at the moment of delivery, and used only as a veto for
   contradicting flow. As a positive vote on a multi-hour trade it has no
   support in the data or in the microstructure literature supplied.

6. **Match the target to what price does.** Only after an entry shows drift:
   set the target and hold from the measured excursion distribution of that
   rule (its median favourable move), not from a fixed 3R projection. For the
   current intraday rules this cannot help, as shown.

7. **Run it when the market is liquid, or weight for it.** The record is
   concentrated in Asian hours because that is when the host was on. Either run
   on an always-on host or treat the thin-hours record as its own segment.

## What not to do

- Do not retune the current weights on the 372 delivered alerts. Forty inputs
  and 367 outcomes will always yield a combination that looks good in sample.
- Do not add another scoring or gating version on the intraday rules.
- Do not turn ML back on to rescue the score. A model given inputs unrelated to
  the outcome learns to abstain, which is what it did.
- Do not read the swing result as proven. +1.5 standard errors at 1:1 and
  break-even after costs is a lead to test, not an edge to size up.

## Reproduce

```
python analysis/audit/feature_stats.py      # measurement 1
python analysis/backtest/download.py        # one year of 5m candles, 12 symbols
python analysis/backtest/run.py             # measurement 2 (about 15 minutes)
python analysis/backtest/report.py
```
