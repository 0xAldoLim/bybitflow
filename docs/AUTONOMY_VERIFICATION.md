# Autonomous operation verification — 20 September 2026

This report distinguishes verified operation from research claims. It accompanies the main-branch commit containing this file; use `git log -1 --format=%H -- docs/AUTONOMY_VERIFICATION.md` to identify it. No profitability or validated SSS claim is made.

## Implementation and checks

- **Files changed:** scanner, recorder/storage/retention/packing, leases, supervision, reconciliation, calendar, discovery, identity/notification presentation, scoring/fundamentals/levels, ML labeling/research, dashboard APIs, Docker networking, documentation and regression tests. The unrelated `desk-last45m.txt` log is excluded.
- **Tests:** 239 passed in the full suite, plus 28 affected tests passed after the final additive lookup-index change (including one new query-plan regression); four dependency deprecation warnings in the full suite. Includes incremental restart equivalence, protected-range compaction, 10 GB pressure thresholds, recorder retry/recovery, downtime stop/target ambiguity, calendar/DST, duplicate claims and frozen lifecycle behavior.
- **Lint:** Ruff, JavaScript syntax and final Git whitespace checks passed.
- **Docker:** desk and trainer images built and deployed without deleting volumes. DNS receives reserved address `.2`, desk `.3`, trainer `.4`. A trainer/DNS address collision was repaired. TLS verification remains enabled.
- **Load test:** isolated volume, 2 CPUs, 4 GB RAM, 10 GB configured budget; 33,000 events written in 32.751 seconds, approximately 1,008 events/second. Queue high-water 3,000/10,000; zero drops or overflows. This is a short controlled test, not an unlimited-load or full-budget soak test.

## Data continuity

A complete 5,669,015,552-byte SQLite backup is stored in Docker volume `bybitflow-backup-20260920`, file `/backup/before.sqlite`. This metadata backup does not include every recording/model file; those remain in the original data volume. Interrupted Windows-share copies are not valid recovery backups.

The baseline had zero active setups. Comparison after deployment found **zero missing or changed rows** among 26,614 terminal signals, 26,614 original plans, 47,771 frozen snapshots, 9,493 primary labels and 7,219 late labels. New rows are additive. Three plans created before an intermediate restart retained every checked original price, version, horizon and deadline; they subsequently expired. Before the final restart, 20 active plans were captured; all 20 remained present with unchanged levels, versions and deadlines (nine still pending, ten expired, one invalidated by lifecycle monitoring). Notification clustering does not modify lifecycle plans.

## Self-maintenance and downtime

- **Supervisor/recovery:** independent task retries with bounded backoff; recorder subprocess replacement after failure/stall; bounded queue and explicit coverage gaps. Native subscriptions wait for persistence recovery and reconnect automatically. Late research and large dashboard reports run away from the live socket event loop.
- **Storage:** at 70% usage, compact eligible segments losslessly; at 85%, also prune verified eligible evidence; at 95%, report emergency pressure. Protected evidence is never deleted merely to meet a budget. Retention opt-out is respected.
- **ML replay with compaction:** short leases cover the reader's current time range. Non-overlapping segments may compact concurrently. Compaction/pruning acquire exclusive range leases before mutation.
- **Stale leases:** heartbeat every 30 seconds, expiry after 180 seconds. Expired lease metadata is archived and the expired row is removed. Healthy leases are retained.
- **Downtime reconciliation:** read original-venue closed one-minute candles from the last trustworthy point, keeping the original stop, targets and deadlines. Missing candles keep monitoring paused. A stop/target in the same candle resolves conservatively to stop with an ambiguity marker. No DOM, tick ordering or account fills are invented.
- **Stop-during-downtime test:** passed; one terminal update is published and subsequent evaluation cannot send a resume notice. Resume requires completed historical catch-up and fresh live coverage.

## Calendar

Forex Factory's structured weekly export is the provider. Only high-impact USD events scheduled on weekdays from 08:00 through 17:00 New York time qualify; DST is handled by the timezone database. New entries pause 30 minutes before through 30 minutes after an event; overlapping windows merge. Existing setup monitoring continues. Candidates deferred by news must obtain fresh confirmation afterward.

The provider was HEALTHY during verification. No next qualifying event was present in the current weekly export; this does not establish that next week has no events. Refresh runs every 15 minutes. A fresh cache can bridge provider failure; without one, the calendar reports DEGRADED and fails open. Currency, impact, session, DST and overlapping-window tests passed.

## Storage evidence

The live `.env` retains its existing **30 GB** setting; it was not changed. Controlled tests used **10 GB**. At the recorded after-deployment sample:

| Measure | Value |
|---|---:|
| Total before | 20,237,759,496 bytes |
| Total after | 20,617,486,429 bytes |
| Raw before / after | 14,256,794,718 / 14,541,000,505 bytes |
| Packed | 239,254,489 bytes |
| Permanent after | 5,837,231,435 bytes |
| Usage | 68.72% of the live 30 GB budget |
| Active / stale leases | 1 / 0 |
| Maintenance state | NORMAL |
| Newly pruned in this verification | 0 bytes |
| Historical cumulative freed | 15,332,231,164 bytes |
| Scheduled deletable below pruning threshold | 0 bytes |
| Protected-byte inventory | Not fully measured at this pressure; reported as null |

Live storage grows during collection. The historical freed total is not savings from this deployment. The complete backup is outside the managed recording budget. A finite budget cannot indefinitely hold growing permanent evidence; the recorder reports backpressure rather than silently discard it.

## Scanner and plan semantics

A completed broad cycle discovered **528** markets, prequalified **73**, verified **30** with real depth, and selected **30** deep feeds using 30 REST depth requests. Selection classifications were ACTIVE 6, CORE 17, EXPLOIT 6, EXPLORE 1. Best-horizon priorities: SHORT 28, CORE 18, SWING 27. Discovery priority is separate from the signal quality score.

| Profile | Context / setup / execution | Expected holding range |
|---|---|---|
| SHORT_INTRADAY | 1H / 15M / 5M | 15 minutes–2 hours |
| CORE_INTRADAY | 4H / 1H / 15M | 1–4 hours |
| SWING | 1D / 4H / 1H | 4–48 hours |
| EXTENDED_SWING | 1D / 4H / 1H | 2–7 days; research only |

Entry validity and holding deadline are separate, frozen fields. Candle-derived features use their actual semantic timeframe. Older plans retain their original policy.

## Quality, confirmation and levels

**Quality score:** deterministic 0–100, profile `native-evidence-3`. Weights are regime 15, structure 20, order flow 25, derivatives 10, execution 15, fundamentals 10 and cross-market 5. Missing evidence earns no credit and weights are not redistributed. It is not a win probability. ML is a separate ranking/calibration research layer.

**Fundamentals:** source coverage and sourced quality/risk assessment are separate; unassessed facts do not automatically earn favorable points. **Cross-market:** causal beta-adjusted residuals, correlation and stability constrain factor credit; an asset is not its own factor. **BTC/ETH alignment:** disagreement is neutral/uncertain, not automatic conviction. A strong stable BTC conflict requires idiosyncratic residual strength and directional executed participation before a contrarian intraday entry.

**Order flow:** complete recorded execution windows, directional delta/CVD, persistence and family confirmation remain required. A sweep alone is not sufficient participation. **Derivatives:** OI/funding and sampled liquidation limitations remain explicit. **Execution:** spreads, visible depth, cost-adjusted reward/risk and freshness still gate entry. Lower research grades may send when those conditions pass; no alert is fabricated merely to meet a frequency target.

**TP/SL:** new candidates prefer confirmed opposing structural swings/range objectives known at decision time, with at least the policy's required risk multiple. Missing suitable objectives use documented 3R/4R projections. Profile evidence is marked unavailable when not observed; none is invented. Stops retain structural invalidation plus the existing 0.15 setup-ATR buffer. Existing plans are not widened or rewritten.

**Thesis health:** new policy versions require persistent adverse executed flow, book evidence and structural failure together before withdrawal. Legacy plans are shadow-monitored. These are versioned deterministic rules, not statistically validated improvements. Post-withdrawal outcomes are tracked separately.

## ML and policy research

Primary learning uses immutable decision-time snapshots and cost-adjusted recorded-print outcomes. Incomplete coverage cannot become a successful verified label. Repeated evaluations have explicit technical-duplicate exclusions; historical labels remain unchanged. Restart cursors retain unresolved paper positions. Sorting uses small IDs rather than spilling full payloads into the container's limited temporary filesystem.

The configured two-stage family is LightGBM, Random Forest and LSTM in stage one, then logistic regression, SVM and Random Forest probability in stage two. Training checks data readiness automatically. Chronological partitions, outcome purging, embargo and reserved holdouts remain required. **No champion or validated model exists at this checkpoint**; readiness last reported **5/500 complete outcomes with 16-observation sequences**. More stored events do not equal more independent complete outcomes.

Late/horizon ML uses separate coverage-complete post-terminal labels and frozen inputs. It cannot retroactively relabel an expiry or move an existing plan. The final report contains 2,057 independent dataset samples, of which 1,283 have a supported horizon target. Although the count exceeds 500, chronological class coverage after purging and a one-day embargo is insufficient. Status is INSUFFICIENT_EVIDENCE with no horizon recommendation.

The completed score-profile report has nine eligible samples out of the required 500, so no challenger was fitted or promoted. Score-profile research tests a small constrained set of weight proposals, reserves a holdout and reports descriptive cost-adjusted results. It does not edit live weights or auto-promote. Paired uncertainty intervals and broad cohort-stability validation remain outstanding; a shadow result must not be described as validated.

**Swing stop study:** zero eligible Swing samples were available. The holdout result is absent. Available aggregate post-terminal paths cannot establish an unambiguous counterfactual stop-policy holdout. Wider structural/noise buffers remain proposed challengers, not deployed winners. A full path-based counterfactual grid has not been validated. **Current Swing stop policy retained.** Directional recovery after expiry is recorded separately from whether the original stop/target path was successful; late recovery alone does not prove a tight-stop defect.

## Discord and duplicate evidence

The observed HBAR repetition involved different setup families sharing similar prices and a separate Swing plan. The old same-thesis comparison required equal families; it therefore allowed multiple full cards. Evidence did not establish a concurrent same-family HTTP race.

`alert-fingerprint-v1` now claims an economic plan atomically in SQLite before initial HTTP delivery. Technical duplicates are suppressed; genuinely distinct horizons retain independent lifecycle/research records and may appear as compact confirming cards. Stronger displayed theses do not replace existing setup monitoring. Opposite directions are identified as conflicts. Lifecycle cards retain a compact update and original-plan reference; the verbose research footer was removed.

The real Discord connection test and clearly marked synthetic signal both returned **sent**. The synthetic test created **zero signal or ML rows**. At the recorded delivery checkpoint, cumulative genuine initial sends were **87**, exact duplicate suppressions **716**, related cluster updates **22**, uncertain/rejected **0**. Last recorded real delivery: LITUSDT CORE_INTRADAY, signal `63c9306e186acd11eb1e4411`, message `1550360916636471319`. These are historical totals, not 87 new signals from this deployment. No new genuine signal had been verified at that checkpoint.

**Validated SSS:** none. Operational tests and research quality grades do not establish trading performance.

## Operator actions and limits

From Command Prompt:

```cmd
cd /d C:\path\to\bybitflow
docker compose --profile ml up -d
docker compose exec desk bybit-flow doctor
```

Leave Docker running and Windows awake for continuous collection. Closing the browser or terminal does not stop the services. To stop while retaining data:

```cmd
docker compose --profile ml stop
```

Use the same `up -d` command to start again. Do not use `down -v`. Monitor the dashboard, doctor and Discord. External exchange outages, Windows sleep/clock changes and insufficient independent research samples remain real limitations. Signals have no guaranteed arrival interval; fresh confirmation and entry/risk conditions must pass.
