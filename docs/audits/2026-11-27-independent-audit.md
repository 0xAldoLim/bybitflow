# Independent audit, 2026-11-27

Scope: why the system barely runs, why most delivered trades stop out, and why
the ML consumes space without working. Every number below was measured from the
production database, the host, or public Binance candles on the audit date. The
scripts are in `analysis/audit/` and can be re-run.

This audit deliberately disagrees with several positions in the existing
documentation. Where it does, the evidence is stated.

## Verdict in four lines

1. **Uptime.** The system was down for two separate reasons that the existing
   health tooling could not repair: Docker Desktop crashing at startup, and a
   storage budget deadlock. Both are fixed in this change.
2. **Trades.** The delivered alerts have no measurable edge. Entries behave like
   coin flips, slightly worse. Stops are not misplaced; a 3R target on a coin-flip
   entry stops out about three times in four by arithmetic. Not fixable by tuning.
3. **Score.** The Setup Score does not rank outcomes. Higher grades did not do
   better. It also never gated delivery: grades C, D, E and F were all sent.
4. **ML.** 2.5 months of recording produced 128 usable primary labels, 10 of them
   wins. No model can learn from that. The models made 127 predictions and
   accepted none.

## 1. Why it barely runs

### 1a. Docker Desktop startup crash (host)

Docker Desktop's backend log on the audit date:

```
backend crashed: initializing Inference manager: listening on
unix://...\Docker\run\dockerInference: remove ...: The file cannot be accessed by the system.
```

After an unclean shutdown, AF_UNIX socket files remain in
`%LOCALAPPDATA%\Docker\run` and `%LOCALAPPDATA%\docker-secrets-engine`. On this
Windows build they can be neither opened nor deleted, so Docker Desktop crashes
on every start until the folder is renamed. The host holds **fourteen** leftover
`run.stale-*` / `run-backup-*` folders dated 2026-09-11 through 2026-11-27: this
has been repaired by hand at least thirteen times before.

Consequence, from `signals` created per day: nothing from 2026-11-08 to
2026-11-16, and nothing from 2026-11-21 to the audit. The stack was not degraded;
it did not exist.

**Fix:** `scripts/start.ps1` now detects stale socket folders, moves them aside
and retries Docker Desktop start (up to four rounds, since each crash can leave a
new one). The existing login task `BybitFlow-StartExisting` runs this script, so
the repair is automatic at login.

**Not fixed, and the real recommendation:** Docker Desktop on a Windows Insider
build is the least reliable component of this system. The application is a
single Python process plus SQLite. Running it natively (or on a small always-on
Linux box) removes this entire failure class. That is a migration of the data
volume and was not done here.

### 1b. Storage deadlock (application)

State found on restart:

```
scanner:  {"state": "blocked_by_storage", "reason": "Waiting for recording storage to become available"}
storage:  used 39.99 GB of 40.00 GB budget
cleanup:  CLEANUP_VERIFICATION_BLOCKED, "Failed run predates durable frozen row fingerprints",
          recoverability AUDIT_UNAVAILABLE
original failure: "Cleanup ownership expired/lost; stopped"
```

Chain of events:

1. A cleanup run lost its claim, most likely a restart or sleep mid-run. That is
   an interruption. No integrity comparison failed.
2. The failure latched. The reconciler demands a pre-run audit file with row
   fingerprints. This run has none, so it can never be reconciled. All cleanup is
   blocked permanently, with 7.9 GB of raw recordings it had itself classified as
   deletable.
3. Storage reached the budget. The recorder went unhealthy.
4. The scanner refuses to scan when the recorder is unhealthy, and the feed
   handlers refuse to "collect unrecorded data".

So a research archive for an ML that has never accepted a prediction was able to
switch off the product. The live path does not need the recording: books, tapes,
confirmation coverage and lifecycle all run from memory and public candles
(`ml/coverage.py` describes recorded coverage as "diagnostic only").

**Fixes:**

- `FLOW_RECORDING_REQUIRED` (default `false`). Raw recording is now best effort.
  A full budget, queue overflow or writer error drops the affected raw rows as
  explicit, durable coverage gaps and the scanner keeps running. The recorder
  reports `STORAGE_FULL_NOT_RECORDING`, retries by itself every 60 s, and the
  dashboard shows a warning instead of an outage. Set it to `true` for the old
  fail-closed behaviour.
- An interrupted cleanup (`Cleanup ownership expired/lost`) is now
  `SAFE_TO_RETRY`. The next run takes and verifies its own fresh pre/post audit,
  which is the protection that matters. A failure that reports an actual
  integrity mismatch still blocks, as before.

This overrides the earlier engineering rule that a failed broad cleanup "stays
blocked without its original frozen proof". For an interruption, the proof cannot
exist, and the block protects nothing while it stops the product.

### 1c. Observability gap

Scanner failures are logged as `{"event": "scan_failed", "error_type":
"RuntimeError"}` with no message. The cause of an outage was not recoverable from
logs. Not changed here; worth fixing.

## 2. Why most trades are stopped

### 2a. Lifecycle record of everything delivered

372 alerts were delivered to Discord between 2026-09-13 and 2026-11-20.

| Outcome | Count |
|---|---:|
| Target reached | 14 |
| Stop reached | 135 |
| Expired, neither | 211 |
| Invalidated otherwise / open | 12 |

Of resolved alerts, 9.4% reached target. The median plan asks for 3.2R, which
needs about 24% to break even before costs.

By grade (target / stop / expired):

| Grade | Alerts | Target | Stop | Expired | Target rate of resolved |
|---|---:|---:|---:|---:|---:|
| A (75+) | 17 | 0 | 1 | 14 | 0% |
| B (65+) | 151 | 6 | 60 | 81 | 9% |
| C (50+) | 123 | 5 | 45 | 71 | 10% |
| D (35+) | 61 | 3 | 22 | 32 | 12% |
| E (20+) | 8 | 0 | 4 | 4 | 0% |
| F | 12 | 0 | 3 | 9 | 0% |

Median score of alerts that stopped: 63.7. That reached target: 62.1. That
expired: 63.4. The score did not separate them. No S, SS or SSS setup was ever
produced.

### 2b. What price actually did after each alert

For 367 of the 372 alerts, public Binance 1m candles were fetched from the alert
time (`analysis/audit/fetch_paths.py`, `path_stats.py`). Entry is the next
minute's open, which is what a person acting on a Discord message gets. R is the
distance to the published stop. Same-bar ties count as the stop.

Barrier test, no time limit:

| Test | Observed | Random walk | z |
|---|---:|---:|---:|
| +1R before −1R | 42.8% | 50.0% | −2.8 |
| +1.5R before −1R | 36.8% | 40.0% | −1.3 |
| +2R before −1R | 29.7% | 33.3% | −1.5 |
| +3R before −1R | 23.0% | 25.0% | −0.9 |
| +1R before −2R (stop twice as wide) | 65.0% | 66.7% | −0.7 |

Signed return in the alert's direction, no stop or target:

| After | Mean | 95% half-width | Right direction |
|---|---:|---:|---:|
| 15 minutes | −3.6 bps | 4.5 | 49% |
| 1 hour | −14.5 bps | 10.4 | 47% |
| 4 hours | −11.5 bps | 16.3 | 48% |
| 24 hours | −25.6 bps | 54.1 | 50% |

Every exit geometry tested loses 0.23R to 0.32R per alert after the project's own
fee and slippage assumptions (15 bps round trip, about 0.14R of the median 110 bps
stop). Wider stops, closer targets and no time limit all give the same answer,
because the entry carries no information.

Reading:

- "Most trades are stopped" is not a stop-placement bug. With a 3.2R target and a
  coin-flip entry, roughly 76% stop out. Observed without a deadline: 286 stops to
  76 targets, 79%.
- 57% expired because a 3R target is rarely reached inside a 1 to 4 hour window.
  Only 7% of alerts travelled 3R in their favour before the stop or deadline.
- The +1R/−1R result is significantly *below* 50%. The dominant families
  (liquidity sweep and range rejection, 94% of alerts) fade moves that more often
  continue. This is not strong enough to trade in reverse: the inverse wins about
  57% at 1:1, which the 0.14R cost consumes.
- Entry slippage from the delay is small (median 0.01R). Latency is not the cause.

By segment (+1R before −1R): grade A 41% (n=17), B 48% (n=151), C 41% (n=120),
D 40% (n=60), E 13% (n=8), F 27% (n=11). Score below 50: 35% (n=79, z=−2.6).
Swing horizon: 57% (n=54, z=+1.1, not significant). Short intraday: 36% (n=61,
z=−2.2). Nothing clears 50% with a meaningful sample. The one usable signal in the
score is at the bottom: alerts graded below C were measurably worse than the rest.

### 2c. The wider candidate population

36,948 ML labels exist. For the two candle-proxy label sets (3,804 and 2,465
resolved candidates) mean net R is −0.25 and −0.32. Spearman correlation of score
with net R: +0.16 and −0.03. In the first set the 35 to 50 band was positive and
the 75 to 85 band was the worst; the score is not monotonic in outcome anywhere.
Tick-accurate labels are harsher: 7.8% and 14.6% wins, mean about −1R. The candle
proxy flatters the strategy relative to real prints.

### 2d. What was changed, and what was not

- **Changed:** `FLOW_MIN_ALERT_TIER` (default `C`). Delivery now follows the
  grade, which the design always implied and the code never enforced: with
  `FLOW_RESEARCH_ALERTS=true`, every gate-passing setup was sent regardless of
  score, including 81 alerts graded D, E or F. Lower grades stay on the dashboard.
  Set `F` for the old behaviour.
- **Changed:** `bybit-flow scorecard` prints the table in 2a from the live
  database, with a Wilson interval and a break-even comparison per grade, so this
  evidence is one command away instead of an audit away.
- **Not changed:** score weights, gates, stops, targets, families. The data does
  not support any specific retune, and fitting 372 alerts to find one would be
  curve fitting. The grade gate removes the band that was measurably worst
  (below 50: 35%). What remains is still below 50%, so the gate cuts noise and
  losses; it does not create an edge.

### 2e. What would actually improve trade quality

In order of expected value:

1. Stop treating delivery as validation. Use the scorecard and
   `analysis/audit/path_stats.py` as the acceptance test for any change: a
   variant earns Discord delivery only after its +1R/−1R rate is above 50% with
   z > 2 on at least 200 forward alerts.
2. Test the entry hypothesis directly before adding more scoring layers. Eleven
   scoring and gating versions were stacked on four entry rules that were never
   shown to predict direction. The cheapest experiment is the barrier test on
   every *generated* candidate per family, from public candles, with no recording.
3. Either trade the reversal families only at swing horizon (the one segment not
   below 50%) or drop them and test continuation setups. Both are hypotheses.
4. Shorten targets or lengthen holds so that plans can resolve: 57% expiring
   means more than half of all alerts never produce a learning signal.

## 3. Why the ML only eats space

| Item | Measured |
|---|---:|
| Data volume | 38.4 GB (23 GB SQLite, 12 GB raw segments, 3.4 GB ML files) |
| `ml_snapshots` | 12.0 GB for 150,491 rows |
| One decision snapshot | about 170 KB, of which feature values are 8.4 KB |
| Remainder of each snapshot | 61 KB of feature descriptions and 100 KB signal copy, repeated per row |
| `signals` + `signal_origins` | 7.6 GB for 121,788 signals, of which 372 were delivered |
| Raw recordings written and deleted to date | 298 GB |
| Primary (tick) labels | 28,516: 18,473 incomplete, 9,915 duplicates, **128 usable, 10 wins** |
| Models trained | 4 |
| Predictions made | 127, accepted 0 |

Why it cannot work as built:

- The primary label needs gap-free recorded ticks from decision to outcome. On a
  desktop host that sleeps and restarts, 65% of labels are incomplete. More
  recording does not fix this; it is a property of the host.
- 95% of the main table is the same descriptive text stored 150,000 times.
- The features describe setups whose direction is a coin flip (section 2). A
  model can only learn to say "no", which is what it did.

**Changed here:** recording can no longer stop the product (1b), and the latch
fix lets the existing retention reclaim the 7.9 GB it had already classified as
deletable.

**Recommended, not done because it deletes or rewrites your data:**

1. Stop the trainer and set `FLOW_ML_ENABLED=false` until section 2 shows an
   entry edge. There is nothing for it to learn yet.
2. Rewrite `ml_snapshots` to keep `values`, the identifiers and a hash of the
   metadata, with metadata stored once. Expected to take 12 GB to under 1 GB.
3. Prune `signals` / `signal_origins` payloads of never-delivered, terminal
   candidates older than 30 days. Expected to free most of 7.6 GB.
4. Label from public 1m candles (already implemented as the monitored track)
   instead of raw ticks. It is free, complete across downtime, and is what
   produced 6,269 resolved labels against 128.

## 4. Process criticism

The repository contains 37,585 lines of application Python, 102 test files and
about 35 documents. In the audited period the product was offline for roughly a
third of the time, delivered alerts with no edge, and ran a model that never
acted. The engineering effort went into provenance, frozen evidence, audit
latches and versioned policy layers. None of it answered the only product
question: do the alerts make money. That question was answerable from 372 rows
and public candles in an afternoon.

The existing contract says a lack of alerts is not a reason to weaken gates. The
mirror rule was missing: a lack of edge is a reason to stop building on top of
the signal.

## Follow-up, same day

Approved and done after this audit: the trainer was stopped and ML disabled in
the local configuration; `storage slim` removed 121,821 unread generation
snapshots and 121,418 unread origin copies; see the
[scoring review](2026-11-27-scoring-review.md) for the year-long entry test.

## Reproduce

```
python analysis/audit/dump_alerted.py   # inside the desk container, stdout to alerted.json
python analysis/audit/outcomes.py       # section 2a
python analysis/audit/fetch_paths.py    # public Binance candles
python analysis/audit/path_stats.py     # section 2b
python analysis/audit/label_stats.py    # section 2c
```

The exported data files contain full signal payloads and are git-ignored.
