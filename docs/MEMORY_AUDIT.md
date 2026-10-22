# Memory audit: 30-market deployment

Audit date: 22 October 2026. Starting source: `d1c1fd7329ba1775142d2ec773e7e37c597ecb52`.

**SIGNIFICANT_SAFE_SAVINGS_FOUND.** Exact, bounded sharing of repeated immutable
trade Decimals and a smaller duplicate-ID index reduced controlled 30-market
post-GC RSS by **371.7 MiB (32.4%)**, without deleting trades. The workload
retained 1,500,000 trades before and after and had an identical semantic hash.
This is an offline result, not a measured reduction in the running deployment.

**No live deployment was performed. Live RSS removed by this patch: 0 MiB.**
The desk, trainer and DNS start times were unchanged. No host settings, market
configuration, stored evidence, models, lifecycle rules or delivery rules were
changed.

## A. Repository and measurement scope

- Branch: `main`; expected starting HEAD matched exactly.
- Existing working-tree item: untracked `desk-last45m.txt`, excluded from this work.
- Ending source: the commit containing this report; identify it with `git log -1`.
- App changes: `models.py`, `exchanges.py`, `streams.py` and `orderflow.py`.
- Regression coverage: `tests/test_trade_memory.py` and an archive-streaming
  regression in `tests/test_print_recovery.py`.
- Measurement tool: `examples/memory_benchmark.py`, an explicitly offline CLI.
- Verification: `python scripts/verify.py` passed: **793 passed, 4 skipped**;
  Ruff lint/format, JavaScript syntax and diff checks also passed. Three existing
  dependency deprecation/future warnings remain.

The actual running settings were read, rather than assuming the repository
default of eight markets: 30 deep markets, 50,000 trades per Tape, automatic
venue selection with Binance active, 60-second flow windows, 50 configured
book depth, 60-second spread buckets over 30 minutes, 10,000 queued records,
32,000,000 queue bytes and a 30 GB application-data budget. The existing
23-symbol core watchlist and all active-lifecycle requirements were preserved.
The actual selected symbols rotated normally during the audit; configuration
and selection logic did not change. Native Binance collection still requests
1,000 levels per side and retains its existing 6,000-level combined safeguard.

Measurements have three different meanings:

1. **Live OS measurements:** read-only `/proc` and cgroup statistics, Windows
   counters, container identity/start time and bounded SQLite primary-key reads.
2. **Controlled object graphs:** Python-owned representations in an isolated
   workload, counted with shared references counted once. These are not a live
   heap census.
3. **Controlled process measurements:** RSS/PSS/anonymous/file memory in fresh
   offline Linux processes running the old and new representations.

No debugger, live heap tracer, signal, GC request or injected instrumentation
was attached to production. Exact live Tape occupancy, exact oldest retained
trade timestamps, native-library allocation ownership and per-cache live byte
counts are therefore **not available**. Existing feed diagnostics expose a
continuity boundary, not exact Tape population. There is no claim that every
live Tape was full.

The recent wire sample read only one committed gzip segment: 3,052,939 decoded
bytes, 362 public prints, 26 symbols, 201 distinct price strings and 247 size
strings. A short repeated-token sample does not establish an exchange-wide
price/size distribution. It supports this controlled case, not a guaranteed
production saving.

Offline containers had no network, no live data volume, a read-only repository
mount, a disposable 256 MiB `/tmp`, half a CPU and bounded memory. No full
live historical replay or training was run.

## B. Actual runtime memory

Initial live snapshot, approximately 11:49 MYT. MiB and GiB below are binary;
database and configured byte budgets are also given in bytes where relevant.

| Process | RSS | PSS | Anonymous RSS | File RSS | Swap |
|---|---:|---:|---:|---:|---:|
| Desk main Python | 1,275.8 MiB | 1,267.8 MiB | 1,264.0 MiB | 11.8 MiB | 362.3 MiB |
| Desk writer subprocess | 110.7 MiB | 101.7 MiB | 96.1 MiB | 14.6 MiB | 13.0 MiB |
| Trainer Python | 196.3 MiB | 187.6 MiB | 183.3 MiB | 13.1 MiB | 14.1 MiB |

Desk shared-memory RSS was effectively zero. PSS apportions shared pages; it is
not an additional allocation to add to RSS. The later 12:34 MYT snapshot of the
same deployment was desk RSS 1,180.7 MiB/PSS 1,172.5 MiB, anonymous 1,169.4 MiB,
file 11.2 MiB, swap 503.7 MiB; trainer RSS 354.1 MiB/PSS 345.0 MiB, anonymous
341.1 MiB, file 13.0 MiB, swap 52.3 MiB. These natural changes include paging
and worker activity, not deployment of the patch. Falling RSS while swap rises
is not evidence of reclaimed application state.

| Initial cgroup measurement | Desk | Trainer |
|---|---:|---:|
| `memory.current` | 4,095.7 MiB | 2,047.9 MiB |
| Limit | 4,096 MiB | 2,048 MiB |
| Anonymous | 1,368.5 MiB | 192.3 MiB |
| File | 2,667.4 MiB | 1,833.5 MiB |
| Kernel | 40.2 MiB | 20.6 MiB |
| Socket | 0.09 MiB | negligible |
| Inactive file | 400.4 MiB | 533.0 MiB |
| Active file | 2,267.1 MiB | 1,300.3 MiB |
| Slab | 34.6 MiB | 19.3 MiB |
| Cgroup swap | 643.6 MiB | 92.2 MiB |
| OOM / OOM kills | 0 / 0 | 0 / 0 |

Both cgroups had many historical `max` events and refaults. Cache is useful and
reclaimable under pressure, but repeated cache eviction can still cost I/O and
latency. No OOM kill was observed.

Initial Linux memory: 7.46 GiB total, 4.99 GiB available, 4.66 GiB cached;
swap 2 GiB total with approximately 1.27 GiB free. Windows: 15.40 GiB physical,
656 MiB available. `vmmemWSL` working set was 5.04 GiB; private committed memory
was 7.63 GiB. Later Windows available memory was about 1.23 GiB and the WSL
working set 6.64 GiB. WSL working set, Linux cache, cgroup charges and process
RSS overlap and must not be added together.

Docker CLI reported approximately 3.63 GiB desk and 1.48 GiB trainer initially.
On Linux its displayed memory subtracts inactive file cache; it does not
subtract all file memory. See [Docker stats](https://docs.docker.com/reference/cli/docker/container/stats/)
and [runtime metrics](https://docs.docker.com/engine/containers/runmetrics/).

## C. Application attribution

The following table is a **controlled, saturated per-market graph**, not live
object attribution. It includes 50,000 independently decoded prints, a
1,000-level book, 5,000 changes, 120 pressure/OFI samples, 120 independently
decoded liquidity frames, 1,000 liquidation records, 600 immutable Candles,
three referencing history slices, derivative context and 31 spread buckets.
Accounting order counts shared IDs in trades before the index. Decimal-cache
ownership is globally bounded; it does not own Trades or Tapes.

| Component | Before per market | Before x30 | After per market |
|---|---:|---:|---:|
| book | 1.9 MiB | 55.9 MiB | 1.9 MiB |
| candles | 0.2 MiB | 4.9 MiB | 0.2 MiB |
| context derivatives | 0.1 MiB | 2.2 MiB | 0.1 MiB |
| event features | 0.3 MiB | 9.6 MiB | 0.3 MiB |
| frames | 2.6 MiB | 79.0 MiB | 2.6 MiB |
| id index | 4.4 MiB | 130.8 MiB | 1.8 MiB |
| liquidations | 0.2 MiB | 6.4 MiB | 0.2 MiB |
| spread history | 0.0 MiB | 0.2 MiB | 0.0 MiB |
| tape trades | 23.2 MiB | 695.3 MiB | 13.3 MiB |

The controlled graph total was 32.81 -> 20.38 MiB per market. It is deliberately
not an exact prediction for every market: active level counts, liquidation
activity, frame shape, history lengths and wire-token repetition differ.

| Remaining component | Approximate RAM / evidence | Confidence |
|---|---|---|
| Scanner feature cache | 473 entries observed, capped at 512; exact live bytes not exposed | High count, unknown bytes |
| Derivative cache | 134 entries observed, capped at 300; per-market fixture above | High count, controlled bytes |
| Instrument/context/candle caches | Source audit below; live object bytes not measured | Limited |
| Signals/research/V8 | Primarily durable rows and bounded working views; active plans 0 initially, 1 later | High storage distinction; no heap census |
| ML readiness | 28.10 MiB traced peak for 30,000 unique outcomes across three sources | Controlled |
| Print recovery | 1.04 MiB traced peak across a 12-page candidate | Controlled |
| Recorder | Writer RSS 110.7 MiB initially; queue capped by 10,000 records/32 MB pending bytes | Live process plus configured bounds |
| SQLite/native | Main-process file RSS 11.8/13.1 MiB; cannot apportion all anonymous allocations among SQLite, Python and native libraries | Limited attribution |
| Arrow | Trainer's existing cached release diagnostic reported zero allocated Arrow bytes | Cached measurement, not a continuous peak |

Python live objects, allocator arenas and native allocations all contribute to
anonymous RSS. An exact split would require more invasive instrumentation than
this audit permits. Large memory-mapped database ownership was not observed.
Retained trade graphs are the largest demonstrated application representation.

### Ownership and collection inventory

| Owner / collection | Creator and growth key | Bound / eviction | Consumers and required lifetime |
|---|---|---|---|
| Tape trades / IDs | Feed decoder, accepted trade ID | Configured 50,000; FIFO/reset | All execution windows, profiles, lifecycle, coverage and replay |
| EventFeatures sizes/seconds/rotations | Accepted trade | 512/181/128 deques; reset | Causal event evidence and recent quantiles |
| Book bids/asks | Snapshots and deltas, price level | Native combined-depth safeguard 6,000; reset/removal | Visible liquidity and executable impact; no depth reduction |
| Book changes / pressure / OFI | Book updates and sampled seconds | 5,000/120/120 | Absorption, pressure and OFI evidence |
| Stream books/tapes/liquidations | Selected/required symbol | Symbol removal; liquidation deque 1,000 native/10,000 Bybit | Selected and required lifecycle markets |
| Frames / last sample / sessions | Five-second sample / connection | 120 frames; per-symbol removal | Dashboard plus durable recording; recording every frame preserved |
| Stream tasks / stopping tasks | Symbol selection | Cancel and await removed jobs; bounded by subscriptions | Live feeds and controlled shutdown |
| Native reconnect counts | Reconnection, symbol | No per-symbol eviction | Cumulative per-market diagnostic; small cardinality risk |
| Bybit subscription_changes | Subscription request ID | ACK pop; clear on reconnect | Pending subscription acknowledgements |
| Candle cache / requested limits | Scanner request, symbol + timeframe | Replace at new boundary; clear on source change; no global symbol cap | Closed-candle histories shared by horizons |
| Scanner context | Admitted symbol | Replace on refresh; clear on source change; old symbol keys remain | Generation, derivatives, profile context and active lifecycle |
| Feature cache | Source/symbol/timeframe/boundary | LRU 512; source reset | Return deep copies to prevent caller mutation |
| Derivative cache | Source/symbol/endpoint | FIFO 300 plus freshness check; source reset | Funding/OI snapshots and historical decisions |
| Instrument metadata cache | Broad scan | One current universe, 30-minute freshness; source reset | Parse/rank/admit and metadata isolation |
| Volume/swing profiles | Completed profile, symbol | Latest per symbol; no global symbol cap | Prior value migration comparisons on reselection |
| Volume research attempts | Symbol + warmup | Latest 128 keys | Comparable-session research warmup |
| OI collector cache / oi_series | Current scope, source/symbol | Prune to scope <=10; series deque 30 | Current causal OI and liquidation features |
| V8 cache | Selected scoped symbol | Refresh rebuilds active scope | Current observations attached by causal deep copy |
| Liquidation baseline | Symbol, completed window | 120 per-symbol observations; source reset; no global key cap | Abnormal liquidation comparisons |
| CrossVenue peers/jobs | Venue | At most other two venues; stop closes clients/jobs | Required lifecycle + existing optional coverage |
| CrossVenue contexts/candles | Venue/active symbol/timeframe | Prune against active plans on refresh | Original-venue active setup monitoring |
| Spot requests/errors | Public request | Two deques of 200; pooled client | Rate/error diagnostics |
| SpreadHistory rows | Valid quote bucket | 30-minute window in this configuration; transient object backed by durable KV | Spread maturity and liquidity gates |
| Candidate/active signal lists | Indexed SQL reads per evaluation | Local variables released after call; existing query limits | Independent setup lifecycles |
| Research structures/suppressions | Durable observations | Existing 400/1,000 retained lists; no new daemon list | Frozen structural diagnostics |
| Flow/liquidity/session baselines | Durable KV reads | Existing per-key windows/bounds | Causal percentile/reference observations |
| Recorder queue/batch | Serialized offer / flush | Queue 10,000 and 32 MB pending budget; bounded segment rows | Durable publication, retries and idempotent batch acknowledgement |
| Recorder gaps/rate samples | Failed offers / seconds | Gaps drained with next new batch; rate deques 120 | Integrity and health reporting |
| REST/WS decoded responses | Request/receive | Local/bounded library queues and connection pools | Parse, record, feed update, then release |
| SQLite results | Query call | Current scalar pages 500/1,000 in readiness/recovery | Release each page; no feature-payload history materialization |
| Readiness identity sets | Source + exact candidate identity | Invocation lifetime; grows with all scanned identities | Exact counts; planning rows alone capped at 10,000/source |
| Recovery pending/seen | Current schema opportunity identity | Invocation lifetime; exact set, candidate batch returned <=3 | No probabilistic deduplication |
| Recovery page/consumed | Provider page / rolling digest | <=1,000 trades/page and existing page/deadline budgets | Checkpoint and certificate verification |
| Archive verified map | Daily archive name | One bounded provider batch lifetime | Avoid rehashing already verified files within that batch |
| Trade Decimal cache (new) | Exact finite wire text | LRU 8,192 keys, <=64 characters/key | Share immutable scalar values only |

Durable database tables/KV may grow on disk until their existing retention
policy runs. That does not make every stored row a permanently retained Python
object. No history deletion was used for memory savings.

### Deep-copy classification

| Copy | Classification | Decision |
|---|---|---|
| Scanner feature cache insert/return | REQUIRED_CAUSAL_SNAPSHOT | Mutable feature dictionaries; preserved |
| Derivative cache insert/return | REQUIRED_CAUSAL_SNAPSHOT | Mutable historical values and availability; preserved |
| Instrument cache insert/return | UNKNOWN without immutable replacement proof | P3/limited attribution; preserved |
| Signal volume-profile attachment | REQUIRED_CAUSAL_SNAPSHOT | Later profile updates cannot mutate frozen evidence |
| V8/breadth/evidence attachments | REQUIRED_CAUSAL_SNAPSHOT | Preserve causal decision isolation |
| EventFeatures latest closed second | REQUIRED_CAUSAL_SNAPSHOT | Mutable aggregate must not leak later updates |
| Research baseline signal / legacy frozen plan | REQUIRED_CAUSAL_SNAPSHOT | Independent counterfactual and immutable lifecycle plan |
| Profile Tape -> list of Trades | SAFE_IMMUTABLE_SHARE of frozen Trades, REQUIRED container snapshot | About 0.4 MB at 50,000 references; thread isolation preserved |
| New finite Decimal sharing | SAFE_IMMUTABLE_SHARE | Exact representation; no mutable containers shared |

Candle is already frozen/slotted. Candle-cache and context slices can share
immutable Candle instances while retaining independent lists. This patch does
not share mutable historical containers or expose future bars.

## D. Findings and change assessment

The following assessment preceded the app changes.

**P1: duplicate immutable trade scalars — SAFE_WITH_TESTS**

- Component: price/size Decimal objects retained by Trade/Tape.
- Measured baseline: an early 50,000-trade probe retained 29,254,160 bytes;
  the final component fixture retained 24,302,213 bytes in its trade graph.
- Reason/lifetime: every wire print constructed new Decimals, retained for the
  full Tape and any in-flight snapshots. Consumers include footprint, profiles,
  flow quality, event features, lifecycle and PaperPosition recovery.
- Avoidable portion: separate immutable objects with identical exact wire text.
- Change: cache finite Decimals by exact string, maximum 8,192 entries and
  64-character keys. Non-string/long/non-finite inputs keep direct construction.
  Numerically equal spellings remain distinct, preserving exponent/trailing zeros.
- Estimated saving before implementation: combined prototype about 390 MB across
  30 saturated tapes for the repeated-token sample; not a live guarantee.
- Risk: Decimal precision, traps/flags/errors, multiplier arithmetic, trade order
  and every trade field must remain identical. Focused tests cover these.
- CPU: preliminary paired trials were favorable; final full workload CPU fell
  20.5%. Three paired microtrials showed sample median 1.640 -> 1.736 CPU seconds
  (+5.9%), unique-token median 2.223 -> 2.153 (-3.2%). Microtrial noise/outliers
  prevent claiming universal CPU improvement. No pathological miss-only slowdown
  was demonstrated, and cache ownership remained bounded.

**P2: duplicate FIFO bookkeeping — SAFE_WITH_TESTS**

- Component: Tape.ids, formerly an OrderedDict.
- Measured baseline: 4,571,136 bytes at 50,000 IDs, excluding strings already
  owned by the trades. Fresh plain dict: 1,922,496 bytes.
- Reason/lifetime: exact duplicate membership and FIFO eviction.
- Avoidable portion: linked-order bookkeeping duplicates the trade deque's order.
- Change: plain insertion-ordered dict; evict the oldest retained trade's ID at
  exactly the original boundary. Expected O(1), no repeated list construction.
- Initial estimated saving: about 2.65 MB/market, approximately 79 MB across 30.
  Cycling capacity differs; the full benchmark includes repeated FIFO churn.
- Risk/tests: duplicate rejection, eviction/reacceptance, timestamps, coverage
  boundary, reset and out-of-order behavior. All retained evidence stays intact.

**P0-scale risk to track, no demonstrated runaway leak:** symbol-cardinality
caches and invocation-wide exact identity sets (section E). No speculative
eviction was implemented because prior context and exact identity semantics
must be established before reclamation. Repeated scans/rotations did not show
monotonic large-object accumulation in the controlled workload.

**P3 / do not redesign:** readiness (~28 MiB even at 10,000 outcomes/source),
one-candidate recovery (~1 MiB), SQLite mappings, metadata copies and bounded
temporary reference lists. CPU/latency and causal risks outweigh unproven gains.
No extra production `gc.collect()` or `malloc_trim()` calls were added.

## E. Structures without a fixed population bound

A blanket “no unbounded application collection” finding would be inaccurate:

- `candle_cache`, `candle_cache_limits` and `context` retain keys for distinct
  symbols/timeframes until source change; values replace rather than append
  indefinitely for the same key.
- `volume_profiles`, `swing_profiles` and `liquidation_baseline` have bounded
  per-symbol content but no fixed cap on ever-seen symbol keys.
- Native `reconnects` retains cumulative counters for ever-seen symbols.
- Bybit `subscription_changes` depends on ACK/reconnect cleanup, rather than
  an independent hard population cap.
- Readiness identity sets, and recovery discovery's `seen`/candidate list, scale
  with history during one invocation. They are released on return; pagination
  bounds individual SQL results, not the exact all-history identity population.
- Recorder gap keys can expand with affected markets while publication is
  stalled, then drain on publication. No event-per-failure list accumulates.

These are cardinality-dependent growth risks, not proof that the 30-market live
process is leaking one full Tape per rotation. Some retained prior profiles are
still consumed on reselection; unconditional deletion could change decisions.
Future work should measure these key populations through supported diagnostics,
then define ownership/expiry after the last possible consumer. Exact identity
sets could use indexed durable aggregation at much greater scale, with proof
that causal cutoffs and identity equality remain unchanged. No approximate set
or shortened evidence window was introduced.

## F. Rotation and temporary reachability

Actual NativeStreams and connected Bybit rotation regressions use weakrefs:
removed Book, Tape, liquidation deque and frame history become collectible;
retained selected and required active-lifecycle Book/Tape objects survive.
Native removal cancels and awaits jobs, then evicts sessions/last-sample state.
Bybit disconnected frame cleanup and reconnect cleanup retain existing tests.

The controlled workload rotates ten of thirty markets twice: all 20 removed
Tapes are collected and each replacement still contains 50,000 trades. Main
stream children are released; **not all scanner caches for removed markets are
evicted**, as described in section E.

Broad-scan fixture temporaries comprise 525 markets x120 immutable daily bars,
slightly above the 524 discovered in the live snapshot. These lists are released
after evaluation; peak process RSS captures the allocation. Repeated use did not
retain another full broad universe each cycle. This is a controlled broad scan,
not a timing benchmark of real REST requests.

Existing failed REST-batch cancellation/awaiting remains unchanged and passes
repository recovery tests. Optional REST cancellation deserves separate
investigation if an unawaited coroutine is reproduced; no large retained
response/task leak was demonstrated here. Library buffers remain bounded
(native message queues 64 and configured per-message bounds); actual userspace
occupancy was not measured by kernel socket counters.

## G. ML readiness

At **10,000 current, complete, unique sequence-ready outcomes per source**:

- 30,000 total fixture decisions across Binance, Bybit and OKX.
- Peak traced readiness allocation: **29,462,673 bytes (28.10 MiB)**, including
  partition feasibility work.
- Each source: `identities=10,000`, `unique=10,000`, `current=10,000`,
  `sequence=10,000`, `planning_rows=10,000`.
- 31 scalar SELECT pages, including the final empty page.
- EXPLAIN for the scalar page query reported **no TEMP B-TREE**.
- No whole feature-payload result or old window-function sort was restored.
- SQLite temp disk bytes were not independently instrumented; the inspected
  plan needs no temporary sorting B-tree. The fixture database was disposable.

The live cached baseline had 7,007 Binance identities/2,006 current-schema
decision snapshots and 99 eligible primary outcomes. This is far below the
synthetic maturity stress population, not evidence that every historical row is
eligible. Source separation, 500-outcome/500-sequence minimums, schema
`candidate-v20`, `prints-v1` and exact identity semantics are unchanged.

## H. Historical print and archive recovery

One isolated candidate used 12 pages x1,000 aggregate prints, checkpointed after
six pages, then resumed to a verified stop outcome:

- Candidate peak traced allocation: **1,086,938 bytes (1.04 MiB)**.
- A representative 1,000-row wire page graph: **249,186 bytes (243.3 KiB)**.
- Checkpoint graph: **10,671 bytes**; contained position graph **9,024 bytes**.
- At most one previous page remains alive at the next provider fetch; the
  replacement can briefly overlap with it. Zero page objects remained after
  completion/GC. The per-page `consumed` list does not accumulate all pages.
- Trade transformation is one record at a time; PaperPosition preserves its
  required fill state. The fixture had one fill, not an unlimited fill history.
- Old/new checkpoint and certified outcome digests matched byte-for-byte.
- Recovered ordering, ID continuity, original snapshot, source and certificate
  checks were preserved.

Checkpoint digest:
`481ef385fb33b51ed70763442e888c4d5b57e9fb0602353c22a1b76368eb04a7`.
Outcome digest:
`bf5fc257e629f5d35b313511daee0b2602970e67004e4f154c51cb7a2b223e09`.

Recovery remains optional, subordinate to live recording, <=3 discovered
candidates per batch, bounded pages, a 45-second worker deadline, <=1,000
REST records/page and the existing 95%-storage/live-pressure guards.

Official archive downloads use 1 MiB chunks written to disk; SHA-256 reads
1 MiB at a time. ZIP CSV iteration uses <=4,097-byte reads, <=1,000 rows per
page and a ten-second page limit. The approximately 128,000,000-byte limit is
a **disk cache**, not persistent Python heap. No full archive/CSV is loaded
into RAM. A regression rejects unbounded ZIP-member reads and verifies page
cursor resumption. Checksum rejection is already covered.

ZIP seeking may reinflate a prefix and cost CPU/I/O; this is not a retained
full-archive buffer. The provider's verified-file map lives for one bounded
worker batch. The audit did not download a live archive, populate its cache,
trigger live recovery or rewrite historical labels.

## I. Before/after controlled process results

| Metric | Before | After | Change |
|---|---:|---:|---:|
| Controlled desk RSS | 1,145.7 MiB | 773.9 MiB | -371.7 MiB |
| Controlled desk PSS | 1,135.3 MiB | 763.3 MiB | -372.0 MiB |
| Controlled desk anonymous RSS | 1,131.7 MiB | 732.2 MiB | -399.5 MiB |
| Controlled desk file RSS | 13.9 MiB | 41.7 MiB | 27.8 MiB |
| Controlled peak RSS | 1,182.2 MiB | 790.0 MiB | -392.2 MiB |
| Total workload process CPU | 87.66 s | 69.67 s | -20.5% |
| Retained markets / trades | 30 / 1,500,000 | 30 / 1,500,000 | unchanged |
| Removed Tape objects collected | 20 / 20 | 20 / 20 | unchanged |

| Stage | Before RSS MiB | After RSS MiB | Before cumulative CPU s | After cumulative CPU s |
|---|---:|---:|---:|---:|
| cold | 82.5 | 82.1 | 0.00 | 0.00 |
| warm | 1,163.3 | 771.1 | 40.26 | 33.65 |
| scan-1 | 1,162.9 | 774.1 | 43.36 | 36.34 |
| scan-2 | 1,163.5 | 774.2 | 46.18 | 39.27 |
| scan-3 | 1,162.6 | 775.2 | 50.26 | 42.38 |
| rotation-1 | 1,148.9 | 774.2 | 67.46 | 52.13 |
| rotation-2 | 1,145.5 | 773.6 | 79.24 | 61.85 |
| post-gc | 1,145.7 | 773.9 | 87.66 | 69.67 |

Both processes reported zero swap. No allocator trim was forced; post-GC is
only part of the offline benchmark. The optimized RSS rose about 4 MiB between
warm and the last scan, then remained near 774 MiB through rotations. This
supports bounded behavior over this workload, not a months-long no-leak proof.

The semantic digest of all retained trade fields, ID ordering, coverage clocks,
flow and event features was identical:

`d8d7a6daf47b33953cd3133db0b027232d18f7edd3a961e36765b7f2ecfad295`.

Full-workload CPU includes warmup, scan-state construction, flow/evidence
evaluation, rotations and hashing. Median controlled scan-cycle CPU was
3.09 -> 2.93 seconds. This is **not** live scanner duration: the real scan
baseline was 349.4 seconds including network/history/admission work. There was
no after-deployment latency measurement.

Trainer RSS was not optimized or deployed: live 196.3 MiB initially, 354.1 MiB
at the later worker snapshot. These are observations of unchanged source.

### Recorder verification

| Metric | Offline control A | Offline control B |
|---|---:|---:|
| Events offered / written | 9,750 / 9,750 | 9,725 / 9,725 |
| Drops / overflow / retries | 0 / 0 / 0 | 0 / 0 / 0 |
| Final queue depth | 0 | 0 |
| Queue high-water | 1,400 | 1,625 |
| Backpressure waits | 0 | 0 |
| Last batch writer latency | 125.9 ms | 64.9 ms |
| Observed offered throughput | 461.8/s | 461.5/s |

These are two isolated 20-second controls of the **unchanged recorder path**,
not a causal before/after recorder-performance claim. Half-CPU, 768 MiB
container limits overrode the soak tool's static resource-description string.
Time-driven producer counts and queue peaks vary with scheduling. Decoder
regressions separately verify unchanged raw recorded payloads; tests cover
frame recording, recorder integrity, retry behavior and recovery.

Live recording continued: initial queue 1,715/10,000, mid-audit queue 0;
later queue 0 with a transient RECORDER_PRESSURE state. Live cumulative
backpressure waits increased 2,445 -> 2,747 while receiving market data.
Drops/overflow stayed **0/0**, high-water stayed **5,102**, retries stayed **16**
with the same pre-audit TimeoutError timestamp. Last sampled batch latency
varied 3,002 -> 5,276 ms; no unsupported claim of constant live latency is made.
The host was already under memory/cache pressure and live feeds were not all
fresh (18/30 initially, 22/30 later; active feed freshness also varied).
This memory patch does not repair or mask network/feed health.

## J. Semantic freeze

```text
Market coverage changed: NO
Strategy changed: NO
Scoring changed: NO
Gate thresholds changed: NO
Evidence windows changed: NO
Recorder safety changed: NO
ML thresholds changed: NO
ML outcome semantics changed: NO
Historical recovery semantics changed: NO
Lifecycle/deduplication changed: NO
Discord qualification/delivery changed: NO
Model compatibility/selection/promotion changed: NO
```

The 30% Location / 45% Mechanism / 25% Trade Offer weighting and weakest-pillar
logic are untouched. There is no global Decimal-to-float replacement, price
rounding, shortened Tape, reduced book depth, removed OFI evidence, combined
venue dataset, relaxed minimum, retraining or promotion.

Proof is layered: exact decoded Trade fields and Decimal representations;
identical flow/features/paper outcomes under direct versus shared construction;
FIFO/coverage/reset regressions; equal full-workload and recovery digests; and
normal repository fixtures for generation, identities, structure, scores,
quality/confidence, entry/risk, V8, lifecycle, research and Discord rules.
The controlled benchmark does not run the entire networked scanner or send
Discord messages.

## K. WSL/Docker assessment

File cache accounted for approximately **65% of the desk cgroup and 90% of the
trainer cgroup** initially. That explains much of the Docker figures.
Small file-backed process RSS and mmap disabled on the probe contradict
equating a ~21 GB SQLite file with a similarly large mapped Python heap.

The read-only SQLite probe reported `page_size=4096`, `cache_size=-2000`,
`mmap_size=0` and `temp_store=0`. These are probe-connection settings, not a
direct interrogation of every running connection. Store/writer source does not
override the defaults. Six main-desk file descriptors referenced the database,
two WAL and one SHM; descriptors are not an exact connection count. The database
was 21,760,897,024 bytes, WAL 266,489,872 bytes, SHM 458,752 bytes. No VACUUM,
cache purge, evidence cleanup or WAL intervention was performed.

WSL was version 3.0.3.0 with kernel 6.18.54.1-1; `.wslconfig` was absent.
Docker's effective host resource/reclaim policy could not be established from
the available filtered settings; cgroup limits were observed directly.
Current Microsoft documentation lists `autoMemoryReclaim` options separately
from the VM memory limit. Absence of a local override does not prove which
effective Docker/WSL policy is active.
See [WSL configuration](https://learn.microsoft.com/windows/wsl/wsl-config).

Operator recommendations, **not applied**:

- Evaluate this representation change in a separately approved deployment and
  measure live RSS/PSS, swapped anonymous memory and recorder latency over
  normal busy periods.
- Review effective Docker/WSL memory-reclaim settings at a planned maintenance
  window if Windows remains short of RAM while Linux has reclaimable memory.
- Compare Windows available RAM with Linux available/cache and process memory;
  a higher WSL working set after an offline workload need not be a Python leak.
- Avoid periodic forced page-cache drops and routine service restarts as a
  memory-management mechanism. They discard useful cache and interrupt work.

## L. Container limits

**KEEP CURRENT LIMITS.** Desk remains 4 GiB, trainer 2 GiB.

There is no optimized live steady-state/p95/burst/peak series with recorder and
recovery concurrency, and existing cgroups already encounter reclaim pressure.
The offline benchmark is not sufficient evidence for a lower production hard
limit. No host/container configuration was changed.

## M. Verdict and operational limits

**SIGNIFICANT_SAFE_SAVINGS_FOUND**

- Largest demonstrated real application state: retained Tape/Trade objects,
  including duplicate immutable Decimal values and duplicate FIFO bookkeeping.
- Much of the alarming cgroup number was file cache: ~2.60 GiB desk and
  ~1.79 GiB trainer initially. These overlap Linux/WSL accounting.
- Real leak: no runaway Tape/frame leak reproduced. Cardinality-dependent
  retained caches and exact temporary identity sets remain explicit risks.
- Actual steady-state live desk RSS removed: **0 MiB**, because no deployment.
  Controlled post-GC RSS saving: **371.7 MiB**; live savings require measurement.
- Repeated scans/rotations: no large monotonic growth; all removed Tapes collected.
- Historical recovery: about 1 MiB transient peak in the bounded candidate test;
  no persistent all-pages buffer or full-archive materialization found.
- Lighter operation is supported by this bounded representation change, with
  configuration/evidence preserved. The exact benefit depends on live token
  diversity and Tape occupancy.
- Docker limits: keep current values.
- Live recorder interrupted: **NO**. No desk/trainer/DNS restart, recreate,
  reconnect request, WSL shutdown, training or destructive maintenance.

## Reproducing the offline checks

Run from a development checkout with the project's development dependencies
installed. Use a disposable environment; these commands do not target live data.

```sh
python scripts/verify.py
python examples/memory_benchmark.py --mode full --symbols 30 --trades 50000
python examples/memory_benchmark.py --mode components --symbols 30 --trades 50000
python examples/memory_benchmark.py --mode readiness --readiness-rows 10000
python examples/memory_benchmark.py --mode recovery
```

Use the deployment's actual market count, not an assumed default. Without
`--sample` the trade input is synthetic and highly repetitive. A supplied JSON
sample uses `{"trades":[{"s":"TESTUSDT","T":1000,"a":1,"p":"100.00","q":"1.000","m":false}]}`;
it remains an offline software workload and is not trading evidence.
The private audit capture is excluded from the repository.

For an old/new comparison, export `orderflow.py` and `exchanges.py` from
`d1c1fd7:src/bybit_flow/` into a temporary directory with `git show` (no checkout
or reset), then supply `--baseline-dir`. Both runs must use the same interpreter,
sample, market count, capacity and resource limits. Do not use the old early
prototype probe against modified source as an independent baseline.

Linux `/proc` measurements are emitted when available; non-Linux runs report
that RSS is unavailable. Object-graph traversal is restricted to component
mode because its own visited-object set can inflate process RSS. Tracemalloc
is restricted to the isolated readiness/recovery cases. No exact-byte unit-test
threshold depends on interpreter allocator internals.
