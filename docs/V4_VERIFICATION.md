# V4 implementation and verification

This pass fixes a reproduced confirmation failure and adds measured diagnostics
and forward policy research. Production confirmation and stop policies remain
unchanged until independently validated challengers exist.

## Reproduced failure and recovered delivery

The optional horizon recommendation queried and sorted large research-label
payloads inside live confirmation. On this Docker configuration it raised
`SQLITE_FULL` in SQLite temporary storage, aborting evaluation before scores and
gates were saved. Recommendation reads now use a small causal cache; the background
research worker materializes it. Confirmation health records structured failures.

The original audit found 1,820 generated candidates and no new confirmed setups.
After the fix, six genuine initial Discord deliveries were verified by their
outbox status and Discord message IDs. They were not synthetic cards:

| Market | Direction | Horizon | Quality | Discord message ID |
|---|---|---|---:|---|
| LSKUSDT | SHORT | Short intraday | 58.1 | 1551143826444521584 |
| ZECUSDT | SHORT | Short intraday | 78.5 | 1551171703508308041 |
| ZENUSDT | SHORT | Short intraday | 71.9 | 1551172014784512052 |
| SOLUSDT | SHORT | Short intraday | 65.3 | 1551172649991016501 |
| COTIUSDT | SHORT | Short intraday | 72.0 | 1551175761212346481 |
| HYPEUSDT | SHORT | Short intraday | 68.9 | 1551187822390804514 |

These deliveries prove the confirmation repair restored the genuine alert path.
They do not validate the new research challengers or predict future signal timing.

## Operator diagnostics

`bybit-flow signals status`, `/api/funnel` and the Signal journal separate recorder
events, broad discovery, setup generation, confirmation, gates and Discord delivery.
Counters include 15-minute, one-hour, 24-hour and process windows, stage timestamps,
horizon/family/direction totals and representative rejection reasons. Historical
unsaved gate outcomes are unavailable rather than inferred. Technical test messages
are excluded. Minute counters are bounded; cumulative totals remain durable.

Dashboard ML counts are materialized by the background worker, with a summary
timestamp. Routine API calls no longer scan all large label payloads. Confirmation
reads similarly avoid optional historical research queries. Full integrity checking
is separate from the lightweight operational database check in `doctor`.

## Decision evidence and research boundaries

- Complete execution windows from selected native markets populate participation
  baselines independently of candidate generation. Baselines separate venue,
  instrument, horizon and session. Current/future windows cannot enter their own
  reference distribution. Missing coverage adds no sample; 20 prior windows are
  required for percentiles.
- Factor evidence uses aligned closed bars, prior fit data and decision-time
  availability. BTC/ETH self-factor inputs are excluded. Context, setup and
  execution timeframes retain separate observations where available.
- Executed-volume profiles use complete closed 30-minute intraday or four-hour Swing tape windows, tick-aligned
  bins, a contiguous 70% value area and observed volume nodes. Heavy work runs in a
  background thread with a trade-count bound. Suitable profile targets may enter
  new structural-targets-v2 plans. No old plan is retargeted or given a wider stop.
- Confirmation research compares the incumbent, normalized participation,
  participation plus contrarian residual evidence, and multidomain confirmation.
  Challenger admission remains shadow-only and retains coverage, macro, entry-zone
  and risk requirements. It does not remove production gates to manufacture alerts.
- Forward Swing jobs freeze incumbent, noise-buffer, sweep/volatility and ATR-floor
  stops at decision time. They track first stop/TP1/TP2 bounds, overshoot, reclaim,
  time beyond the stop and post-stop MFE/MAE. Ambiguous entry/exit ordering and gaps
  exclude complete return labels. A 4H close through the decision structure is a
  descriptive failure proxy, not a validated thesis label.
- Research comparisons require at least 500 independent eligible examples and
  adequate chronological train/calibration/validation/holdout partitions, outcome
  purging and a one-day embargo. Holdout cohorts are reserved before evaluation.
  Return intervals remain descriptive; correlated-market uncertainty requires
  stronger validation before any promotion. Existing production policies remain.
- Feature schema candidate-v6 adds participation fields. Existing snapshots,
  labels, holdouts and artifacts are preserved; a schema change never recaptures an
  old decision using later lifecycle information. Older cohorts are not silently
  relabeled as current-schema examples.

## Verified operational evidence

- Existing GitHub Actions workflow retained, formatting repaired, and a short
  offline recorder test added. It does not access exchanges or Discord.
- Final full suite: **255 tests passed**; Ruff checks and formatting, JavaScript
  syntax and the offline research example passed. Four third-party warnings
  concern Starlette/httpx/anyio and scikit-learn SVM deprecation.
- Isolated recorder soak: 1,801.17 seconds, 1,766,350 offered and written events,
  980.67 events/second, zero drops, zero overflow, zero retries, final queue zero.
  Limits: two CPUs and 4 GiB. Queue high-water mark: 1,850 of 10,000.
  This was an offline recorder load test, not a 30-minute live scanner soak.
- Bybit, Binance and OKX passed public REST and genuine trade WebSocket probes.
- Discord connection test and labeled synthetic card both returned `sent`.
  Synthetic verification created zero signal rows and zero ML rows.
- A genuine broad scan discovered 528 instruments, found 74 eligible markets,
  performed 30 depth checks and selected 30 deep markets with zero scan errors.
- Cleanup dry-run at approximately 18.2 GB scheduled no deletion below pressure.
  Compaction dry-run found 167 segments / 501 files eligible for lossless packing.
  Existing private configuration remains unchanged, including its 30 GB budget.

## Preservation

An additive SQLite backup was completed before schema changes:
`bybitflow-backup-v4-20260920:/backup/before.sqlite` (5,879,812,096 bytes).
No backup or model artifact was deleted.

Read-only comparisons found zero missing or changed records among 28,419 terminal
signals, 28,431 original plans, 49,588 snapshots, 21,057 ML labels and 7,308 research
labels. All 12 setups active at the initial baseline subsequently reached their
original entry expiry with no changed entry, zone, stop, targets, version, horizon
or deadlines. No setup was terminated by clustering or deployment. A separate
intermediate pre-deployment capture had zero still-active setups; the final
deployment captured eight scored pending setups for a separate comparison. All eight reached their original entry expiry with zero changed plan fields. Confirmation now also
preserves already-assigned deadlines instead of extending them at first scoring.

## Remaining empirical limits

No trained champion or promoted confirmation/stop challenger exists. New normalized
and Swing path cohorts need to mature before independent validation is possible.
Profiles also retain causal rolling POC/value-area migration, descriptive
acceptance/rejection and excess evidence. Their windows do not establish a complete
multi-session auction. Thesis-versus-stop classes require coverage, reclaim, small
overshoot, intact structural proxy and a successful wider-stop counterfactual; they
remain descriptive hypotheses until validated. Late recovery alone is insufficient.

The recorder soak establishes isolated ingestion capacity; it is not a substitute
for a sustained live feed/maintenance soak on every subsequent code revision.
No guaranteed alert cadence or profitability is claimed.


Final endpoint measurements returned HTTP 200: `/api/funnel` in 0.141 seconds and
`/api/ml` in 0.012 seconds using the worker cache, compared with 10.725 seconds for
the earlier uncached ML request. These are local observations, not latency guarantees.
The final recorder smoke test wrote all 19,600 offered events with zero drops and
an empty final queue in 21.27 seconds.
