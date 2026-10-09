# ML research

For installation and everyday operation, use the [README](../README.md). For clock,
disk, Docker, or feed problems, use [operator troubleshooting](USER_RUNBOOK.md#troubleshooting).
This guide describes the implemented research pipeline, not the current runtime's
model count or data readiness; inspect `bybit-flow ml status` for those values.

## Collection and labels

Current feature schema `candidate-v10` stores immutable generation snapshots and the first
scored decision for each setup. Source identity, strategy version, event time,
availability time, missingness, and point-in-time membership accompany the features.
Exports are source- and schema-specific. Lifecycle states, predictions, and outcomes
are excluded from the feature allowlist.

The `prints-v1` label policy evaluates frozen candidates, including rejected setups,
against recorded trades. Paper fills use participation limits, latency, fees,
slippage, and a funding reserve. Outcomes exit at TP1, stop, or the frozen holding
deadline (four hours for legacy/core intraday). TP2 is informational. Missing continuity, late exits, and unresolved
positions do not become complete training outcomes.

The worker preflights committed raw hashes, manifests, row counts, bounds, and
ordering. Missing, unreadable packed, clock-damaged, and overlapping intervals become
audited global gaps. Original files and packs remain unchanged. Hash and manifest
mismatches on readable evidence halt processing.
Strict explicit replay continues to reject unordered input.

Use the [primary completeness audit](PRIMARY_COMPLETENESS.md) to distinguish
unready decisions, genuine evidence gaps, duplicates, open horizons and
unverifiable historical recordings. `ml audit-completeness` performs bounded
read-only reconstruction; `ml status` shows its cached quality summary.

Snapshot queries materialize a bounded result before yielding. This releases SQLite
read cursors before concurrent recorder commits and subsequent ML writes.

## Training

`FLOW_ML_ENABLED=true` enables inference when a compatible model is available.
`FLOW_ML_FILTER_RESEARCH=false` keeps model acceptance from filtering research
alerts. The separate trainer process runs with an advisory lock.

```sh
docker compose --profile ml up -d
docker compose exec trainer bybit-flow ml status
```

The worker monitors outcomes every 15 minutes. Each source and training track
starts its first fit on the next loop after reaching 500 usable outcomes and
meeting causal partition and class requirements. Failed
fits retry after 15 minutes or a change in the usable count. After a successful
fit, another cycle needs 50 new outcomes or seven days. A manual cycle can also
request a fit. Chronological partitions can still leave too few samples; the
worker reports that reason instead of weakening the checks.

## Monitored setups and laptop downtime

Continuous local recording is required for the strict `prints-v1` paper-fill
policy, but is **not required for monitored learning**. The separate
`monitored-ohlc-v1` track uses the full, immutable current `candidate-v10` decision
features and recovers the original venue's closed one-minute price history.
It keeps the original entry zone/window, stop, TP1, holding deadline and cost
assumptions. Current-schema frozen candidates are included independently of
Discord delivery; canonical economic identities prevent repeated evaluations
from becoming independent samples.

For example, a swing enters the zone while the application is running, the
laptop shuts down, and the stop is reached overnight. On restart, lifecycle
reconciliation updates the original setup. The trainer recovers that price path
and records a complete loss when the entry/exit ordering is established. The
strict print label stays unchanged; both policies coexist. No local trade tape
is required for the missing hours. Terminal stop/target setups can be labeled
before the full swing horizon ends.

New live entry-zone touches store their event time, price, source and trade ID.
Recovery uses that entry evidence when it is within the frozen decision and
entry window. Older setups without that evidence use a historical zone touch.
Both are hypothetical fills, not verified account executions. A partial entry
candle with an exit touch, or a candle touching both stop and TP1, is ambiguous
and excluded. Missing public history or a still-open path is retried rather
than permanently labeled incomplete. Unfilled setups are excluded, not losses.

The trainer refreshes monitored recovery and readiness every 15 minutes, subject
to the existing disk budget and bounded REST/cache limits. Labels retain candle
event availability separately from materialization time; dataset availability
cannot precede recovery. The same 500-outcome, chronological partition, class,
embargo and untouched-holdout checks apply. Source-specific baselines and the
16-observation two-stage models can learn from complete monitored outcomes.
Monitored models remain advisory, retain proxy provenance and cannot be promoted
or filter delivery. Compatible primary models take priority over monitored
models; monitored models take priority over historical bootstrap models.

From CMD in the repository directory:

```bat
docker compose --profile ml up -d
docker compose exec trainer bybit-flow ml status
docker compose exec trainer bybit-flow ml monitored-labels --source binance --limit 100
```

The status includes `monitored_trainability`, `monitored_backfill`,
`monitored_model_id` and the monitored partition/fit plans. Normal operation
does not require the manual labeling command. `monitored-export` and
`monitored-train` are available for explicit offline research.

## Historical bootstrap track

`ohlc-path-v1` evaluates the original venue's public, fully closed one-minute
candles against the frozen entry zone, stop, TP1, entry deadline and holding
horizon. TARGET, STOP and TIME_EXIT can become complete outcomes with finite net
R after the frozen cost reserve. NO_ENTRY, AMBIGUOUS and INCOMPLETE never become
training losses. A candle that cannot establish the order of entry, stop and
target is excluded. Partial decision and deadline candles do not supply invented
intrabar ordering. Event availability is the relevant candle close, separate
from the time a historical label was materialized.

Backfill includes isolated decisions whose timestamps fall between minute
boundaries and uses the last fully closed candle before the frozen deadline.
Labels retain the frozen aggregate cost and fee, slippage and funding components
when available; missing historical components remain unspecified.

These labels are **OHLC proxies**, with assumed costs and no verified execution.
They coexist with immutable `prints-v1` labels and never repair or replace them.
Rejected frozen decisions are included; Discord delivery is not a sampling rule.
Canonical economic identities prevent technical duplicate evaluations from
becoming independent outcomes.

The `bootstrap-core-v1` projection explicitly supports native candidate schemas
v4–v10. The catalog and implementation history preserve the selected closed-bar
ATR, volatility, efficiency, slope, volume expansion, quoted spread, microprice,
funding and frozen plan cost/reward definitions. Horizon roles changed in v5;
their actual timeframes are explicit contexts. Definition and receipt-time checks
reject incompatible or future observations. Original snapshots remain intact.
Flow semantics, quality scoring and alignment changed across these versions, so
they are excluded, as are OFI, breadth, AVWAP, spot/perpetual comparisons,
event-window OI, context EV, liquidation and V8 gate features.

Source-specific logistic and LightGBM baselines require at least 500 complete
unique outcomes and the same chronological partition minimums as primary models.
Bootstrap and primary reserve separate holdouts keyed by track, source, feature
schema and label policy. LSTM/two-stage training still requires 500 complete
16-observation sequences. Venues are never pooled.

A bootstrap challenger may provide an advisory score and explanations. Discord
shows “Bootstrap score / 100”, “OHLC-proxy historical challenger” and
“Production authority: none”. It cannot change the deterministic grade, entry,
stop or targets, filter delivery, display a validated probability or become a
champion—even with a named reviewer. A compatible primary model takes priority.

The worker already runs bounded resumable backfill automatically. From CMD in
the repository directory, inspect its status or preview a backfill batch:

```bat
docker compose exec trainer bybit-flow ml bootstrap-labels --source binance --limit 500 --resume --dry-run
docker compose exec trainer bybit-flow ml status
```

Use `bybit` or `okx` only for candidates frozen on that venue. Overlapping symbol ranges are merged, REST
requests use an independent one-request-per-second limiter and bounded retries,
and progress survives restarts. A small Zstandard Parquet candle cache can be
reclaimed after labels are durable. Manual fitting is an advanced operation:
stop the persistent trainer first, use a one-off container, and resume it afterward.
The following requests a bounded cycle; it can legitimately abstain on missing
classes, unseen holdout shortage, or storage pressure:

```bat
docker compose --profile ml stop trainer
docker compose --profile ml run --rm --no-deps trainer bybit-flow ml cycle
docker compose --profile ml up -d trainer
```

Always run the resume command even if the manual cycle reports a failure. Do not
use manual cycles to reuse holdouts or bypass compatibility checks. Separate
`bootstrap-export` and `bootstrap-train` commands are available in the ML CLI;
fitting should not compete with the running worker.

## Storage and replay checkpoints

Primary incremental checkpoints preserve positions, subscriptions, trade IDs,
continuity state, input hashes and the consumed receipt cursor. A fresh, sealed,
consistent checkpoint protects only the unprocessed tail plus a 15-minute replay
margin for an unresolved primary outcome. Missing, stale or inconsistent
checkpoints retain the full original interval. Late-published segments remain
protected until a later checkpoint consumes them. Operational active setups
independently protect their unprocessed history plus 15 minutes around the
durable event/receipt cursors. Missing or inconsistent lifecycle progress keeps
the original interval protected. Cleanup preserves original plans and their
independent lifecycle; finalized ML evidence cannot release unprocessed setup data.

At 95% storage usage, the worker prioritizes incremental outcomes and integrity-safe
pruning. Bootstrap downloads and fitting wait for headroom. Permanent snapshots,
labels, model artifacts, manifests and audit hashes are retained. Status reports
protected intervals and checkpoint boundaries; storage pressure cannot be
described as healthy merely because the worker heartbeat is fresh. SQLite busy
writes retry briefly and defer the worker loop without a crash/restart cycle.

For a CI-gated Windows deployment, install PowerShell 7 and Git, then run the
checked script from CMD in a clean `main` checkout with the desk already running:

```bat
pwsh -NoProfile -File scripts\deploy_ml_bootstrap.ps1
```

It requires green GitHub CI for the exact `main` commit, preserves the data volume,
records original active plans and delivery receipts, serializes bounded trainer
work and saves a continuity audit under `data/deployment-audits`. It defers
backfill if safe cleanup cannot bring storage below 95%. The script has no volume
deletion or model-promotion step.

Manual research commands:

```sh
bybit-flow ml label RECORDING.jsonl.gz
bybit-flow ml export --source okx
bybit-flow ml train DATASET.parquet --model logistic
bybit-flow ml infer MODEL_ID DATASET.parquet
```

Training requires at least 200 training, 100 calibration, 100 validation, and 100
holdout outcomes after chronological partitioning, purging, and embargo. These
minimums do not establish promotion eligibility.
The `adaptive-causal-v2` policy keeps V1's earliest feasible boundaries unless
the split has at least 1,500 outcomes, at least half in holdout and at most a
quarter in training. In that case, a bounded search of at most 64 timing-based
boundaries prefers post-purge shares near 50% training, 15% calibration,
15% validation and 20% holdout. These shares are targets, not minimums. Exact
causal counts validate every candidate. If no better feasible allocation exists,
the V1 split remains available with a recorded fallback reason.

Each development row must have its exit and label
available more than four hours before the next partition starts. Boundaries
depend only on timing and sample counts, never returns, classes or model scores.
This avoids empty partitions caused by fixed decision-time percentiles when
outcomes have different holding periods. Equal decision times stay together.

Later training cycles require a new holdout starting strictly after the previous
holdout end plus four hours. The planner can move that start later to leave enough
causal development data. Existing holdout reservations remain binding, and
walk-forward research retains its separate chronological checks.

For source/track datasets with at least 500 usable outcomes, `ml status` includes
cached `partition_feasibility`: selected boundaries, counts before and after
purging, shortfalls and the blocking partition. The worker computes these details
while preparing data; normal status requests do not rescan feature payloads.
`READY` means partition sizes are sufficient. Model fitting still requires both
outcome classes in training and calibration and does not imply validated edge.
Cached `model_fit_feasibility` reports each partition's row and class counts,
`fit_ready`, and a specific blocker: partition shortage, single-class training
or calibration, or model-library failure. Positive means net R above zero;
the negative class includes zero. Class counts are diagnostics only and never
move a partition boundary. Status also records `partition_policy_used` and
`fallback_reason`. A consumed holdout can make the next cycle `NOT_READY`
while the existing challenger remains usable for compatible advisory inference.
Millions of recorded events do not substitute for complete, independent outcomes.
`ml status` reports complete labels, recent worker activity, abstention reasons, and
whether a compatible challenger exists. Network gaps can leave many labels incomplete.

The pipeline fits preprocessing on training rows only, then trains regularized
logistic or shallow LightGBM models. Calibration uses independent data. Bounded
threshold trials, ablations, baselines, development-fold diagnostics, and clustered
uncertainty are retained. Holdouts are reserved before evaluation and cannot be
reused as unseen evidence.

Models use JSON coefficients or tree structures. Live inference does not load
pickle artifacts or require training libraries. Research ranking is separate from
the deterministic quality score and is not a validated win probability.

## Promotion and live admission

The implemented `promotion-v2` policy requires real data, verified costs, clean code
provenance, point-in-time membership, at least 200 selected holdout outcomes and 78
weekly clusters, positive adjusted EV and incremental-baseline bounds, improved
Brier score, and supported regime and tail-risk results. Additional calibration
and development-fold checks apply.

Live probability display also requires a matching family/direction/regime cohort
with at least 100 outcomes, 26 clusters, a positive EV lower bound, and a win-rate
interval width no greater than 0.20. Counts alone do not demonstrate independence
or predictive accuracy.

Promotion and rollback require a named reviewer and cannot waive admission checks:

```sh
bybit-flow ml promote MODEL_ID --reviewer REVIEWER
bybit-flow ml rollback --reviewer REVIEWER
```

Current print labels use assumed costs and cannot satisfy verified-cost promotion.
Model approval, strategy-code changes, score-weight changes, and rollback are not
automatic.

## Compatibility and limits

Inference abstains on incompatible source, stage, schema, strategy, or context;
missing required features; degraded models; or stale evidence. Model history and
deployment records retain the reason and evidence.

Exports are limited to 10,000 rows, 128 MB Parquet files, and 256 MB uncompressed row
groups. The primary worker uses durable incremental replay checkpoints. Historical
bootstrap work uses bounded resumable batches and a compact candle cache. Large
histories still require capacity planning; protected evidence and permanent records
are never deleted to force a fit.

Historical `chart-v1` rows remain readable for audit, but TradingView ingestion and
new chart labels are retired. Manual journal outcomes are not automatically imported
into training.

Datasets, recordings, models, audit files, and failed experiments belong in persistent
research storage, outside Git. See [operations](OPERATIONS.md) for backup and restore.

## Stable maturity period

The CI-verified deployment script creates `ml_maturity_epoch` after its runtime
and continuity checks. It records the deployed commit, start time, schemas,
label policies, partition policy and V8 gating policy. Creation is atomic;
restarts, normal worker cycles and later deployments preserve the original epoch.
This marker describes collection provenance and never participates in trade
decisions. `ml status` and the ML dashboard show the marker and cached progress.
Primary progress uses the active venue's complete, unique `candidate-v10` /
`prints-v1` outcomes divided by 500. Venues remain separate; percentage progress
is training eligibility, not confidence or evidence of profitable predictions.
The view also includes sequence progress, decisions since the epoch, model IDs,
storage usage and recorder drops.

After this final stabilization pass, keep candidate-v10 feature meanings, V8
production-gate thresholds, strategy family rules, quality-score weights, label
policies, bootstrap projection, partition policy and ML minimums fixed. Change
them only to repair a correctness bug, safety issue, broken exchange API or
severe runtime/storage failure. A handful of recent trades is not a reason to
tune policy. There is no automatic strategy optimization, threshold tuning or
model promotion.

Let the collector materialize causal print outcomes, bootstrap outcomes and shadow
V8 counterfactuals. The worker still trains its first ready model immediately and
subsequent models after 50 new usable outcomes, seven days, or a manual cycle.
Unseen contexts and insufficient unseen holdouts remain explicit abstentions.
Do not force a fit by shortening embargoes, reusing holdouts or weakening
compatibility checks.

Storage status uses healthy below 80%, warning at 80–90%, cleanup pressure at
90–95% and bootstrap/training backpressure at 95% or above. Checkpoint-aware
retention and recorder priority remain unchanged. Permanent labels, models,
snapshots and active setup evidence are preserved; raising the cap is not a
retention repair.

## Two-stage outcome models

When two-stage mode is enabled but fewer than 500 compatible complete sequences are
available, the worker attempts the Logistic Regression/LightGBM tabular baseline.
Chronological partitions and class requirements remain unchanged. Feature schema
`candidate-v10` includes causal flow-quality, market-alignment and V8 research fields; historical snapshots
remain immutable and incompatible schemas are not silently pooled for training.
Advisory inference skips incompatible or degraded artifacts. Models are never
automatically promoted, and deterministic setup scores are not replaced by ML scores.
Flow confirmation mode remains signal provenance. Historical v9 snapshots remain
unchanged; primary training needs enough completed, source-specific `candidate-v10`
outcomes and never rewrites historical decision features. Historical bootstrap labels use the separate
`ohlc-path-v1` policy. `python -m bybit_flow.ml.ablation --source binance`
reports insufficient evidence until at least 500 compatible outcomes exist. Holdout
consumption requires an explicit offline `--consume-holdout` run and is irreversible
for that period. The ablation report never promotes features or models automatically.

Set `FLOW_ML_ENABLED=true` and `FLOW_ML_TWO_STAGE=true` to collect causal sequences
and run the two-stage research pipeline. Stage one uses LightGBM, Random Forest
and a CPU LSTM. Stage two compares Logistic Regression, an RBF SVM, and Random
Forest probabilities. The SVM uses separate chronological probability calibration.
LightGBM is the selected boosting implementation; XGBoost is not installed.

Each LSTM input has 16 timestamped observations from the same venue, symbol,
direction and setup family within two hours. Missing history is not padded with
invented observations. Existing snapshots and labels remain immutable and available
to the original tabular models. Only sequence-complete outcomes enter two-stage
training. Base-model labels must be available at least four hours before the meta
training period. Independent calibration, validation and untouched holdout periods
follow. At least 500 usable sequence outcomes are needed overall, with additional
partition and class requirements; this is a software minimum, not proof of edge.

The worker updates readiness every 15 minutes and dispatches the first eligible fit
on its next 30-second loop. Later fits require 50 new usable outcomes or seven days;
failed attempts retry after 15 minutes or a change in usable count. LSTM training is confined to the CPU worker; live
inference reads JSON weights using NumPy, without loading PyTorch or pickle files.
`FLOW_ML_FILTER_RESEARCH=false` keeps incomplete or abstaining models from blocking
otherwise confirmed research signals. Model rankings are not validated win rates;
there is no automatic promotion. The configured recording budget and retention policy
remain independent of model choice.

Implementation references: [PyTorch LSTM](https://docs.pytorch.org/docs/2.14/generated/torch.nn.LSTM.html),
[scikit-learn SVC](https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html),
and [Random Forest probabilities](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html).

V8.1 gate snapshots include readiness separately from the unchanged quality score.
Corrected OFI, return versus regime breadth, VAH/VAL acceptance and minute OI
semantics belong to v10. Sequence history never mixes schemas. Source-specific
ablation uses v10 outcomes with chronological purging, embargoes and untouched
holdouts. Frozen V8-blocked V7-valid plans enter the existing primary-print labeling
path, with complete outcomes only; an incomplete or hypothetical path is not
training evidence. Opportunity priority is excluded from model inputs.

Context EV remains descriptive until 200 effective causal observations support
mature confidence. Its source-specific cache must be at most 30 minutes old.
Production context is isolated to `candidate-v10` decisions with confirmation
policy `v8-production-gating-v1`, the same venue and primary `prints-v1` outcomes
available strictly before the current decision. Historical research uses a separate
cache and cannot trigger a production veto. The effective-sample heuristic and
-0.10R lower-confidence-bound threshold are unchanged.
The V8 effectiveness report describes avoided stops, missed targets and matched
passed outcomes. These comparisons do not establish improved live returns or
automatically change thresholds.

Primary paper positions with a recorded coverage gap are finalized immediately
as **incomplete**, with no return label or training eligibility. Waiting for a
multi-day horizon cannot repair that missing evidence. Their exclusion and
frozen snapshots remain; safe cleanup may release the raw data once no active
setup or replay lease needs it. Clean pending positions remain protected.
This ML exclusion does not close or modify an operational setup.
