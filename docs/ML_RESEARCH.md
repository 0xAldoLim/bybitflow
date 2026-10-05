# ML research

## Collection and labels

Current feature schema `candidate-v10` stores immutable generation snapshots and the first
scored decision for each setup. Source identity, strategy version, event time,
availability time, missingness, and point-in-time membership accompany the features.
Exports are source- and schema-specific. Lifecycle states, predictions, and outcomes
are excluded from the feature allowlist.

The `prints-v1` label policy evaluates frozen candidates, including rejected setups,
against recorded trades. Paper fills use participation limits, latency, fees,
slippage, and a funding reserve. Outcomes exit at TP1, stop, or the four-hour
deadline. TP2 is informational. Missing continuity, late exits, and unresolved
positions do not become complete training outcomes.

The worker preflights committed raw hashes, manifests, row counts, bounds, and
ordering. Missing, unreadable packed, clock-damaged, and overlapping intervals become
audited global gaps. Original files and packs remain unchanged. Hash and manifest
mismatches on readable evidence halt processing.
Strict explicit replay continues to reject unordered input.

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
starts its first fit on the next loop after reaching 500 usable outcomes. Failed
fits retry after 15 minutes or a change in the usable count. After a successful
fit, another cycle needs 50 new outcomes or seven days. A manual cycle can also
request a fit. Chronological partitions can still leave too few samples; the
worker reports that reason instead of weakening the checks.

## Historical bootstrap track

`ohlc-path-v1` evaluates the original venue's public, fully closed one-minute
candles against the frozen entry zone, stop, TP1, entry deadline and holding
horizon. TARGET, STOP and TIME_EXIT can become complete outcomes with finite net
R after the frozen cost reserve. NO_ENTRY, AMBIGUOUS and INCOMPLETE never become
training losses. A candle that cannot establish the order of entry, stop and
target is excluded. Partial decision and deadline candles do not supply invented
intrabar ordering. Event availability is the relevant candle close, separate
from the time a historical label was materialized.

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

From CMD in the repository directory, inspect or run bounded resumable work:

```bat
docker compose exec trainer bybit-flow ml bootstrap-labels --source binance --limit 500 --resume --dry-run
docker compose exec trainer bybit-flow ml bootstrap-labels --source binance --limit 500 --resume
docker compose exec trainer bybit-flow ml bootstrap-export --source binance
docker compose exec trainer bybit-flow ml bootstrap-train --source binance
docker compose exec trainer bybit-flow ml status
```

Use `bybit` or `okx` only for candidates frozen on that venue. The worker already
runs bounded backfill automatically. Overlapping symbol ranges are merged, REST
requests use an independent one-request-per-second limiter and bounded retries,
and progress survives restarts. A small Zstandard Parquet candle cache can be
reclaimed after labels are durable. Run manual fitting with the worker stopped
to avoid competing experiments.

## Storage and replay checkpoints

Primary incremental checkpoints preserve positions, subscriptions, trade IDs,
continuity state, input hashes and the consumed receipt cursor. A fresh, sealed,
consistent checkpoint protects only the unprocessed tail plus a 15-minute replay
margin for an unresolved primary outcome. Missing, stale or inconsistent
checkpoints retain the full original interval. Late-published segments remain
protected until a later checkpoint consumes them. Operational active setups keep
their original evidence protection and lifecycle.

At 95% storage usage, the worker prioritizes incremental outcomes and integrity-safe
pruning. Bootstrap downloads and fitting wait for headroom. Permanent snapshots,
labels, model artifacts, manifests and audit hashes are retained. Status reports
protected intervals and checkpoint boundaries; storage pressure cannot be
described as healthy merely because the worker heartbeat is fresh. SQLite busy
writes retry briefly and defer the worker loop without a crash/restart cycle.

For a Windows deployment of this repair, PowerShell 7 can run the checked script
from CMD in the repository directory:

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
groups. The worker rescans retained segments; incremental checkpoints are not
implemented. Large histories require capacity planning.

Historical `chart-v1` rows remain readable for audit, but TradingView ingestion and
new chart labels are retired. Manual journal outcomes are not automatically imported
into training.

Datasets, recordings, models, audit files, and failed experiments belong in persistent
research storage, outside Git. See [operations](OPERATIONS.md) for backup and restore.

## Two-stage outcome models

When two-stage mode is enabled but fewer than 500 compatible complete sequences are
available, the worker attempts the Logistic Regression/LightGBM tabular baseline.
Chronological partitions and class requirements remain unchanged. Feature schema
`candidate-v10` includes causal flow-quality, market-alignment and V8 research fields; historical snapshots
remain immutable and incompatible schemas are not silently pooled for training.
Advisory inference skips incompatible or degraded artifacts. Models are never
automatically promoted, and deterministic setup scores are not replaced by ML scores.
The V7.1 flow confirmation mode remains signal provenance. V8.1 bumps the schema once from v9 to v10; historical v9 remains unchanged;
training needs enough newly completed, source-specific `candidate-v10` outcomes and
never backfills historical decisions. `python -m bybit_flow.ml.ablation --source binance`
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

The worker checks training readiness every 15 minutes until a challenger exists.
Successful cycles remain weekly. LSTM training is confined to the CPU worker; live
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
