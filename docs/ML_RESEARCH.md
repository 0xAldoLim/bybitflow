# ML research

## Collection and labels

Feature schema `candidate-v4` stores immutable generation snapshots and the first
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
ordering. Missing, clock-damaged, and overlapping intervals become audited global
gaps. Original files remain unchanged. Integrity mismatches halt processing.
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

The worker monitors outcomes every 15 minutes, retries unsuccessful training daily,
and schedules successful challenger cycles weekly. Insufficient data produces an
abstention with a reason.

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
`candidate-v7` adds the existing causal flow-response fields; historical snapshots
remain immutable and incompatible schemas are not silently pooled for training.
Advisory inference skips incompatible or degraded artifacts. Models are never
automatically promoted, and deterministic setup scores are not replaced by ML scores.

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
there is no automatic promotion. The 10 GB recording budget and retention policy
remain independent of model choice.

Implementation references: [PyTorch LSTM](https://docs.pytorch.org/docs/2.14/generated/torch.nn.LSTM.html),
[scikit-learn SVC](https://scikit-learn.org/stable/modules/generated/sklearn.svm.SVC.html),
and [Random Forest probabilities](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestClassifier.html).
