# ML research implementation and audit

## Repository audit, 2026-09-09

Started from clean, freshly pulled `origin/main` (`9690cd3`). The existing tested TradingView
gateway and measured scoring were on `feature/tradingview-research-gateway` (`f238250`), not
main. Integrated that history by fast-forward; no modules or history were discarded.
Inspected scoring, calibration, strategy, features, orderflow, scanner, replay, backtest,
storage, risk, notifications, configuration, research and verification documentation.

Main's original constant score fractions were unsuitable ML inputs. The preserved measured
native and TradingView rubrics are separate profiles, not probabilities. The new grade table
adds E (20–34); F is 0–19 or mandatory rejection. Grades remain heuristics.

Existing replay simulated only accepted candidates. Its outcome set therefore cannot estimate
the benefit of rejecting a candidate. Existing paper fills already use real prints, adverse
slippage, participation limits and explicit gaps; these mechanics are reused, not replaced.
Existing TradingView manual outcomes lack verified costs/fills and remain excluded from training.

## Foundation

SQLite additive schema migration 3 creates immutable generation/decision snapshots, labels,
model history and holdout reservations. No retrospective backfill from mutable signal rows:
collection starts with this version. Every stored candidate is captured; first scored decisions,
including quality rejects, are frozen separately. Candidates never reaching a scored decision
remain generation snapshots with explicitly missing flow/execution features.

The explicit feature allowlist excludes lifecycle, rejection flags, predictions and outcomes.
Numerical fields have definitions, source, event/availability timestamps, schema and missing
status. Unknown features remain null. Actual TradingView classification is not native taker
classification. Source-attested availability is not independent verification of Pine history.
Context and unimplemented feature limitations are documented alongside each dataset.

Parquet exports preserve each entire snapshot and label as canonical JSON plus indexed identity
and time columns; DuckDB can query JSON fields directly. Content hashes identify immutable
datasets. SQLite is the durable small-record write buffer; Parquet is the research export,
avoiding a tiny file per live candidate. Exports refuse more than 100,000 snapshots in memory.

`prints-v1` labels replay all frozen candidates independently, including rejects, using
PaperPosition. They retain actual simulated fill timestamp, planned/filled price, MFE/MAE,
costs, partial/missed fill, stop/TP1 and max-four-hour policy. Unresolved labels are not saved
as trainable outcomes. Gaps and late exits are incomplete. The funding reserve is explicitly an
assumption, not reconstructed settlements; these labels alone cannot approve validated SSS.

## Verification boundary

Foundation regression suite: 57 passing software tests. No financial performance inferred.
The official Bybit HTTPS endpoint still fails hostname verification on this host; TLS stays
enabled. No real market training dataset or Discord delivery is claimed.

## Supervised pipeline milestone

Optional pinned training dependencies are in `requirements-ml.lock`. Training uses a sklearn
DictVectorizer → median imputer with missing indicators → standard scaler → regularized
logistic or shallow LightGBM pipeline. All preprocessing fits training data only. Sigmoid
calibration needs >=100 separate calibration labels; isotonic needs >=1000. Models are stored
as JSON coefficients or LightGBM's documented JSON tree dump, never pickle. The lightweight
live image needs neither sklearn nor LightGBM to evaluate these artifacts.

Run `bybit-flow ml label <actual.jsonl.gz> ...`, `bybit-flow ml export`, then
`bybit-flow ml train data/ml/datasets/<hash>.parquet`. Use `--model logistic` for the first
baseline. `ml status`, `ml infer MODEL_ID DATASET`, `ml promote MODEL_ID --reviewer NAME`,
`ml rollback --reviewer NAME`, `ml drift MODEL_ID` and `ml worker` are available. Model
promotion refuses insufficient/synthetic evidence regardless of reviewer or configuration.

Chronological 50/15/15/20 development/calibration/selection/holdout windows purge outcome and
label-availability overlaps plus a four-hour embargo. Three expanding-window checks, nine
bounded probability/quality thresholds, no-SMC/no-flow ablations, deterministic/simple/base-rate
comparisons and clustered uncertainty are recorded. Threshold selection is a finite exhaustive
search, not Optuna; only the two declared non-safety parameters are writable. The holdout is
reserved durably before scoring the frozen winner. Its period cannot be reused under another
file name or data hash. A later research cycle needs genuinely new unseen holdout data.

Set `FLOW_ML_ENABLED=true` for shadow inference; `FLOW_ML_FILTER_RESEARCH=true` additionally
lets the model reject research candidates. Neither changes raw quality or risk limits.
Unapproved models never populate public calibrated probability/expected-value fields.
The authenticated dashboard now has an ML Research view, and Discord cards show model status
and explanations when available. `FLOW_OPS_WEBHOOK` is a separate optional operational channel.

Default manual model approval is intentional. Weekly challenger creation runs in a separate
worker process with an advisory lock, not in the market ingestion loop. Registry histories and
model cards are append-only. Public Discord admission checks registry approval and recent
persisted inference, not just a flag in a signal. Current cost-assumed labels are insufficient
for validated SSS; the remaining verification requirements are not bypassed.

This milestone passed 65 software tests, including logistic/LightGBM JSON parity, chronology,
holdout reuse, tamper rejection, rejected-candidate labels, gaps, API authentication and mock
Discord operations. The runtime Docker image builds. Real collection tables remain empty.
