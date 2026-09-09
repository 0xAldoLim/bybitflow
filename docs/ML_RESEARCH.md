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

SQLite additive schema migrations 3–4 create immutable generation/decision snapshots, labels,
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
avoiding a tiny file per live candidate. Exports refuse more than 10,000 snapshots in memory.

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

## Bybit-unavailable fallback

Save actual received TradingView setups and export real corresponding 15M candles. CSV columns:
`time,open,high,low,close`. Time accepts Unix seconds/milliseconds or timezone-qualified ISO8601.
The account, Premium footprint, domain/HTTPS and Discord instructions remain in
[TRADINGVIEW_SETUP.md](TRADINGVIEW_SETUP.md). Real Pine/alert delivery needs account-side testing.

```sh
# Redirect to a NEW filename; tv-export prints saved inbox records, not generated signals.
bybit-flow tv-export > data/saved-tv-events.jsonl
bybit-flow ml chart-label data/saved-tv-events.jsonl data/actual-BTCUSDT-15m.csv --symbol BTCUSDT
bybit-flow ml chart-export
bybit-flow ml train data/ml/datasets/<printed-hash>.parquet --model both
```

The separate `chart` stage / `chart-v1` policy includes rejected candidates. It assumes entry
at the first subsequent bar open inside the zone, adverse slippage, fees, funding reserve,
stop-first ambiguous bars and at most four hours from fill. Missed/incomplete entries cannot
train. MFE/MAE are OHLC bounds including the exit bar, not known intrabar paths. Native trade
volumes/book data remain null; footprint values exist only in saved source observations.
Symbol correspondence and chart history are operator-attested. This can train a separate
research challenger, never a validated native-execution model. Live shadow use is restricted
to matching TradingView proxy research. Manual journal results are not imported by this command.

## Scoring, uncertainty and promotion

`native-evidence-2` grades continuation flow through directional stacked imbalance, but reversal
flow through actual opposing aggressive notional and matched passive replenishment. Delta or
an absorption boolean alone cannot earn reversal-flow credit. ML learns coefficients/tree
importance and acceptance thresholds; it never turns probability into the raw quality score.

The finite search tests probability minima .50/.60/.70 and quality minima 65/85/95. Its fixed
objective uses lower clustered EV, minus .02×drawdown, a loss-tail penalty, .005×daily signal
frequency and 1/sqrt(selected N). Trials need >=30 selected validation observations and eight
market-week clusters. Every model/ablation/threshold is retained. Bonferroni-adjusted holdout
intervals increase bootstrap draws to resolve the adjusted tails. This does not eliminate
all model-selection bias or dependence across long-lived regimes.

Policy `promotion-v2` requires real data, verified costs, clean code provenance, recorded
point-in-time membership, >=200 selected holdout outcomes and >=78 market-week clusters,
positive adjusted lower EV and incremental-baseline bounds, improved Brier score, three
positive chronological development-fold diagnostics, >=200 calibration observations, positive
supported regime breakdowns, drawdown <=20R and worst-5% mean >=-1.5R. Development folds are
diagnostic and conditional on model selection; only the final holdout is untouched by selection.
Cluster counts are upper bounds on independence, not guarantees.

A live candidate additionally needs its family/direction/regime/probability cohort to have
>=100 held-out outcomes, >=26 clusters, positive EV lower bound and win-frequency interval
width <=.20. The displayed probability is a calibrated model estimate; the interval describes
the historical contextual cohort, not exact individual certainty. Expected R is that cohort's
mean/interval. An optional regression fitting primitive exists but is not deployed.

Subsequent cycles reserve only outcomes strictly after the latest consumed holdout plus
embargo. An old holdout may become past training data, never a new unseen evaluation.
Reservations are transactional. Creating a fresh database to reuse an inspected holdout
would invalidate the research, even though a local administrator can control their own files.

## Operations and migration

```sh
pip install -r requirements-ml.lock
bybit-flow ml worker
# Or, with .env and FLOW_ADMIN_TOKEN configured:
docker compose --profile ml up --build -d
docker compose --profile ml logs --tail 100 trainer
docker compose --profile ml exec trainer bybit-flow ml status
```

One separate worker resolves recorded labels/checks drift every 15 minutes and attempts a
challenger weekly. Insufficient new data may cause many abstaining cycles. Feature/prediction
distribution, acceptance rate, Brier and net-EV deterioration have heuristic monitors. Unknown
source/context/strategy version, changed feature schema, missing normally present features,
future model availability or evidence older than 90 days abstains. Degradation persists per
model in history and cannot disappear when another model degrades. There is no automatic
promotion or rollback. Named-reviewer commands cannot waive statistical gates. Model and
deployment cards preserve evidence; operations notices use a separate optional webhook.

Before upgrade, stop collector/worker and run `bybit-flow backup /absolute/new-backup.sqlite`.
Also back up `data/ml`, raw segments and manifests. Tables are additive; old signals/recordings
are preserved. `candidate-v3` includes source-specific TV/context metadata and explicitly signed
confirmed BOS/CHoCH (earlier snapshots remain untouched). Older schemas are
not silently converted into new models. Restore a verified backup into a new data directory
with both processes stopped, never over an active SQLite database. PostgreSQL remains future work.

Training limits: 10,000 rows, 128MB Parquet file, 256MB uncompressed row groups. Chart inputs:
64MB per file, 10,000 events and 250,000 bars. Limits fail explicitly without silent sampling.
The labeler still rescans retained segments: large histories need offline maintenance windows;
incremental replay checkpoints are not implemented. The trainer's one-CPU/2GB container limits
are not measured capacity guarantees. Datasets/models belong in ignored `data/`, backed up
separately from Git. Do not store software-test experiments in the live research directory.

## Remaining limitations and audited libraries

- No genuine training dataset, fitted production model, approved champion or actual Discord
  delivery exists here. Host and Docker both fail Bybit hostname verification. No TLS, DNS or
  regional workaround was introduced.
- Print labels reserve funding rather than verifying settlements. A verified-cost label adapter
  plus independent evidence is still needed before deployment can pass its gate.
- The feature allowlist is a subset: regimes/ATR/efficiency/slope/volume, sweep flags, footprint
  aggregates, visible depth/replenishment, OI/funding, execution costs, fact coverage and TV
  observations. Full level ages, volatility percentiles, liquidation aggregates, tokenomics,
  BTC/ETH encoding and advanced book persistence are not yet ML inputs. Original evidence is
  retained for future schema work. No candle-derived substitute is labeled exchange-native.
- No automatic SMC/imbalance rule tuning or score-weight rewriting. Current bounded adaptation
  affects only research acceptance thresholds, never leverage, account risk or data gates.
- No financial improvement is established. More data, stricter filters or more training do not
  guarantee improvement. No fitted test model or synthetic dataset is committed.

Audited official [scikit-learn calibration documentation](https://scikit-learn.org/stable/modules/calibration.html)
for disjoint calibration data and [LightGBM Booster documentation](https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.Booster.html)
for JSON tree export. Pinned installed releases: scikit-learn 1.9.0, LightGBM 4.7.0, SciPy 1.18.1.
Consult upstream projects for their licenses.
