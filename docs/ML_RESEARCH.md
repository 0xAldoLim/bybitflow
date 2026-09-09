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
