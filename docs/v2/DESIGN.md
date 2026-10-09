# BybitFlow 2.0 production design

This design follows the archive ledger and baseline code audit. It is an evidence architecture upgrade, not a profitability claim. Versioned numeric mappings are **implementation inferences**. Strategy and safety gates are not lowered to manufacture signals.

## Single new-candidate path

The existing four-family scanner creates one plan. Existing closed structure, actual footprint/profile, flow quality, participation, market factors, risk and V8 gates remain authoritative. A bounded incremental tape summary adds prior-only trade-size buckets and closed activity rotations. A compact assessment interprets location, family-specific mechanism and after-cost trade offer from these shared observations.

New plans use `candidate-v20`, product version `2.0.0`, score profile `bybitflow-v2-evidence-1` and formula revision `v2-score-1`. There is no live v1 comparison scorer or second candidate engine.

The initial quality formula is:

```text
weighted = .30 * location + .45 * mechanism + .25 * trade_offer
raw = .80 * weighted + .20 * min(location, mechanism, trade_offer)
quality = min(raw, evidence_confidence)
```

Quality is not win probability. Alternative flow descriptions take the strongest supported pathway, not their sum. Missing evidence cannot add points or redistribute weights. Native and cross-venue substitution evidence remain explicit; substitution does not manufacture native trade-size or event features. Familiar tier cutoffs remain usable, but persisted score profiles distinguish their new meaning.

## Cutover and continuity

Initialize `bybitflow_v2_cutover` once with SQLite insert-if-absent. Persist epoch, deployed code identity, schema/profile and SHA256 hashes of the manifest and ledger. Restart returns the original marker. Do not create a cutover just by opening the database for a CLI audit; initialize when the production scanner starts.

All existing active plans are immutable in their creation version. The storage layer protects entry, zone, stop, targets, thesis, timing, family, direction, horizon and score before snapshot capture. The scanner skips new-decision scoring/qualification for these objects. Original-source lifecycle and reconciliation continue, including internally monitored plans and deferred delivery of an already-confirmed original decision. No deployment-induced reconfirmation or initial delivery for an unconfirmed legacy object.

New candidates must not recreate the same still-active original structural opportunity. Preserve economic identity and explicit original-plan association without retargeting, reclustering or pausing that original plan.

## Learning and storage

Snapshots use the object's stored schema, with the v10 feature catalog preserved. Immutable old snapshots and labels are not rewritten. Monitored historical-candle recovery continues across supported stored schemas; learning datasets stay separately selected by schema and source. An incompatible v1 model cannot be used on v20. Existing bootstrap/monitored fidelity distinctions remain visible.

V20 primary model fitting requires >=500 unique complete source-specific `prints-v1` outcomes and existing chronology, embargo, class and partition checks. Deterministic production does not wait for model maturity.

Raw tape/event-construction buffers are bounded and temporary. Persist compact candidate features, provenance, outcomes and relevant transitions; keep existing retention. The archive and generated OCR/transcripts are local review artifacts and are not repository assets.

## Validation and deployment

Test causal size quantiles and source separation, immutable event closes and bounded state, family-specific mechanism semantics, no fade credit during accelerating liquidations, path friction, score bottlenecks/missing data, model compatibility and old-plan continuity. Reuse existing beta/leakage/Discord/retention regressions.

Run focused checks, then the full verifier once. Push main and require both CI Python jobs green before rebuilding services with preserved volumes. Compare active frozen plans, historical counts/receipts, new v20 snapshots, duplicates, latency, memory, feed lag and recorder health over 5–10 minutes. Record actual observations; do not claim live signal delivery unless a real qualified card was delivered.
