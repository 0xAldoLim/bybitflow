"""Separate causal late-outcome dataset and research-only horizon experiment."""

import json
import math

from .features import CATALOG
from .store import canonical, digest


def dataset(store, asof):
    rows, seen = [], set()
    for signal_id, available in store.db.execute(
        "SELECT signal_id,available_ms FROM research_labels WHERE available_ms<=? ORDER BY available_ms",
        (asof,),
    ).fetchall():
        text = store.db.execute("SELECT payload FROM research_labels WHERE signal_id=?", (signal_id,)).fetchone()[0]
        label = json.loads(text)
        if not label.get("coverage_complete"):
            continue
        snap = store.db.execute(
            "SELECT id,decision_ms,payload FROM ml_snapshots WHERE signal_id=? AND stage='decision' AND decision_ms<=? ORDER BY decision_ms LIMIT 1",
            (signal_id, label.get("terminal_ms", available)),
        ).fetchone()
        if not snap:
            continue
        identity = store.db.execute(
            "SELECT candidate_identity FROM candidate_identities WHERE signal_id=?", (signal_id,)
        ).fetchone()
        opportunity = identity[0] if identity else signal_id
        if opportunity in seen:
            continue
        frozen = json.loads(snap[2])
        # Only the explicit decision feature allowlist is eligible as input.
        values = {key: frozen["values"].get(key) for key in CATALOG}
        targets = {
            key: label.get(key)
            for key in (
                "late_target_hit",
                "extended_same_rules_outcome",
                "best_observed_horizon",
                "timing_classification",
            )
        }
        rows.append(
            dict(
                signal_id=signal_id,
                candidate_identity=opportunity,
                snapshot_id=snap[0],
                decision_ms=snap[1],
                label_available_ms=available,
                values=values,
                targets=targets,
            )
        )
        seen.add(opportunity)
    return sorted(rows, key=lambda r: (r["decision_ms"], r["signal_id"]))


def fit(store, settings, asof):
    rows = dataset(store, asof)
    supported = {"SHORT_INTRADAY", "CORE_INTRADAY", "SWING", "EXTENDED_SWING"}
    labeled = [r for r in rows if r["targets"]["best_observed_horizon"] in supported]
    report = dict(
        status="INSUFFICIENT_EVIDENCE",
        samples=len(labeled),
        dataset_samples=len(rows),
        minimum=settings.horizon_model_min_samples,
        production_enabled=False,
        recommended_horizon=None,
        validation_status="unvalidated",
        target="best observed horizon conditional on coverage-complete labelled followthrough",
        embargo_ms=86_400_000,
    )
    directory = store.root / "ml" / "horizon"
    directory.mkdir(parents=True, exist_ok=True)
    if rows:
        target = directory / (digest(rows) + ".json")
        if not target.exists():
            target.write_text(canonical(rows))
        report["dataset"] = str(target)
    if len(labeled) < settings.horizon_model_min_samples:
        store.put("horizon_model", report)
        return report
    old = store.get("horizon_model", {})
    if old.get("experiment_id") and old.get("samples") == len(labeled):
        return old
    n = len(labeled)
    va, te = int(n * 0.6), int(n * 0.8)
    validation_start, test_start = labeled[va]["decision_ms"], labeled[te]["decision_ms"]
    embargo = report["embargo_ms"]
    train = [r for r in labeled[:va] if r["label_available_ms"] < validation_start - embargo]
    validation = [r for r in labeled[va:te] if r["label_available_ms"] < test_start - embargo]
    test = labeled[te:]
    if (
        min(map(len, (train, validation, test))) < 10
        or len({r["targets"]["best_observed_horizon"] for r in train}) < 2
    ):
        report["reason"] = (
            "Insufficient chronological class coverage after outcome purging and one-day embargo"
        )
        store.put("horizon_model", report)
        return report
    # A consumed holdout cannot become a later training or validation partition.
    used = store.get("horizon_holdouts", [])
    if any(start <= r["decision_ms"] <= end for start, end in used for r in train + validation + test):
        report.update(
            status="HOLDOUT_RESERVED",
            reason="Previously consumed holdout cannot be reused in this experiment",
        )
        store.put("horizon_model", report)
        return report
    import numpy as np
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import balanced_accuracy_score
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    keys = sorted(CATALOG)

    def x(part):
        return np.array(
            [
                [
                    float(r["values"][k])
                    if isinstance(r["values"].get(k), (int, float)) and math.isfinite(r["values"][k])
                    else np.nan
                    for k in keys
                ]
                for r in part
            ]
        )

    def y(part):
        return [r["targets"]["best_observed_horizon"] for r in part]

    model = make_pipeline(
        SimpleImputer(strategy="median", keep_empty_features=True),
        StandardScaler(),
        LogisticRegression(max_iter=500, C=0.1, random_state=0),
    )
    model.fit(x(train), y(train))
    # Fixed model: validation does not tune it, and holdout is opened exactly once.
    validation_score = balanced_accuracy_score(y(validation), model.predict(x(validation)))
    store.put("horizon_holdouts", used + [[test[0]["decision_ms"], test[-1]["decision_ms"]]])
    test_score = balanced_accuracy_score(y(test), model.predict(x(test)))
    experiment = digest([r["snapshot_id"] for r in labeled])
    fitted = model.steps[-1][1]
    artifact = dict(
        keys=keys,
        classes=fitted.classes_.tolist(),
        coefficients=fitted.coef_.tolist(),
        intercept=fitted.intercept_.tolist(),
        imputation=model.steps[0][1].statistics_.tolist(),
        mean=model.steps[1][1].mean_.tolist(),
        scale=model.steps[1][1].scale_.tolist(),
        partitions={
            name: [r["snapshot_id"] for r in part]
            for name, part in (("train", train), ("validation", validation), ("test", test))
        },
    )
    (directory / (experiment + "-model.json")).write_text(canonical(artifact))
    report.update(
        status="RESEARCH_ONLY",
        experiment_id=experiment,
        validation_balanced_accuracy=validation_score,
        test_balanced_accuracy=test_score,
        partition_counts=[len(train), len(validation), len(test)],
        reason="Research experiment only; no production promotion or active-plan mutation",
    )
    store.put("horizon_model", report)
    return report
