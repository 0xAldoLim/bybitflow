"""Dependence-aware evidence checks; degradation never increases risk or position size."""

import json

import numpy as np

from .models import transform
from .registry import Registry
from .validation import cluster_interval


def check(store, model_id, minimum=200):
    registry = Registry(store)
    model = registry.get(model_id)
    records = store.db.execute(
        "SELECT s.payload,p.probability,p.accepted,l.payload AS label FROM ml_predictions p "
        "JOIN ml_snapshots s ON s.id=p.snapshot_id LEFT JOIN ml_labels l "
        "ON l.snapshot_id=s.id AND l.policy='prints-v1' WHERE p.model_id=? "
        "ORDER BY p.at_ms DESC LIMIT 500",
        (model_id,),
    ).fetchall()
    if len(records) < minimum:
        return dict(status="insufficient", n=len(records), minimum=minimum)
    rows = [json.loads(r["payload"]) for r in records]
    x = transform(model["model"], rows)
    shift = abs(x.mean(axis=0) - np.array(model["model"]["reference_mean"])) / np.maximum(
        0.25, np.array(model["model"]["reference_std"])
    )
    reasons = []
    if float((shift > 1).mean()) > 0.2:
        reasons.append("more than 20% of features shifted over one training standard deviation")
    reference = model["model"].get("reference_prediction_histogram")
    psi = None
    if reference:
        observed = np.histogram([r["probability"] for r in records], bins=np.linspace(0, 1, 11))[0] + 1
        expected = np.array(reference) + 1
        observed, expected = observed / observed.sum(), expected / expected.sum()
        psi = float(np.sum((observed - expected) * np.log(observed / expected)))
        if psi > 0.25:
            reasons.append("prediction-distribution PSI exceeds .25")
    rate = float(np.mean([r["accepted"] for r in records]))
    reference_rate = model["model"].get("reference_acceptance", rate)
    if abs(rate - reference_rate) > 0.30:
        reasons.append("acceptance rate changed by more than 30 percentage points")
    labeled, p = [], []
    for row, raw in zip(rows, records, strict=True):
        label = json.loads(raw["label"]) if raw["label"] else None
        if label and label.get("complete") and label.get("net_r") is not None:
            labeled.append(row | {"label": label})
            p.append(raw["probability"])
    if len(labeled) >= minimum:
        ev, clusters = cluster_interval(labeled, [r["label"]["net_r"] for r in labeled])
        if ev and ev[1] < 0:
            reasons.append("recent net expectancy upper bound below zero")
        brier = float(np.mean((np.array(p) - [r["label"]["net_r"] > 0 for r in labeled]) ** 2))
        if clusters >= 8 and brier > model["report"]["all_holdout"]["brier"] + 0.05:
            reasons.append("recent Brier deteriorated by more than .05")
    if reasons and not registry.is_degraded(model_id):
        registry.degrade(model_id, reasons)
    return dict(
        status="degraded" if reasons else "observed",
        n=len(rows),
        reasons=reasons,
        standardized_shift=shift.tolist(),
        prediction_psi=psi,
        acceptance_rate=rate,
        caveat="heuristic drift thresholds, no automatic risk changes",
    )
