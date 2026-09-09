"""Offline training; untouched holdout is consumed only after a winner is frozen."""

import json
import subprocess
import uuid
from importlib.metadata import version

import numpy as np
import pyarrow.parquet as pq

from ..storage import now_ms
from . import POLICY_VERSION, SCHEMA_VERSION
from .models import calibrate, fit, predict
from .store import digest
from .validation import accepted, breakdown, cluster_interval, metrics, optimize, partitions, walk_forward


def read_dataset(path, max_rows=100_000):
    file = pq.ParquetFile(path)
    if file.metadata.num_rows > max_rows or path.stat().st_size > 512_000_000:
        raise ValueError("Dataset exceeds offline worker bounds")
    rows = [
        json.loads(r["payload"]) for batch in file.iter_batches(batch_size=1000) for r in batch.to_pylist()
    ]
    if not rows:
        raise ValueError("No training rows")
    if len({r["source"] for r in rows}) != 1 or len({r["stage"] for r in rows}) != 1:
        raise ValueError("Train source/stage-specific models; no silent TradingView/native pooling")
    for row in rows:
        label = row["label"]
        if (
            row["schema_version"] != SCHEMA_VERSION
            or not label["complete"]
            or not np.isfinite(label["net_r"])
            or not row["decision_ms"] < label["exit_ms"] <= row["label_available_ms"] <= now_ms()
        ):
            raise ValueError("Unresolved, future, nonfinite or incompatible training label")
        if any(
            not meta["missing"] and max(meta["available_ms"], meta["source_ms"]) > row["decision_ms"]
            for meta in row["feature_metadata"].values()
        ):
            raise ValueError("Future feature leakage")
    return sorted(rows, key=lambda r: r["decision_ms"])


def train(store, path, kinds=("logistic", "lightgbm"), calibration="sigmoid"):
    rows = read_dataset(path)
    train_rows, cal_rows, validation, holdout = partitions(rows)
    if len(train_rows) < 200 or len(cal_rows) < 100 or len(validation) < 100 or len(holdout) < 100:
        raise ValueError("Need >=200 train, 100 independent calibration, 100 validation, 100 holdout labels")
    ident = uuid.uuid4().hex
    start, end = holdout[0]["decision_ms"], max(r["label_available_ms"] for r in holdout)
    # Never reuse a prior holdout even if the dataset filename/hash changes.
    if store.db.execute("SELECT 1 FROM ml_holdouts WHERE start_ms<=? AND end_ms>=?", (end, start)).fetchone():
        raise ValueError("Holdout overlaps a consumed period; collect new unseen outcomes")
    base_rate = np.mean([r["label"]["net_r"] > 0 for r in train_rows])
    reports, candidates = [], []
    for kind in kinds:
        for excluded in ((), ("orderflow",), ("smc",)):
            artifact, _ = fit(train_rows, kind, excluded)
            calibrate(artifact, cal_rows, calibration)
            p = predict(artifact, validation)
            best, trials = optimize(validation, p)
            reports.append(
                dict(
                    kind=kind,
                    excluded=list(excluded),
                    validation=metrics(validation, p),
                    uncalibrated=metrics(validation, predict(artifact, validation, False)),
                    trials=trials,
                )
            )
            if best:
                candidates.append((best["objective"], artifact, best["parameters"], len(reports) - 1))
    # Prefer the interpretable full logistic baseline on ties. Model selection uses validation only.
    if candidates:
        _, winner, parameters, chosen = max(candidates, key=lambda x: (x[0], -x[3]))
    else:
        winner, _ = fit(train_rows, "logistic")
        calibrate(winner, cal_rows, calibration)
        parameters, chosen = dict(min_probability=0.7, min_quality=95), None
    # Record reservation BEFORE reading holdout labels for performance. A crash does not refund it.
    with store.db:
        store.db.execute("INSERT INTO ml_holdouts VALUES(?,?,?)", (start, end, ident))
    p = predict(winner, holdout)
    selected = [r for r, value in zip(holdout, p, strict=True) if accepted(r, value, parameters)]
    selected_p = [value for r, value in zip(holdout, p, strict=True) if accepted(r, value, parameters)]
    deterministic = [r for r in holdout if not r["signal"]["gates"] and r["signal"]["risk"].get("accepted")]
    simple = [
        r
        for r in deterministic
        if r["values"].get("h4_efficiency", 0) is not None and r["values"].get("h4_efficiency", 0) >= 0.3
    ]
    # Selected exposure vs frozen deterministic exposure, counting abstention as 0 R per opportunity.
    differences = [
        (r["label"]["net_r"] if accepted(r, value, parameters) else 0)
        - (r["label"]["net_r"] if not r["signal"]["gates"] and r["signal"]["risk"].get("accepted") else 0)
        for r, value in zip(holdout, p, strict=True)
    ]
    incremental_ci, _ = cluster_interval(holdout, differences, alpha=0.05 / max(1, len(reports) * 9))
    wf = []
    for tr, ca, te in walk_forward(rows[: rows.index(holdout[0])]):
        try:
            m, _ = fit(tr, winner["kind"], winner["excluded"])
            calibrate(m, ca, calibration)
            wf.append(dict(train=len(tr), calibration=len(ca), test=metrics(te, predict(m, te))))
        except ValueError as exc:
            wf.append(dict(status="insufficient", reason=str(exc)))
    winner["thresholds"] = parameters
    final = metrics(selected, selected_p, alpha=0.05 / max(1, len(reports) * 9))
    cohorts = {}
    for lower in np.arange(0, 1, 0.1):
        cohort = [r for r, value in zip(holdout, p, strict=True) if lower <= value < lower + 0.1]
        cohorts[str(round(float(lower), 1))] = metrics(cohort)
    report = dict(
        out_of_sample=True,
        holdout=final,
        all_holdout=metrics(holdout, p),
        constant=metrics(holdout, np.full(len(holdout), base_rate)),
        deterministic=metrics(deterministic),
        simple_trend=metrics(simple),
        uncalibrated_holdout=metrics(holdout, predict(winner, holdout, False)),
        incremental_ev_interval=incremental_ci,
        walk_forward=wf,
        experiments=reports,
        selected_experiment=chosen,
        breakdown=breakdown(holdout, p),
        cohorts=cohorts,
        alpha=0.05 / max(1, len(reports) * 9),
        selection_trials=len(reports) * 9,
        interpretation="Heldout cost-assumed paper outcomes, not executed trades or proof of edge",
    )
    from .registry import Registry

    champion = Registry(store).champion()
    if champion and start > champion["periods"]["holdout"]["end"]:
        cp = predict(champion["model"], holdout)
        delta = [
            r["label"]["net_r"]
            * (int(accepted(r, np_, parameters)) - int(accepted(r, old, champion["model"]["thresholds"])))
            for r, np_, old in zip(holdout, p, cp, strict=True)
        ]
        report["champion_comparison"] = dict(
            model_id=champion["id"], incremental_interval=cluster_interval(holdout, delta, report["alpha"])[0]
        )
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
    except (OSError, subprocess.CalledProcessError):
        commit, dirty = "unavailable", True
    manifest = dict(
        id=ident,
        created_ms=now_ms(),
        policy_version=POLICY_VERSION,
        feature_schema_version=SCHEMA_VERSION,
        source=rows[0]["source"],
        stage=rows[0]["stage"],
        dataset_hash=digest(rows),
        code_commit=commit,
        code_dirty=dirty,
        packages={name: version(name) for name in ("numpy", "scipy", "scikit-learn", "lightgbm")},
        periods={
            name: dict(
                start=group[0]["decision_ms"], end=max(r["label_available_ms"] for r in group), n=len(group)
            )
            for name, group in zip(
                ("training", "calibration", "validation", "holdout"),
                (train_rows, cal_rows, validation, holdout),
                strict=True,
            )
        },
        contexts={
            key: sorted({r["values"].get(key, "unknown") for r in train_rows})
            for key in ("family", "direction", "regime", "score_profile")
        },
        minimum_coverage=min(r["data_coverage"] for r in train_rows),
        real_data=all(r["label"].get("data_kind") == "recorded-public-prints" for r in rows),
        verified_costs=all(r["label"].get("costs_verified") is True for r in rows),
        universe_scope=sorted({r["universe_scope"] for r in rows}),
        model=winner,
        report=report,
        status="challenger",
        validated=False,
    )
    Registry(store).register(manifest)
    return manifest
