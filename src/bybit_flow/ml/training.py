"""Offline training; untouched holdout is consumed only after a winner is frozen."""

import json
import subprocess
import uuid
from collections import defaultdict
from importlib.metadata import version

import numpy as np
import pyarrow.parquet as pq

from ..storage import now_ms
from . import POLICY_VERSION, SCHEMA_VERSION
from .models import calibrate, fit, predict
from .store import digest
from .validation import (
    accepted,
    breakdown,
    cluster_interval,
    metrics,
    optimize,
    walk_forward,
)


def read_dataset(path, max_rows=10_000, track="primary"):
    from .monitored import POLICY as MONITORED_POLICY

    if track not in {"primary", "bootstrap", "monitored"}:
        raise ValueError("Unknown model track")
    file = pq.ParquetFile(path)
    if (
        file.metadata.num_rows > max_rows
        or path.stat().st_size > 128_000_000
        or sum(file.metadata.row_group(i).total_byte_size for i in range(file.metadata.num_row_groups))
        > 256_000_000
    ):
        raise ValueError("Dataset exceeds offline worker bounds")
    rows = [
        json.loads(r["payload"]) for batch in file.iter_batches(batch_size=1000) for r in batch.to_pylist()
    ]
    if not rows:
        raise ValueError("No training rows")
    if len({r["source"] for r in rows}) != 1 or len({r["stage"] for r in rows}) != 1:
        raise ValueError("Train source/stage-specific models; no silent TradingView/native pooling")
    for row in rows:
        from .bootstrap_features import SCHEMA as BOOTSTRAP_SCHEMA

        label = row["label"]
        if (
            row["schema_version"] != (BOOTSTRAP_SCHEMA if track == "bootstrap" else SCHEMA_VERSION)
            or not label["complete"]
            or not np.isfinite(label["net_r"])
            or not row["decision_ms"] < label["exit_ms"] <= row["label_available_ms"] <= now_ms()
        ):
            raise ValueError("Unresolved, future, nonfinite or incompatible training label")
        policy = {"bootstrap": "ohlc-path-v1", "monitored": MONITORED_POLICY}.get(track, "prints-v1")
        if label.get("policy", "prints-v1") != policy:
            raise ValueError("Mixed or incompatible label policies")
        if track == "bootstrap" and (
            label.get("execution_fidelity") != "proxy"
            or label.get("data_kind") != "original-venue-public-1m-OHLC"
            or label.get("event_available_ms") != row["label_available_ms"]
            or label.get("source") != row["source"]
            or label.get("costs_verified") is not False
        ):
            raise ValueError("Invalid bootstrap provenance/availability")
        if track == "monitored" and (
            label.get("execution_fidelity") != "proxy"
            or label.get("data_kind") != "original-venue-public-1m-OHLC"
            or label.get("source") != row["source"]
            or label.get("costs_verified") is not False
            or label.get("account_fill_verified") is not False
            or label.get("production_execution_verified") is not False
            or label.get("entry_basis") not in {"recorded-live-price-touch", "historical-zone-touch"}
            or not row["decision_ms"] <= label.get("event_available_ms", -1) <= row["label_available_ms"]
            or not label.get("materialized_ms", 0) <= row["label_available_ms"]
        ):
            raise ValueError("Invalid monitored outcome provenance/availability")
        if any(
            not meta["missing"] and max(meta["available_ms"], meta["source_ms"]) > row["decision_ms"]
            for meta in row["feature_metadata"].values()
        ):
            raise ValueError("Future feature leakage")
    return sorted(rows, key=lambda r: (r["decision_ms"], r["id"]))


def train(store, path, kinds=("logistic", "lightgbm"), calibration="sigmoid", track="primary"):
    from .locking import exclusive

    with exclusive(store.root / "ml-fit.lock"):
        return _train(store, path, kinds, calibration, track)


def _train(store, path, kinds, calibration, track):
    if track not in {"primary", "bootstrap", "monitored"}:
        raise ValueError("Unknown model track")
    rows = read_dataset(path, track=track)
    if len({r.get("candidate_identity", r["id"]) for r in rows}) != len(rows):
        raise ValueError("Duplicate candidate identities in training data")
    if len(rows) < 500:
        raise ValueError("Need >=500 complete unique outcomes")
    if track == "bootstrap" and any(k.startswith("two_stage_") for k in kinds):
        raise ValueError("Bootstrap uses tabular baselines; sequence threshold is not relaxed")
    schema = rows[0]["schema_version"]
    label_policy = {"bootstrap": "ohlc-path-v1", "monitored": "monitored-ohlc-v1"}.get(track, "prints-v1")
    scope = "|".join((track, rows[0]["source"], schema, label_policy))
    scopes = (scope, "legacy-primary") if track == "primary" else (scope, scope)
    sequence_excluded = 0
    if all(k.startswith("two_stage_") for k in kinds):
        eligible = [r for r in rows if len(r.get("sequence", [])) == 16]
        sequence_excluded = len(rows) - len(eligible)
        rows = eligible
        if len(rows) < 500:
            raise ValueError(
                f"Two-stage models need >=500 complete outcomes with 16-observation sequences; available {len(rows)}"
            )
    from .operations import cache_partition_feasibility

    partition_plan, groups = cache_partition_feasibility(
        store, rows, track, rows[0]["source"], schema, label_policy, include_groups=True
    )
    if partition_plan["status"] != "READY":
        raise ValueError(partition_plan["reason"])
    fit_plan = store.get(f"ml_model_fit_feasibility:{track}:{rows[0]['source']}:{schema}:{label_policy}")
    if not fit_plan["fit_ready"]:
        raise ValueError(f"{fit_plan['blocker']}: {fit_plan['reason']}")
    train_rows, cal_rows, validation, holdout = groups
    if len(train_rows) < 200 or len(cal_rows) < 100 or len(validation) < 100 or len(holdout) < 100:
        raise ValueError("Need >=200 train, 100 independent calibration, 100 validation, 100 holdout labels")
    ident = uuid.uuid4().hex
    start, end = holdout[0]["decision_ms"], max(r["label_available_ms"] for r in holdout)
    # Never reuse a prior holdout even if the dataset filename/hash changes.
    if store.db.execute(
        "SELECT 1 FROM ml_holdouts WHERE scope IN (?,?) AND start_ms<=? AND end_ms>=?", (*scopes, end, start)
    ).fetchone():
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
        winner, _ = fit(
            train_rows, kinds[0] if all(k.startswith("two_stage_") for k in kinds) else "logistic"
        )
        calibrate(winner, cal_rows, calibration)
        parameters, chosen = dict(min_probability=0.7, min_quality=95), None
    # Record reservation BEFORE reading holdout labels for performance. A crash does not refund it.
    with store.db:
        store.db.execute("BEGIN IMMEDIATE")
        if store.db.execute(
            "SELECT 1 FROM ml_holdouts WHERE scope IN (?,?) AND start_ms<=? AND end_ms>=?",
            (*scopes, end, start),
        ).fetchone():
            raise ValueError("Holdout concurrently consumed by another experiment")
        store.db.execute(
            "INSERT INTO ml_holdouts(start_ms,end_ms,experiment_id,scope) VALUES(?,?,?,?)",
            (start, end, ident, scope),
        )
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
    development = [r for r in rows if r["decision_ms"] < holdout[0]["decision_ms"]]
    for tr, ca, te in walk_forward(development):
        try:
            m, _ = fit(tr, winner["kind"], winner["excluded"])
            calibrate(m, ca, calibration)
            wf.append(dict(train=len(tr), calibration=len(ca), test=metrics(te, predict(m, te))))
        except ValueError as exc:
            wf.append(dict(status="insufficient", reason=str(exc)))
    winner["thresholds"] = parameters
    reference_p = predict(winner, cal_rows)
    winner["reference_prediction_histogram"] = np.histogram(reference_p, bins=np.linspace(0, 1, 11))[
        0
    ].tolist()
    winner["reference_acceptance"] = float(
        np.mean([accepted(r, value, parameters) for r, value in zip(cal_rows, reference_p, strict=True)])
    )
    final = metrics(selected, selected_p, alpha=0.05 / max(1, len(reports) * 9))
    cohorts = {}
    for lower in np.arange(0, 1, 0.1):
        cohort = [r for r, value in zip(holdout, p, strict=True) if lower <= value < lower + 0.1]
        cohorts[str(round(float(lower), 1))] = metrics(cohort)
    context_groups = defaultdict(list)
    for row, probability in zip(holdout, p, strict=True):
        key = "|".join(
            [
                row["signal"]["family"],
                row["signal"]["direction"],
                row["signal"]["regime"],
                str(min(9, int(probability * 10))),
            ]
        )
        context_groups[key].append(row)
    report = dict(
        partition_feasibility=partition_plan,
        model_fit_feasibility=fit_plan,
        sequence_excluded=sequence_excluded,
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
        context_cohorts={key: metrics(group) for key, group in context_groups.items()},
        alpha=0.05 / max(1, len(reports) * 9),
        selection_trials=len(reports) * 9,
        interpretation="Heldout cost-assumed paper outcomes, not executed trades or proof of edge",
    )
    from .registry import Registry

    champion = Registry(store).champion()
    if (
        champion
        and track == "primary"
        and champion.get("source") == rows[0]["source"]
        and start > champion["periods"]["holdout"]["end"]
    ):
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
        feature_schema_version=schema,
        track=track,
        label_policy=label_policy,
        holdout_scope=scope,
        label_fidelity={"bootstrap": "OHLC_PROXY", "monitored": "MONITORED_OHLC_PROXY"}.get(
            track, "RECORDED_PRINTS"
        ),
        promotion_eligible=False if track != "primary" else None,
        production_filter_eligible=False if track != "primary" else None,
        strategy_versions=sorted({r["signal"]["version"] for r in rows}),
        source=rows[0]["source"],
        stage=rows[0]["stage"],
        dataset_hash=digest(rows),
        code_commit=commit,
        code_dirty=dirty,
        packages={
            name: version(name)
            for name in ("numpy", "scipy", "scikit-learn", "lightgbm")
            + (("torch",) if winner["kind"].startswith("two_stage_") else ())
        },
        training_spec=dict(
            seed=7,
            logistic_C=0.1,
            lgbm_trees=80,
            lgbm_leaves=7,
            lgbm_max_depth=3,
            lgbm_learning_rate=0.03,
            lgbm_min_child_samples=30,
            lgbm_l2=5,
            threads=1,
            calibration=calibration,
            embargo_ms=14_400_000,
            partition_planner=partition_plan["partition_policy_used"],
            partition_policy_used=partition_plan["partition_policy_used"],
            fallback_reason=partition_plan["fallback_reason"],
            partition_requirements=partition_plan["requirements"],
            partition_boundaries=partition_plan["chosen_boundaries"],
            partition_counts=partition_plan["counts"],
            label_policies=sorted({r["label"].get("policy", "unknown") for r in rows}),
        ),
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
            key: sorted({r["values"].get(key, "unknown") for r in train_rows}, key=str)
            for key in (
                (
                    "family",
                    "direction",
                    "regime",
                    "horizon_profile",
                    "h4_timeframe",
                    "h1_timeframe",
                    "m15_timeframe",
                )
                if track == "bootstrap"
                else ("family", "direction", "regime", "score_profile")
            )
        },
        minimum_coverage=min(r["data_coverage"] for r in train_rows),
        required_features=[
            key
            for key in train_rows[0]["values"]
            if sum(r["values"].get(key) is not None for r in train_rows) / len(train_rows) >= 0.95
        ],
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
