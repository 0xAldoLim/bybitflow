"""Predeclared, offline V8 feature-group ablation; never promotes a live model."""

import argparse
import json
import uuid

from ..storage import Store, now_ms
from . import SCHEMA_VERSION
from .models import calibrate, fit, predict
from .store import FeatureStore
from .validation import accepted, cluster_interval, metrics, next_cycle_partitions, walk_forward

GROUPS = ("context_ev", "liquidation", "breadth", "ofi", "anchored", "spot_perp", "volatility", "v8_gate")
POLICY = "v8-ablation-v1"
PARAMETERS = {"min_probability": 0.6, "min_quality": 65}


def _evaluation(rows, probabilities):
    selected = [row for row, p in zip(rows, probabilities, strict=True) if accepted(row, p, PARAMETERS)]
    result = metrics(
        selected, [p for row, p in zip(rows, probabilities, strict=True) if accepted(row, p, PARAMETERS)]
    )
    result["acceptance_rate"] = len(selected) / len(rows) if rows else 0
    result["cost_adjusted_net_r_per_opportunity"] = (
        sum(row["label"]["net_r"] for row in selected) / len(rows) if rows else None
    )
    result["calibration_brier_all"] = metrics(rows, probabilities)["brier"]
    return result


def report(store, source, asof_ms, *, consume_holdout=False):
    rows = FeatureStore(store).dataset(
        asof_ms - 1, source=source, schema_version=SCHEMA_VERSION, limit=10_000
    )
    base = dict(
        policy=POLICY,
        schema_version=SCHEMA_VERSION,
        source=source,
        asof_ms=asof_ms,
        status="INSUFFICIENT_EVIDENCE",
        production_enabled=False,
        sample_count=len(rows),
        experiment_count=len(GROUPS) + 2,
        holdout_consumed=False,
        feature_utility=[],
        note="Research only; no feature or model is promoted automatically",
    )
    if len(rows) < 500:
        return base
    previous_end = store.db.execute("SELECT MAX(end_ms) FROM ml_holdouts").fetchone()[0]
    try:
        training, calibration, validation, holdout = next_cycle_partitions(rows, previous_end)
    except ValueError:
        return base
    if min(map(len, (training, calibration, validation, holdout))) < 100:
        return base
    experiment_specs = (
        [("baseline", GROUPS)]
        + [("plus_" + group, tuple(other for other in GROUPS if other != group)) for group in GROUPS]
        + [("all_v8", ())]
    )
    models = {}
    evaluations = {}
    for name, excluded in experiment_specs:
        try:
            artifact, _ = fit(training, "logistic", excluded)
            calibrate(artifact, calibration, "sigmoid")
            models[name] = artifact
            evaluations[name] = _evaluation(validation, predict(artifact, validation))
        except (ValueError, TypeError):
            return base
    baseline = evaluations["baseline"]["cost_adjusted_net_r_per_opportunity"]
    utilities = []
    for group in GROUPS:
        names = [key for key, meta in validation[0]["feature_metadata"].items() if meta["group"] == group]
        total = len(validation) * len(names)
        missing = sum(row["values"].get(key) is None for row in validation for key in names)
        name = "plus_" + group
        utilities.append(
            dict(
                feature_group=group,
                samples=len(validation),
                missing_rate=missing / total if total else 1,
                validation_delta=evaluations[name]["cost_adjusted_net_r_per_opportunity"] - baseline,
                holdout_delta=None,
                stable_across_folds=None,
                production_enabled=False,
            )
        )
    folds = []
    development = [*training, *calibration, *validation]
    for fold_train, fold_calibration, fold_test in walk_forward(development):
        if min(map(len, (fold_train, fold_calibration, fold_test))) < 100:
            continue
        try:
            fold_result = {}
            for name, excluded in experiment_specs:
                artifact, _ = fit(fold_train, "logistic", excluded)
                calibrate(artifact, fold_calibration, "sigmoid")
                fold_result[name] = _evaluation(fold_test, predict(artifact, fold_test))[
                    "cost_adjusted_net_r_per_opportunity"
                ]
            folds.append(fold_result)
        except (ValueError, TypeError):
            continue
    for item in utilities:
        deltas = [fold["plus_" + item["feature_group"]] - fold["baseline"] for fold in folds]
        item["fold_deltas"] = deltas
        item["stable_across_folds"] = (
            sum(value > 0 for value in deltas) >= 2 and min(deltas) > -0.05 if len(deltas) >= 2 else None
        )
    base.update(status="NOT_READY", validation=evaluations, feature_utility=utilities)
    if not consume_holdout:
        return base
    start, end = holdout[0]["decision_ms"], max(row["label_available_ms"] for row in holdout)
    reservation = uuid.uuid4().hex
    with store.db:
        store.db.execute("BEGIN IMMEDIATE")
        if store.db.execute(
            "SELECT 1 FROM ml_holdouts WHERE start_ms<=? AND end_ms>=?", (end, start)
        ).fetchone():
            raise ValueError("A prior experiment consumed this holdout period")
        store.db.execute("INSERT INTO ml_holdouts VALUES(?,?,?)", (start, end, reservation))
    held_probabilities = {name: predict(model, holdout) for name, model in models.items()}
    held = {name: _evaluation(holdout, probabilities) for name, probabilities in held_probabilities.items()}
    held_base = held["baseline"]["cost_adjusted_net_r_per_opportunity"]
    for item in utilities:
        group_name = "plus_" + item["feature_group"]
        item["holdout_delta"] = held[group_name]["cost_adjusted_net_r_per_opportunity"] - held_base
        differences = [
            row["label"]["net_r"]
            * (int(accepted(row, group_p, PARAMETERS)) - int(accepted(row, base_p, PARAMETERS)))
            for row, group_p, base_p in zip(
                holdout, held_probabilities[group_name], held_probabilities["baseline"], strict=True
            )
        ]
        item["holdout_delta_interval"], item["holdout_market_weeks"] = cluster_interval(
            holdout, differences, alpha=0.05 / 8
        )
    # Several predeclared comparisons share this holdout. No automatic promotion.
    any_positive = any(item["validation_delta"] > 0 and item["holdout_delta"] > 0 for item in utilities)
    promotion_candidates = [
        item["feature_group"]
        for item in utilities
        if item["missing_rate"] <= 0.5
        and item["validation_delta"] > 0
        and item["holdout_delta_interval"] is not None
        and item["holdout_delta_interval"][0] > 0
        and item["stable_across_folds"] is True
        and isinstance(held["baseline"].get("tail_loss_5pct_r"), (int, float))
        and isinstance(held["plus_" + item["feature_group"]].get("tail_loss_5pct_r"), (int, float))
        and held["plus_" + item["feature_group"]]["tail_loss_5pct_r"]
        >= held["baseline"]["tail_loss_5pct_r"] - 0.1
        and held["plus_" + item["feature_group"]]["calibration_brier_all"]
        <= held["baseline"]["calibration_brier_all"] + 0.02
    ]
    base.update(
        status="PROMOTION_CANDIDATE"
        if promotion_candidates
        else "NOT_READY"
        if any_positive
        else "NO_INCREMENTAL_VALUE",
        holdout=held,
        holdout_consumed=True,
        holdout_reservation=reservation,
        multiple_comparisons="Eight predeclared comparisons; exploratory, not a production promotion decision",
        promotion_candidate_groups=promotion_candidates,
    )
    return base


def main():
    from ..config import Settings

    parser = argparse.ArgumentParser(description="Offline V8 research ablation")
    parser.add_argument("--source", choices=("binance", "bybit", "okx"), required=True)
    parser.add_argument("--consume-holdout", action="store_true")
    args = parser.parse_args()
    settings = Settings()
    store = Store(settings.data_dir)
    try:
        print(json.dumps(report(store, args.source, now_ms(), consume_holdout=args.consume_holdout)))
    finally:
        store.close()


if __name__ == "__main__":
    main()
