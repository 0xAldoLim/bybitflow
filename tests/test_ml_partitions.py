"""Timing-only causal partition regressions; all rows are synthetic test data."""

import random
from copy import deepcopy
from itertools import combinations

import pytest

from bybit_flow.ml import validation
from bybit_flow.ml.validation import (
    HOUR,
    next_cycle_partitions,
    partition_feasibility,
    partitions,
)

BASE = 1_600_000_000_000
NAMES = ("training", "calibration", "validation", "holdout")


def rows(count, spacing=HOUR, delay=HOUR):
    return [
        dict(
            id=f"synthetic-{index:05d}",
            candidate_identity=f"opportunity-{index:05d}",
            source="binance",
            decision_ms=BASE + index * spacing,
            label_available_ms=BASE + index * spacing + delay,
            label=dict(complete=True, exit_ms=BASE + index * spacing + delay),
        )
        for index in range(count)
    ]


def quantile_counts(data):
    times = sorted({row["decision_ms"] for row in data})
    boundaries = [times[int(len(times) * part)] for part in (0.50, 0.65, 0.80)]
    return [
        sum(
            lower <= row["decision_ms"] < upper
            and max(row["label_available_ms"], row["label"]["exit_ms"]) + 4 * HOUR < upper
            for row in data
        )
        for lower, upper in zip([times[0], *boundaries], [*boundaries, float("inf")], strict=True)
    ]


def test_fixed_quantile_fails_but_adaptive_succeeds():
    data = rows(1200, spacing=HOUR // 6)
    for row in data[600:]:
        row["label_available_ms"] = row["label"]["exit_ms"] = row["decision_ms"] + 240 * HOUR
    assert quantile_counts(data)[1:3] == [0, 0]
    plan = partition_feasibility(data)
    assert plan["status"] == "READY"
    assert plan["shortfall"] == dict.fromkeys(NAMES, 0)
    assert all(
        len(group) >= minimum for group, minimum in zip(partitions(data), (200, 100, 100, 100), strict=True)
    )


def test_truly_infeasible_calibration_reports_exact_shortfall():
    data = rows(600)
    for row in data[220:]:
        row["label_available_ms"] = row["label"]["exit_ms"] = row["decision_ms"] + 1000 * HOUR
    plan = partition_feasibility(data)
    assert plan["status"] == "NOT_READY" and plan["blocker"] == "calibration"
    assert plan["counts"]["training_after_purge"] == 200
    assert plan["counts"]["calibration_after_purge"] == 15
    assert plan["shortfall"]["calibration"] == 85
    assert plan["counts"]["holdout"] == 100
    assert "train=200, calibration=15, validation=0, holdout=100" in plan["reason"]
    with pytest.raises(ValueError, match="after 4h embargo"):
        partitions(data)


def test_every_partition_preserves_availability_exit_causality_and_strict_boundary():
    data = rows(900)
    data[10]["label_available_ms"] = data[10]["decision_ms"] + 100 * HOUR
    groups = partitions(data)
    plan = partition_feasibility(data)
    boundaries = [
        plan["chosen_boundaries"][key] for key in ("train_end", "calibration_end", "validation_end")
    ]
    for group, boundary in zip(groups[:3], boundaries, strict=True):
        assert all(
            max(row["label_available_ms"], row["label"]["exit_ms"]) + 4 * HOUR < boundary for row in group
        )
    # The last first-partition candidate that releases exactly on the selected
    # boundary stays purged; using <= would fabricate an extra observation.
    equal = [row for row in data if row["label_available_ms"] + 4 * HOUR == boundaries[0]]
    assert equal and all(row not in groups[0] for row in equal)
    assert all(row["decision_ms"] >= boundaries[2] for row in groups[3])
    assert all(row["label"]["complete"] for row in groups[3])


def test_next_cycle_can_delay_new_holdout_until_development_is_feasible():
    data = rows(900)
    consumed = data[300]["label_available_ms"]
    earliest_unseen = next(row["decision_ms"] for row in data if row["decision_ms"] > consumed + 4 * HOUR)
    groups = next_cycle_partitions(data, consumed)
    plan = partition_feasibility(data, consumed)
    assert plan["status"] == "READY"
    assert groups[3][0]["decision_ms"] > earliest_unseen
    assert all(row["decision_ms"] > consumed + 4 * HOUR for row in groups[3])
    assert max(row["label_available_ms"] for row in groups[2]) + 4 * HOUR < groups[3][0]["decision_ms"]
    assert not {row["id"] for row in groups[3]} & {row["id"] for group in groups[:3] for row in group}
    assert data[300] in [row for group in groups[:3] for row in group]


def test_consumed_holdout_requires_one_hundred_strictly_unseen_rows():
    data = rows(700)
    consumed = data[650]["decision_ms"] - 4 * HOUR
    plan = partition_feasibility(data, consumed)
    assert plan["status"] == "NOT_READY" and plan["blocker"] == "holdout"
    assert plan["counts"]["holdout"] == 49 and plan["shortfall"]["holdout"] == 51
    with pytest.raises(ValueError, match="consumed"):
        next_cycle_partitions(data, consumed)


def test_planner_is_deterministic_and_keeps_same_time_rows_together():
    data = rows(900)
    for index in range(1, len(data), 2):
        data[index]["decision_ms"] = data[index - 1]["decision_ms"]
    shuffled = deepcopy(data)
    random.Random(42).shuffle(shuffled)
    first, second = partition_feasibility(data), partition_feasibility(shuffled)
    assert first["chosen_boundaries"] == second["chosen_boundaries"]
    assert first["counts"] == second["counts"]
    expected = [[row["id"] for row in group] for group in partitions(data)]
    assert expected == [[row["id"] for row in group] for group in partitions(shuffled)]
    by_time = [{row["decision_ms"] for row in group} for group in partitions(data)]
    assert all(not left & right for left, right in combinations(by_time, 2))


def test_live_like_3700_mixed_horizons_find_a_causal_split():
    data = rows(3700, spacing=HOUR // 6)
    for index, row in enumerate(data):
        delay = (1 + index % 2) * HOUR if index < 800 else (48 + index % 3 * 96) * HOUR
        row["label"]["exit_ms"] = row["decision_ms"] + delay
        row["label_available_ms"] = row["label"]["exit_ms"] + HOUR // 4
    assert quantile_counts(data)[1] < 100
    plan = partition_feasibility(data)
    assert plan["status"] == "READY" and plan["total_rows"] == 3700
    assert all(
        len(group) >= minimum for group, minimum in zip(partitions(data), (200, 100, 100, 100), strict=True)
    )


@pytest.mark.parametrize(
    "kind", ["duplicate_id", "duplicate_candidate", "incomplete", "future", "noncausal", "source"]
)
def test_invalid_or_duplicate_evidence_never_enters_holdout(kind):
    data = rows(700)
    if kind == "duplicate_id":
        data[-1]["id"] = data[0]["id"]
    elif kind == "duplicate_candidate":
        data[-1]["candidate_identity"] = data[0]["candidate_identity"]
    elif kind == "incomplete":
        data[-1]["label"]["complete"] = False
    elif kind == "future":
        data[-1]["label_available_ms"] = 10**15
    elif kind == "noncausal":
        data[-1]["label"]["exit_ms"] = data[-1]["decision_ms"]
    else:
        data[-1]["source"] = "bybit"
    assert partition_feasibility(data)["status"] == "NOT_READY"
    with pytest.raises(ValueError):
        partitions(data)


def test_planning_ignores_values_classes_and_model_metadata():
    data = rows(700)
    altered = deepcopy(data)

    class Unreadable:
        def __bool__(self):
            pytest.fail("Planner must not inspect outcome values or model metadata")

        def __getitem__(self, _):
            pytest.fail("Planner must not inspect outcome values or model metadata")

    for row in altered:
        row["label"]["net_r"] = Unreadable()
        row["values"] = row["signal"] = Unreadable()
    expected, actual = partition_feasibility(data), partition_feasibility(altered)
    assert expected["chosen_boundaries"] == actual["chosen_boundaries"]
    assert expected["counts"] == actual["counts"]


@pytest.mark.parametrize("embargo", [None, "4h", float("nan"), -1])
def test_invalid_embargo_is_diagnostic_abstention(embargo):
    plan = partition_feasibility(rows(700), embargo_ms=embargo)
    assert plan["status"] == "NOT_READY" and plan["blocker"] == "invalid_input"


def test_earliest_greedy_plan_matches_small_exhaustive_timing_oracle(monkeypatch):
    requirements = dict(training=3, calibration=2, validation=2, holdout=2)
    monkeypatch.setattr(validation, "REQUIREMENTS", requirements)
    monkeypatch.setattr(validation, "MINIMUM_TOTAL", 9)
    for seed in range(12):
        data = rows(25)
        rng = random.Random(seed)
        for row in data:
            row["label_available_ms"] = row["label"]["exit_ms"] = (
                row["decision_ms"] + rng.randint(1, 12) * HOUR
            )
        previous = data[10]["decision_ms"] if seed % 2 else None
        times = [row["decision_ms"] for row in data]
        oracle = None
        for a, b, c in combinations(times, 3):
            if previous is not None and c <= previous + 4 * HOUR:
                continue
            lower = times[0]
            sizes = []
            for upper in (a, b, c):
                sizes.append(
                    sum(
                        lower <= row["decision_ms"] < upper and row["label_available_ms"] + 4 * HOUR < upper
                        for row in data
                    )
                )
                lower = upper
            sizes.append(sum(row["decision_ms"] >= c for row in data))
            if all(count >= minimum for count, minimum in zip(sizes, requirements.values(), strict=True)):
                oracle = (a, b, c)
                break
        plan = partition_feasibility(data, previous)
        assert (plan["status"] == "READY") == (oracle is not None)
        if oracle is not None:
            assert tuple(plan["chosen_boundaries"].values()) == oracle
