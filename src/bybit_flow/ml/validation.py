"""Predefined chronological partitions, purging, grouped uncertainty and bounded search."""

from collections import defaultdict
from itertools import product

import numpy as np

HOUR = 3_600_000
WEEK = 7 * 24 * HOUR
BOUNDS = {"min_probability": (0.50, 0.60, 0.70), "min_quality": (65, 85, 95)}


REQUIREMENTS = {"training": 200, "calibration": 100, "validation": 100, "holdout": 100}
MINIMUM_TOTAL = 500
PARTITION_POLICY = "adaptive-causal-v1"


def _earliest_boundary(rows, times, lower, minimum, embargo_ms, ceiling, holdout_after=None):
    """Heap sweep counts released labels without assuming monotonic availability."""
    import heapq
    from bisect import bisect_left, bisect_right

    decisions = [row["decision_ms"] for row in rows]
    index = bisect_left(decisions, lower)
    released, waiting = 0, []
    last = None
    for upper in times[bisect_right(times, lower) : bisect_right(times, ceiling)]:
        while index < len(rows) and rows[index]["decision_ms"] < upper:
            row = rows[index]
            heapq.heappush(waiting, max(row["label_available_ms"], row["label"]["exit_ms"]) + embargo_ms)
            index += 1
        while waiting and waiting[0] < upper:
            heapq.heappop(waiting)
            released += 1
        last = upper
        if released >= minimum and (holdout_after is None or upper > holdout_after):
            return upper, upper
    return None, last


def _partition_plan(rows, previous_holdout_end, embargo_ms):
    """Earliest boundaries dominate later ones by leaving larger future pools.

    Moving a feasible training end earlier adds calibration candidates at every
    possible calibration end. The earliest calibration end likewise adds
    validation candidates, and the earliest validation end maximizes holdout.
    This set-inclusion proof does not require sorted outcome availability.
    Three heap sweeps plus sorting are bounded O(N log N); outcome values and
    model performance never influence the boundaries.
    """
    import math
    from time import time_ns

    asof = time_ns() // 1_000_000
    report = dict(
        status="NOT_READY",
        policy=PARTITION_POLICY,
        asof_ms=asof,
        total_rows=len(rows),
        distinct_decision_times=0,
        minimum_total=MINIMUM_TOTAL,
        embargo_ms=embargo_ms,
        requirements=dict(REQUIREMENTS),
        chosen_boundaries=dict(train_end=None, calibration_end=None, validation_end=None),
        counts={
            "training_before_purge": 0,
            "training_after_purge": 0,
            "calibration_before_purge": 0,
            "calibration_after_purge": 0,
            "validation_before_purge": 0,
            "validation_after_purge": 0,
            "holdout": 0,
        },
        shortfall=dict(REQUIREMENTS),
        blocker=None,
        reason=None,
    )
    groups = [[], [], [], []]

    def fail(blocker, detail=None):
        report["blocker"] = blocker
        counts = report["counts"]
        for name, minimum in REQUIREMENTS.items():
            amount = counts[name if name == "holdout" else name + "_after_purge"]
            report["shortfall"][name] = max(0, minimum - amount)
        hours = (
            f"{embargo_ms / HOUR:g}"
            if isinstance(embargo_ms, (int, float)) and math.isfinite(embargo_ms)
            else "invalid"
        )
        summary = (
            f"No causal 200/100/100/100 partition exists after {hours}h embargo: "
            f"train={counts['training_after_purge']}, calibration={counts['calibration_after_purge']}, "
            f"validation={counts['validation_after_purge']}, holdout={counts['holdout']}"
        )
        report["reason"] = (detail + "; " if detail else "") + summary
        return report, groups

    if not rows:
        return fail("total", "Empty dataset; need >=500 complete unique outcomes")
    if not isinstance(embargo_ms, (int, float)) or not math.isfinite(embargo_ms) or embargo_ms < 0:
        report["embargo_ms"] = None
        return fail("invalid_input", "Invalid embargo")
    try:
        ids = [row["id"] for row in rows]
        identities = [row.get("candidate_identity") or row["id"] for row in rows]
        if len(set(ids)) != len(rows) or len(set(identities)) != len(rows):
            return fail("duplicate_identity", "Duplicate snapshot IDs or candidate identities")
        sources = {row["source"] for row in rows if "source" in row}
        if len(sources) > 1:
            return fail("source", "Source-specific partitions required")
        if previous_holdout_end is not None and (
            not isinstance(previous_holdout_end, (int, float)) or not math.isfinite(previous_holdout_end)
        ):
            return fail("invalid_input", "Invalid previous holdout end")
        for row in rows:
            decision, available, exit_ms = (
                row["decision_ms"],
                row["label_available_ms"],
                row["label"]["exit_ms"],
            )
            if (
                row["label"].get("complete") is not True
                or any(
                    not isinstance(t, (int, float)) or not math.isfinite(t)
                    for t in (decision, available, exit_ms)
                )
                or not decision < exit_ms <= available <= asof
            ):
                return fail("labels", "Incomplete, future or noncausal label timing")
        rows = sorted(rows, key=lambda row: (row["decision_ms"], row["id"]))
        times = sorted({row["decision_ms"] for row in rows})
    except (KeyError, TypeError):
        return fail("invalid_input", "Invalid partition metadata")
    report["distinct_decision_times"] = len(times)
    if len(rows) < MINIMUM_TOTAL:
        return fail("total", "Need >=500 complete unique outcomes")
    if len(times) < 20:
        return fail("distinct_decision_times", "Insufficient distinct decision times; need >=20")

    # Every boundary must leave a full holdout. Equal-time rows stay together,
    # so the latest allowed distinct time may leave more than 100 suffix rows.
    ceiling = rows[-REQUIREMENTS["holdout"]]["decision_ms"]
    report["latest_feasible_holdout_start"] = ceiling
    report["counts"]["holdout"] = sum(row["decision_ms"] >= ceiling for row in rows)
    if previous_holdout_end is not None and ceiling <= previous_holdout_end + embargo_ms:
        report["counts"]["holdout"] = sum(
            row["decision_ms"] > previous_holdout_end + embargo_ms for row in rows
        )
        return fail("holdout", "Holdout period already consumed; new unseen outcomes required")
    lower = times[0]
    for index, (name, key) in enumerate(
        (("training", "train_end"), ("calibration", "calibration_end"), ("validation", "validation_end"))
    ):
        holdout_after = (
            previous_holdout_end + embargo_ms
            if name == "validation" and previous_holdout_end is not None
            else None
        )
        boundary, probe = _earliest_boundary(
            rows, times, lower, REQUIREMENTS[name], embargo_ms, ceiling, holdout_after
        )
        if probe is not None:
            before = [row for row in rows if lower <= row["decision_ms"] < probe]
            groups[index] = [
                row
                for row in before
                if max(row["label_available_ms"], row["label"]["exit_ms"]) + embargo_ms < probe
            ]
            report["counts"][name + "_before_purge"] = len(before)
            report["counts"][name + "_after_purge"] = len(groups[index])
        if boundary is None:
            report["diagnostic_boundary"] = probe
            return fail(name)
        report["chosen_boundaries"][key] = boundary
        lower = boundary
    groups[3] = [row for row in rows if row["decision_ms"] >= lower]
    report["counts"]["holdout"] = len(groups[3])
    if len(groups[3]) < REQUIREMENTS["holdout"]:
        return fail(
            "holdout", "New unseen holdout outcomes required" if previous_holdout_end is not None else None
        )
    report.update(
        status="READY", blocker=None, reason="Causal 200/100/100/100 chronological partitions ready"
    )
    report["shortfall"] = dict.fromkeys(REQUIREMENTS, 0)
    return report, groups


def partition_feasibility(rows, previous_holdout_end=None, embargo_ms=4 * HOUR):
    """Pure bounded timing diagnostics; never inspects outcomes or fits models."""
    return _partition_plan(rows, previous_holdout_end, embargo_ms)[0]


def partitions(rows, embargo_ms=4 * HOUR):
    report, groups = _partition_plan(rows, None, embargo_ms)
    if report["status"] != "READY":
        raise ValueError(report["reason"])
    return groups


def walk_forward(rows, folds=3, embargo_ms=4 * HOUR):
    """Expanding train, independent calibration, next-time test; never random shuffle."""
    times = sorted({r["decision_ms"] for r in rows})
    for i in range(folds):
        a, b, c = [
            times[min(len(times) - 1, int(len(times) * f))]
            for f in (0.4 + i * 0.15, 0.5 + i * 0.15, 0.65 + i * 0.15)
        ]
        train = [
            r
            for r in rows
            if r["decision_ms"] < a and max(r["label_available_ms"], r["label"]["exit_ms"]) + embargo_ms < a
        ]
        calibration = [
            r
            for r in rows
            if a <= r["decision_ms"] < b
            and max(r["label_available_ms"], r["label"]["exit_ms"]) + embargo_ms < b
        ]
        test = [
            r
            for r in rows
            if b <= r["decision_ms"] < c
            and max(r["label_available_ms"], r["label"]["exit_ms"]) + embargo_ms < c
        ]
        yield train, calibration, test


def next_cycle_partitions(rows, previous_holdout_end, embargo_ms=4 * HOUR):
    """Consumed holdouts may enter causal development; new holdout stays unseen."""
    report, groups = _partition_plan(rows, previous_holdout_end, embargo_ms)
    if report["status"] != "READY":
        raise ValueError(report["reason"])
    return groups


def cluster_interval(rows, values, alpha=0.05, seed=7, replicates=1000):
    groups = defaultdict(list)
    for row, value in zip(rows, values, strict=True):
        # All symbols in a market week are resampled together; never pretend trades independent.
        groups[row["decision_ms"] // WEEK].append(float(value))
    count = len(groups)
    if count < 8:
        return None, count
    sums = np.array([sum(g) for g in groups.values()])
    sizes = np.array([len(g) for g in groups.values()])
    rng = np.random.default_rng(seed)
    # At least 20 draws in each adjusted tail; bounded batches avoid a large bootstrap matrix.
    replicates = max(replicates, int(40 / alpha))
    estimates = []
    for start in range(0, replicates, 250):
        index = rng.integers(0, count, (min(250, replicates - start), count))
        estimates.extend((sums[index].sum(axis=1) / sizes[index].sum(axis=1)).tolist())
    return np.quantile(estimates, [alpha / 2, 1 - alpha / 2]).tolist(), count


def metrics(rows, probabilities=None, alpha=0.05):
    if not rows:
        return dict(
            n=0, net_expectancy_r=None, ev_interval=None, effective_samples=0, brier=None, reliability=[]
        )
    if probabilities is not None:
        pairs = sorted(zip(rows, probabilities, strict=True), key=lambda pair: pair[0]["decision_ms"])
        rows, probabilities = [r for r, p in pairs], [p for r, p in pairs]
    else:
        rows = sorted(rows, key=lambda r: r["decision_ms"])
    r = np.array([x["label"]["net_r"] for x in rows], dtype=float)
    win, loss = r[r > 0], r[r < 0]
    curve = np.r_[
        0, np.cumsum([x["label"]["net_r"] for x in sorted(rows, key=lambda x: x["label"]["exit_ms"])])
    ]
    interval, clusters = cluster_interval(rows, r, alpha)
    win_ci, _ = cluster_interval(rows, r > 0, alpha)
    span_days = max(1.0, (rows[-1]["decision_ms"] - rows[0]["decision_ms"]) / (24 * HOUR))
    result = dict(
        n=len(rows),
        net_expectancy_r=float(r.mean()),
        ev_interval=interval,
        win_rate=float((r > 0).mean()),
        win_interval=win_ci,
        effective_samples=clusters,
        average_win=float(win.mean()) if len(win) else None,
        average_loss=float(loss.mean()) if len(loss) else None,
        profit_factor=float(win.sum() / -loss.sum()) if len(loss) else None,
        max_drawdown_r=float((np.maximum.accumulate(curve) - curve).max()),
        tail_loss_5pct_r=float(np.mean(np.sort(r)[: max(1, int(len(r) * 0.05))])),
        signals_per_day=len(rows) / span_days,
        turnover_base=sum(x["label"].get("quantity", 0) * 2 for x in rows),
        uncertainty="weekly market-cluster bootstrap; cluster count is an upper bound on independence",
        brier=None,
        reliability=[],
    )
    if probabilities is not None:
        p = np.asarray(probabilities)
        result["brier"] = float(np.mean((p - (r > 0)) ** 2))
        for low in np.arange(0, 1, 0.1):
            mask = (p >= low) & (p < low + 0.1 + (1e-9 if low > 0.89 else 0))
            if mask.any():
                result["reliability"].append(
                    dict(
                        lower=float(low),
                        count=int(mask.sum()),
                        predicted=float(p[mask].mean()),
                        observed=float((r[mask] > 0).mean()),
                    )
                )
    return result


def accepted(row, probability, params):
    if set(params) != set(BOUNDS) or any(params[k] not in BOUNDS[k] for k in BOUNDS):
        raise ValueError("Optimizer cannot modify hard safety parameters")
    s = row["signal"]
    return (
        not s["gates"]
        and s["risk"].get("accepted", False)
        and s["quality"] >= params["min_quality"]
        and probability >= params["min_probability"]
    )


def optimize(rows, probabilities):
    """Exhaustive seeded finite search is the reproducible alternative to adaptive Optuna trials."""
    trials = []
    for p, q in product(*BOUNDS.values()):
        params = dict(min_probability=p, min_quality=q)
        selected = [
            r for r, probability in zip(rows, probabilities, strict=True) if accepted(r, probability, params)
        ]
        report = metrics(selected, alpha=0.05 / 9)
        # Predeclared net-EV, tail, drawdown, turnover and evidence penalties; never win-rate objective.
        objective = None
        if report["n"] >= 30 and report["effective_samples"] >= 8:
            objective = (
                report["ev_interval"][0]
                - 0.02 * report["max_drawdown_r"]
                + 0.1 * min(0, report["tail_loss_5pct_r"])
                - 0.005 * report["signals_per_day"]
                - 1 / np.sqrt(report["n"])
            )
        trials.append(dict(number=len(trials), parameters=params, objective=objective, report=report))
    usable = [t for t in trials if t["objective"] is not None]
    best = max(usable, key=lambda x: (x["objective"], -x["number"])) if usable else None
    return best, trials


def breakdown(rows, probabilities):
    from ..scoring import tier

    groups = defaultdict(list)
    for row, p in zip(rows, probabilities, strict=True):
        for field in ("family", "direction", "regime", "symbol"):
            groups[field + ":" + row["signal"][field]].append((row, p))
        groups["tier:" + tier(row["signal"]["quality"])].append((row, p))
    return {key: metrics([r for r, p in pairs], [p for r, p in pairs]) for key, pairs in groups.items()}
