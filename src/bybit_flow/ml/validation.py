"""Predefined chronological partitions, purging, grouped uncertainty and bounded search."""

from collections import defaultdict
from itertools import product

import numpy as np

HOUR = 3_600_000
WEEK = 7 * 24 * HOUR
BOUNDS = {"min_probability": (0.50, 0.60, 0.70), "min_quality": (65, 85, 95)}


def partitions(rows, embargo_ms=4 * HOUR):
    if not rows or len({r["id"] for r in rows}) != len(rows):
        raise ValueError("Empty or duplicate dataset")
    rows = sorted(rows, key=lambda r: r["decision_ms"])
    times = sorted({r["decision_ms"] for r in rows})
    if len(times) < 20:
        raise ValueError("Insufficient distinct decision times")
    boundaries = [times[int(len(times) * f)] for f in (0.50, 0.65, 0.80)]
    groups = []
    for lo, hi in zip([times[0], *boundaries], [*boundaries, float("inf")], strict=True):
        groups.append(
            [
                r
                for r in rows
                if lo <= r["decision_ms"] < hi
                and r["label"]["exit_ms"] + embargo_ms < hi
                and r["label_available_ms"] + embargo_ms < hi
            ]
        )
    if any(not group for group in groups):
        raise ValueError("Purging left an empty chronological partition")
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
    estimates = []
    for _ in range(replicates):
        index = rng.integers(0, count, count)
        estimates.append(float(sums[index].sum() / sizes[index].sum()))
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
