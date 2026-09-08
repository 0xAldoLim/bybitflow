"""Research-only qualification statistics. No deployment artifact is auto-approved."""

from collections import defaultdict

import numpy as np


def clustered_statistics(outcomes, seed=7, replicates=2000):
    """Resample entire UTC-week clusters; preserve cross-symbol dependence and overlap.

    Input rows: net_r, cluster. Effective sample is conservatively bounded by cluster count.
    Intervals are descriptive historical estimates, NOT a validated predictive probability.
    """
    groups = defaultdict(list)
    for row in outcomes:
        if not np.isfinite(row["net_r"]):
            raise ValueError("Non-finite outcome")
        groups[str(row["cluster"])].append(float(row["net_r"]))
    if not groups:
        return {"status": "Uncalibrated", "probability": None, "trades": 0, "effective_samples": 0}
    values = np.array([r for g in groups.values() for r in g])
    wins, losses = values[values > 0], values[values <= 0]
    result = dict(
        status="Uncalibrated",
        probability=None,
        trades=len(values),
        effective_samples=len(groups),
        historical_win_rate=float(np.mean(values > 0)),
        net_ev_r=float(np.mean(values)),
        average_win_r=float(np.mean(wins)) if len(wins) else 0,
        average_loss_r=float(np.mean(losses)) if len(losses) else 0,
        profit_factor=float(wins.sum() / -losses.sum()) if losses.sum() < 0 else None,
    )
    if len(groups) < 8:
        return result | {"uncertainty": "fewer than eight independent weekly clusters"}
    g = list(groups.values())
    rng = np.random.default_rng(seed)
    estimates = []
    for _ in range(replicates):
        sample = np.concatenate([g[i] for i in rng.integers(0, len(g), len(g))])
        estimates.append([np.mean(sample > 0), np.mean(sample)])
    q = np.quantile(estimates, [0.025, 0.975], axis=0)
    return result | {
        "historical_win_rate_ci": q[:, 0].tolist(),
        "ev_ci_r": q[:, 1].tolist(),
        "uncertainty_method": "whole UTC-week cluster bootstrap; dependence beyond week unresolved",
    }


def walk_forward_predictions(rows, min_train=200, purge_ms=14_400_000, bin_width=10):
    """Expanding-window empirical score bins, stratified by family/direction/regime/liquidity.

    Every label must end before prediction time minus embargo. Sparse strata abstain.
    Candidate probability estimates stay research-only until independent calibration is evaluated.
    """
    ordered = sorted(rows, key=lambda r: r["entry_ms"])
    predictions = []
    for row in ordered:

        def key(r):
            return (
                r["family"],
                r["direction"],
                r["regime"],
                r["liquidity_bucket"],
                int(r["score"] // bin_width),
            )

        train = [r for r in ordered if r["exit_ms"] < row["entry_ms"] - purge_ms and key(r) == key(row)]
        p = None
        if len(train) >= min_train and len({r["cluster"] for r in train}) >= 26:
            p = (sum(r["net_r"] > 0 for r in train) + 1) / (len(train) + 2)
        predictions.append(
            dict(entry_ms=row["entry_ms"], prediction=p, outcome=int(row["net_r"] > 0), train_size=len(train))
        )
    return predictions


def reliability(predictions):
    rows = [r for r in predictions if r["prediction"] is not None]
    if not rows:
        return {"status": "Uncalibrated", "evaluated": 0}
    brier = sum((r["prediction"] - r["outcome"]) ** 2 for r in rows) / len(rows)
    return {
        "status": "research evaluation; holdout approval required",
        "evaluated": len(rows),
        "brier": brier,
    }
