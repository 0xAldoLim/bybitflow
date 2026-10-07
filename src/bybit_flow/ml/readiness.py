"""Class diagnostics run after timing-only planning; they never choose boundaries."""

import math

from .validation import REQUIREMENTS


def model_fit_feasibility(plan, groups):
    ready = plan["status"] == "READY"
    report = dict(
        partition_ready=ready, fit_ready=False, blocker="PARTITION_NOT_READY", reason=plan["reason"]
    )
    for name, group in zip(REQUIREMENTS, groups, strict=True):
        report[name + "_rows"] = plan["counts"][name if name == "holdout" else name + "_after_purge"]
        values = [r["label"].get("net_r") for r in group] if ready else []
        valid = ready and all(isinstance(v, (int, float)) and math.isfinite(v) for v in values)
        positive = sum(v > 0 for v in values) if valid else None
        negative = len(values) - positive if valid else None
        report[name + "_positive"], report[name + "_negative"] = positive, negative
        report[name + "_classes"] = (["negative"] if negative else []) + (["positive"] if positive else [])
    if not ready:
        return report
    if any(report[name + "_rows"] < minimum for name, minimum in REQUIREMENTS.items()):
        report.update(blocker="INSUFFICIENT_ROWS", reason="Post-purge partition minimums not satisfied")
    elif any(report[name + "_positive"] is None for name in REQUIREMENTS):
        report.update(blocker="MODEL_FIT_ERROR", reason="Nonfinite or missing outcome value")
    elif not report["training_positive"] or not report["training_negative"]:
        report.update(
            blocker="TRAIN_SINGLE_CLASS", reason="Training requires both positive and nonpositive outcomes"
        )
    elif not report["calibration_positive"] or not report["calibration_negative"]:
        report.update(
            blocker="CALIBRATION_SINGLE_CLASS",
            reason="Calibration requires both positive and nonpositive outcomes",
        )
    else:
        report.update(fit_ready=True, blocker=None, reason="FIT_READY")
    return report
