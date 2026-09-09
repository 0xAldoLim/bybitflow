"""Read-only meta-ranking; immutable risk gates and separate research/deployment rights."""

import json

from ..storage import now_ms
from .features import snapshot
from .models import explain, predict
from .registry import Registry
from .store import FeatureStore
from .validation import accepted


def apply(signal, settings, store, at_ms=None):
    # Never retain yesterday's probability/approval while reassessing a plan or disabling ML.
    if signal.validation_status == "validated":
        signal.final_tier = "REJECTED" if signal.gates else "RESEARCH"
        signal.qualification = dict(status="Uncalibrated", probability=None, validated=False)
    signal.calibrated_probability = signal.probability_uncertainty = None
    signal.expected_net_r = signal.expected_net_r_uncertainty = None
    signal.validation_status, signal.model_version = "unvalidated", None
    if not settings.ml_enabled:
        return signal
    now = at_ms or now_ms()
    registry = Registry(store)
    ident = FeatureStore(store).capture(signal, now, "decision")
    # Exactly the frozen feature values that offline training will read.
    row = json.loads(store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0])
    signal.data_coverage = row["data_coverage"]
    try:
        champion = registry.champion()
        latest = store.db.execute("SELECT id FROM ml_models ORDER BY created_ms DESC LIMIT 1").fetchone()
        model = champion or (registry.get(latest[0]) if latest else None)
        if not model:
            signal.qualification["ml_reason"] = "No trained model; deterministic research only"
            return signal
        signal.model_version = model["id"]
        signal.validation_status = "research_challenger"
        reasons = []
        if model["feature_schema_version"] != signal.feature_schema_version:
            reasons.append("feature schema changed; retraining required")
        if now < model["created_ms"]:
            reasons.append("model was not available at this decision timestamp")
        chart_compatible = (
            model["stage"] == "chart"
            and signal.source == "tradingview"
            and not signal.coverage.get("liquidity_observed")
        )
        if model["source"] != signal.source or (model["stage"] != "decision" and not chart_compatible):
            reasons.append("model source/stage does not match this candidate")
        if signal.version not in model.get("strategy_versions", []):
            reasons.append("strategy version not covered by this model")
        for key, vocabulary in model["contexts"].items():
            if row["values"].get(key) not in vocabulary:
                reasons.append("unseen model context: " + key)
        if row["data_coverage"] < model["minimum_coverage"]:
            reasons.append("required training feature coverage lost")
        if any(row["values"].get(key) is None for key in model.get("required_features", [])):
            reasons.append("a normally present model feature is missing")
        if now - model["periods"]["holdout"]["end"] > 90 * 86_400_000:
            reasons.append("model evidence older than 90 days")
        if registry.is_degraded(model["id"]):
            reasons.append("model degraded")
        if reasons:
            signal.validation_status = "abstained"
            signal.qualification["ml_reason"] = "; ".join(reasons)
            if settings.ml_filter_research:
                signal.gates.append("ML abstained: " + "; ".join(reasons))
                signal.final_tier = "REJECTED"
            return signal
        p = float(predict(model["model"], [row])[0])
        qualifies = accepted(row, p, model["model"]["thresholds"])
        signal.evidence["ml"] = dict(
            explanation=explain(model["model"], row),
            research_acceptance=qualifies,
            thresholds=model["model"]["thresholds"],
            note="Meta-label ranking, not a quality-score replacement; attribution is not causation",
        )
        with store.db:
            store.db.execute(
                "INSERT OR IGNORE INTO ml_predictions VALUES(?,?,?,?,?)",
                (ident, model["id"], now, p, int(qualifies)),
            )
        if not qualifies and settings.ml_filter_research:
            signal.gates.append("research meta-label acceptance threshold not met")
            signal.final_tier = "REJECTED"
        if champion and qualifies and not signal.gates:
            key = "|".join([signal.family, signal.direction, signal.regime, str(min(9, int(p * 10)))])
            cohort = model["report"].get("context_cohorts", {}).get(key, {})
            if (
                cohort.get("n", 0) >= 100
                and cohort.get("effective_samples", 0) >= 26
                and cohort.get("ev_interval")
                and cohort["ev_interval"][0] > 0
                and cohort.get("win_interval")
                and cohort["win_interval"][1] - cohort["win_interval"][0] <= 0.20
            ):
                signal.calibrated_probability = p
                signal.probability_uncertainty = tuple(cohort["win_interval"])
                signal.expected_net_r = cohort["net_expectancy_r"]
                signal.expected_net_r_uncertainty = tuple(cohort["ev_interval"])
                signal.validation_status = "validated"
                signal.final_tier = signal.raw_tier
                signal.qualification = dict(
                    status="Validated",
                    probability=p,
                    validated=True,
                    samples=cohort["n"],
                    effective_samples=cohort["effective_samples"],
                    interval_definition="historical score-probability cohort, weekly-cluster uncertainty",
                    model_id=model["id"],
                )
    except (ValueError, KeyError, TypeError, OverflowError) as exc:
        signal.validation_status = "abstained"
        signal.qualification["ml_reason"] = type(exc).__name__ + ": inference unavailable"
        if settings.ml_filter_research:
            signal.gates.append("ML inference unavailable")
            signal.final_tier = "REJECTED"
    return signal


def delivery_eligible(signal, settings, store):
    """Public delivery checks registry approval, not a caller-supplied validated flag."""
    if (
        not settings.ml_enabled
        or signal.validation_status != "validated"
        or signal.gates
        or signal.final_tier not in settings.validated_alert_tiers
        or signal.final_tier not in {"SSS", "SS", "S"}
        or not signal.risk.get("accepted")
        or signal.calibrated_probability is None
        or not signal.expected_net_r_uncertainty
        or signal.expected_net_r_uncertainty[0] <= 0
        or now_ms() >= min(signal.expires_ms, signal.trigger_expires_ms or signal.expires_ms)
    ):
        return False
    try:
        champion = Registry(store).champion()
        if not champion or champion["id"] != signal.model_version:
            return False
        prediction = store.db.execute(
            "SELECT p.at_ms,p.probability,p.accepted FROM ml_predictions p "
            "JOIN ml_snapshots s ON s.id=p.snapshot_id WHERE s.signal_id=? AND p.model_id=?",
            (signal.id, signal.model_version),
        ).fetchone()
        if (
            not prediction
            or not prediction["accepted"]
            or now_ms() - prediction["at_ms"] > 60_000
            or abs(prediction["probability"] - signal.calibrated_probability) > 1e-12
        ):
            return False
        if signal.source == "bybit":
            book = signal.evidence.get("book", {})
            if not 0 <= now_ms() - book.get("receipt_ms", 0) <= settings.book_stale_ms:
                return False
        else:
            liquidity = signal.evidence.get("liquidity") or {}
            if not 0 <= now_ms() - liquidity.get("observed_ms", 0) <= 60_000:
                return False
        snap = snapshot(signal, now_ms(), "decision")
        return accepted(snap, signal.calibrated_probability, champion["model"]["thresholds"])
    except (ValueError, KeyError):
        return False
