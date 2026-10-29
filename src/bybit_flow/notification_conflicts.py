"""Conservative notification-only justification for opposing published plans."""

from types import SimpleNamespace

from .horizons import DURATIONS, PROFILES
from .setup_common import causal, number, positive

POLICY = "opposing-alert-justification-v1"


def assess(plan, original, now, settings):
    """Only a fully evidenced, nested shorter-horizon counter-move may coexist."""
    e, old_e = plan.get("evidence") or {}, original.get("evidence") or {}
    new_h, old_h = PROFILES.get(plan.get("horizon_profile")), PROFILES.get(original.get("horizon_profile"))
    failures = []
    if (
        not new_h
        or not old_h
        or plan.get("horizon_profile") == "LEGACY"
        or original.get("horizon_profile") == "LEGACY"
        or new_h.hold_max >= old_h.hold_max
        or plan.get("setup_timeframe") != new_h.setup
        or original.get("setup_timeframe") != old_h.setup
        or DURATIONS[new_h.setup] >= DURATIONS[old_h.setup]
        or (plan.get("holding_deadline_ms") or plan["expires_ms"])
        > (original.get("holding_deadline_ms") or original["expires_ms"])
    ):
        failures.append("NO_NESTED_SHORTER_HORIZON")
    if plan.get("source") != original.get("source"):
        failures.append("VENUE_PRICE_BASIS_MISMATCH")
    coverage = original.get("coverage") or {}
    health = coverage.get("v22_current") or {}
    monitored = coverage.get("last_monitor_ms")
    if (
        coverage.get("monitoring") != "active"
        or coverage.get("monitor_status") != "MONITOR_READY"
        or not number(monitored)
        or not 0 <= now - monitored <= settings.trade_stale_ms
        or health.get("state") != "STABLE"
        or not number(health.get("available_ms"))
        or not 0 <= now - health["available_ms"] <= 120000
    ):
        failures.append("ORIGINAL_PLAN_NOT_FRESH_AND_STABLE")
    source = SimpleNamespace(source=plan.get("source"), symbol=plan.get("symbol"))
    old_source = SimpleNamespace(source=original.get("source"), symbol=original.get("symbol"))
    structure, old_structure = e.get("frozen_structure") or {}, old_e.get("frozen_structure") or {}
    if (
        not causal(structure, source, now)
        or not causal(old_structure, old_source, now)
        or not structure.get("structure_event_id")
        or not old_structure.get("structure_event_id")
        or structure["structure_event_id"] == old_structure["structure_event_id"]
        or not positive(structure.get("frozen_reference"))
        or not positive(old_structure.get("frozen_reference"))
        or structure["frozen_reference"] == old_structure["frozen_reference"]
    ):
        failures.append("NO_DISTINCT_FROZEN_STRUCTURE")
    levels = [plan.get(k) for k in ("entry", "stop", "tp1", "tp2")] + list(plan.get("zone", []))
    bounds = [original.get("stop"), original.get("tp1")]
    if not all(positive(x) for x in levels + bounds) or not all(
        min(bounds) < x < max(bounds) for x in levels
    ):
        failures.append("COUNTER_PLAN_CROSSES_ORIGINAL_TERMINAL_LEVEL")
    confirmation = e.get("delivery_confirmation") or {}
    quality, response, entry = (
        e.get(k) or {} for k in ("flow_quality", "impact_efficiency", "entry_quality")
    )
    if (
        not causal(confirmation, source, now, settings.book_stale_ms)
        or confirmation.get("passed") is not True
        or confirmation.get("structure_support") is not True
        or confirmation.get("participation_support") is not True
        or confirmation.get("metrics", {}).get("flow", {}).get("strong") is not True
        or not causal(quality, source, now, settings.book_stale_ms)
        or not number(quality.get("flow_trust_score"))
        or quality["flow_trust_score"] < 0.6
        or not causal(response, source, now, settings.book_stale_ms)
        or response.get("coverage_complete") is not True
        or response.get("contradicted") is not False
        or response.get("state") not in {"EFFICIENT_DIRECTIONAL_IMPACT", "DEFENDED_ABSORPTION_RESPONSE"}
        or not causal(entry, source, now, settings.book_stale_ms)
        or entry.get("acceptable") is not True
        or entry.get("quote_state") != "QUOTE_FRESH"
        or plan.get("gates")
    ):
        failures.append("NO_FRESH_STRONG_CONFIRMED_COUNTER_MOVE")
    return dict(
        policy=POLICY,
        available_ms=now,
        signal_id=original["id"],
        allowed=not failures,
        reasons=failures,
        explanation=(
            f"{plan.get('horizon_profile')} {plan['direction']} counter-move inside the active "
            f"{original.get('horizon_profile')} {original['direction']} plan. "
            f"Distinct frozen level {structure.get('frozen_reference')} versus {old_structure.get('frozen_reference')}; "
            f"fresh strong structure/flow confirmation with {response.get('state')}. "
            f"The complete counter-plan stays between the original stop {original.get('stop')} and target {original.get('tp1')}, "
            f"and ends no later than that plan. Original ID {original['id']}."
        )
        if not failures
        else None,
    )
