"""Published v2.2 plan/score evidence is immutable; current health is coverage."""

from copy import deepcopy

from .quality_v22 import POLICY
from .v2 import FROZEN_FIELDS

EVIDENCE = (
    "v22",
    "v22_initial_assessment",
    "delivery_execution_snapshot",
    "structural_tp1",
    "execution_tp1",
    "execution_target",
    "horizon_movement_capacity",
    "target_reachability",
    "stop_viability",
    "impact_efficiency",
    "event_ofi",
    "edge_age",
    "edge_observations_v22",
    "entry_quality",
    "initial_executable_quote",
    "score_profile",
    "score_components",
    "score_reasons",
    "production_policies",
    "frozen_structure",
    "flow",
    "flow_quality",
    "book",
    "confirmation",
    "v8_gate",
    "v211_baseline_assessment",
    "delivery_confirmation",
)


def preserve(store, signal, saved):
    if saved.get("evidence", {}).get("production_policies", {}).get("trade_quality") != POLICY:
        return
    sent = store.db.execute(
        "SELECT 1 FROM outbox WHERE signal_id=? AND status='sent' "
        "AND message_id IS NOT NULL AND key LIKE '%:initial' LIMIT 1",
        (signal.id,),
    ).fetchone()
    if not sent:
        return
    for key in FROZEN_FIELDS:
        if key in saved:
            value = deepcopy(saved[key])
            if key in {"zone", "probability_uncertainty", "expected_net_r_uncertainty"} and value is not None:
                value = tuple(value)
            setattr(signal, key, value)
    for key in EVIDENCE:
        if key in saved.get("evidence", {}):
            signal.evidence[key] = deepcopy(saved["evidence"][key])
        else:
            signal.evidence.pop(key, None)
