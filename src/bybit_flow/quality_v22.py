"""New opportunities only. Existing v21/v21.1 policies retain their semantics."""

from copy import deepcopy

from . import quality_v211
from .impact_efficiency import assess as impact
from .setup_common import envelope, number
from .target_reachability import movement_capacity, select_target

POLICY = "trade-validity-v22"
SCORE_PROFILE = "bybitflow-v22-setup-1"


def enabled(signal):
    return signal.evidence.get("production_policies", {}).get("trade_quality") == POLICY


def prepare_generation(signal, bars, now, settings, instrument, book, funding_rate=None):
    """Once, before the first immutable generation snapshot and any label job."""
    if not enabled(signal) or "execution_target" in signal.evidence:
        return
    from .risk import evaluate_risk

    signal.evidence["structural_tp1"] = signal.tp1
    capacity = movement_capacity(signal, bars, now)
    signal.evidence["horizon_movement_capacity"] = capacity
    signal.risk = evaluate_risk(signal, instrument, settings, book, funding_rate=funding_rate)
    signal.evidence["v211_structural_risk"] = deepcopy(signal.risk)
    target = select_target(signal, capacity, now, settings.min_net_rr)
    signal.evidence["execution_target"] = target
    if target["execution_tp1"] is not None:
        signal.tp1 = target["execution_tp1"]
        signal.evidence["execution_tp1"] = signal.tp1
        signal.evidence["target_method"] = "v2.2 causal execution target"
        signal.risk = evaluate_risk(signal, instrument, settings, book, funding_rate=funding_rate)
    signal.evidence["production_policies"].update(
        scoring=SCORE_PROFILE,
        target="execution-target-v1",
        stop_viability="stop-viability-v1",
        reachability="target-reachability-v1",
        impact="impact-efficiency-v1",
    )
    signal.version += ":bybitflow-v22"


def entry_quality(signal, price, now, minimum_rr=1.5, **kwargs):
    return quality_v211.entry_quality(signal, price, now, minimum_rr, **kwargs) | dict(policy=POLICY)


def apply(signal, price, now, liquidity=None, minimum_rr=1.5, *, book=None, settings=None):
    if not enabled(signal):
        return
    # The scanner can create provisional components before V8/entry gates run.
    # Those components are not an immutable confirmed decision.
    had_scored_decision = signal.state in {"CONFIRMED", "ALERTED"} and bool(
        signal.evidence.get("score_components")
    )
    # Apply the unchanged v21.1 protections on a temporary policy-compatible copy.
    # Never relabel the persisted signal or rewrite any historical observation.
    copy = deepcopy(signal)
    copy.evidence["production_policies"]["trade_quality"] = quality_v211.POLICY
    quality_v211.apply(copy, price, now, liquidity, minimum_rr, book=book, settings=settings)
    baseline = deepcopy(signal)
    baseline.evidence["production_policies"]["trade_quality"] = quality_v211.POLICY
    baseline.tp1 = signal.evidence.get("structural_tp1", signal.tp1)
    baseline.risk = deepcopy(signal.evidence.get("v211_structural_risk", signal.risk))
    baseline.gates = sorted(set(baseline.gates + baseline.risk.get("reasons", [])))
    quality_v211.apply(baseline, price, now, liquidity, minimum_rr, book=book, settings=settings)
    signal.evidence["v211_baseline_assessment"] = envelope(
        signal,
        "v211-forward-control-v22",
        now,
        gates=list(baseline.gates),
        structural_tp1=baseline.tp1,
        risk=baseline.risk,
        entry_quality=baseline.evidence.get("entry_quality"),
        scope="Unchanged v21.1 trade-quality checks on frozen structural objective and shared entry evidence",
    )
    for key in (
        "entry_quality",
        "frozen_entry_cost",
        "aggression_response",
        "execution_deterioration",
        "liquidity_stress",
    ):
        if key in copy.evidence:
            signal.evidence[key] = copy.evidence[key]
    signal.evidence["entry_quality"]["policy"] = POLICY
    signal.evidence["entry_quality"]["minimum_net_rr"] = minimum_rr
    reasons = list(copy.gates)
    if book is not None:
        if book.event_ms > now or book.receipt_ms > now:
            reasons.append("EXECUTION_QUOTE_FUTURE")
        if not book.fresh(now, settings.book_stale_ms if settings else 5000):
            reasons.append("EXECUTION_QUOTE_STALE")
        if settings:
            features = book.features(now)
            if features.get("spread_bps", float("inf")) > settings.max_spread_bps:
                reasons.append("current spread exceeds gate")
            current_impact = book.impact(
                signal.direction, signal.risk.get("notional", settings.hypothetical_notional)
            )
            if not current_impact:
                reasons.append("insufficient executable visible depth")
            elif current_impact["impact_bps"] > settings.slippage_bps + settings.max_spread_bps / 2:
                reasons.append("estimated entry impact exceeds cost allowance")
    process = impact(signal, now, history=signal.evidence.get("impact_response_history", []))
    if process.get("contradicted"):
        reasons.append("MECHANISM_CONTRADICTED")
    target = signal.evidence.get("execution_target") or {}
    if target.get("state") == "UNTRADEABLE":
        reasons.append("TARGET_GEOMETRY_UNTRADEABLE")
    signal.gates = sorted(set(reasons))
    from .setup_scoring import score

    flow_confirmed = signal.evidence.get(
        "flow_confirmed", signal.evidence.get("confirmation", {}).get("passed", False)
    )
    score(signal, flow_confirmed, now)
    if signal.evidence["target_reachability"]["state"] == "LOW_REACHABILITY":
        signal.gates = sorted(set(signal.gates + ["TARGET_GEOMETRY_UNTRADEABLE"]))
    signal.gates = sorted(set(signal.gates + signal.evidence["stop_viability"]["reasons"]))
    if signal.gates:
        signal.final_tier = "REJECTED"
        signal.raw_tier = "F"
        signal.evidence["v22_counterfactual"] = envelope(
            signal,
            POLICY,
            now,
            reasons=signal.gates,
            entry=signal.entry,
            stop=signal.stop,
            execution_tp1=signal.tp1,
            structural_tp1=signal.evidence.get("structural_tp1"),
            hypothetical=True,
        )
    elif settings and settings.sss_research and signal.quality >= 95:
        signal.final_tier = "SSS RESEARCH · UNCALIBRATED"
    signal.evidence["v22"]["research_ablations"] = research_ablations(signal, now)
    signal.evidence.setdefault("v22_initial_assessment", deepcopy(signal.evidence["v22"]))
    if signal.gates and not had_scored_decision:
        # A provisional rejection must not trip the scanner's immutable scored-
        # decision latch. Existing confirmation windows may recover until expiry.
        signal.evidence.pop("score_components", None)


def research_ablations(signal, now):
    """Fixed score/gate ablations on one frozen executable plan, never authorities."""
    from .setup_scoring import TRADEABILITY_WEIGHTS, combine, engine

    full = signal.evidence["v22"]
    variants = dict(v22_full=dict(score=signal.quality, gates=list(signal.gates)))
    for name, component, ignored in (
        ("without_target_reachability", "target_reachability", {"TARGET_GEOMETRY_UNTRADEABLE"}),
        ("without_stop_viability", "stop_geometry", {"STOP_GEOMETRY_UNTRADEABLE"}),
    ):
        pieces = deepcopy(full["tradeability"]["components"])
        pieces[component] = dict(state="RESEARCH_ABLATED", score=None)
        trial = engine(pieces, TRADEABILITY_WEIGHTS)
        variants[name] = dict(
            score=combine(full["evidence_score"], trial["score"], full["confidence_cap"]),
            gates=[g for g in signal.gates if g not in ignored],
        )
    no_ofi = deepcopy(signal)
    no_ofi.evidence["event_ofi"] = dict(state="UNAVAILABLE")
    without = impact(no_ofi, now)
    pieces = deepcopy(full["tradeability"]["components"])
    pieces["follow_through"] = dict(score=without.get("score"), state=without["state"])
    trial = engine(pieces, TRADEABILITY_WEIGHTS)
    evidence = full["evidence_score"] + 0.30 * (
        (without.get("mechanism_score") or 0)
        - (signal.evidence.get("impact_efficiency", {}).get("mechanism_score") or 0)
    )
    variants["without_ofi"] = dict(
        score=combine(evidence, trial["score"], full["confidence_cap"]), gates=list(signal.gates)
    )
    return envelope(
        signal,
        "fixed-component-ablations-v22",
        now,
        variants=variants,
        scope="Score/gate ablations of the same frozen execution plan, not simulated alternative target construction",
        research_only=True,
    )


def frozen_delivery(signal, now):
    """Compact exact point-in-time snapshot, separate from the first ML decision."""
    e = signal.evidence
    q = e.get("entry_quality") or {}
    book = e.get("book") or {}
    execution = q.get("current_execution_cost_estimate") or {}
    v22 = e.get("v22") or {}
    bids, asks = book.get("bids", []), book.get("asks", [])
    return envelope(
        signal,
        "delivery-execution-v22",
        now,
        best_bid=float(bids[0][0]) if bids else None,
        best_ask=float(asks[0][0]) if asks else None,
        executable_price=q.get("executable_price"),
        quote_age_ms=q.get("quote_age_ms"),
        quote_state=q.get("quote_state"),
        book_event_ms=book.get("event_ms"),
        book_receipt_ms=book.get("receipt_ms"),
        spread_bps=book.get("spread_bps"),
        depth=deepcopy(book.get("depth", {})),
        estimated_impact=execution.get("residual_entry_impact_per_base"),
        remaining_gross_rr=q.get("gross_rr_now"),
        remaining_net_rr=q.get("cost_adjusted_rr_now"),
        distance_from_original_entry=(q["executable_price"] - signal.entry)
        if number(q.get("executable_price"))
        else None,
        distance_atr=q.get("post_signal_impulse_atr"),
        zone_relation=q.get("zone_relation"),
        target_reachability=deepcopy(e.get("target_reachability")),
        stop_viability=deepcopy(e.get("stop_viability")),
        ofi_state=deepcopy(e.get("event_ofi")),
        follow_through=deepcopy(e.get("impact_efficiency")),
        confirmation=deepcopy(e.get("delivery_confirmation")),
        setup_score=signal.quality,
        setup_assessment=deepcopy(v22),
        gates=list(signal.gates),
        cost=deepcopy(execution),
        original_plan=dict(
            entry=signal.entry, stop=signal.stop, tp1=signal.tp1, structural_tp1=e.get("structural_tp1")
        ),
    )
