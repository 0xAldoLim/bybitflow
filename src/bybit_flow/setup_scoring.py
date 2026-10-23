"""One public Setup Score, from two non-probabilistic internal assessments."""

from copy import deepcopy
from statistics import mean

from .impact_efficiency import assess as impact
from .scoring import tier
from .setup_common import ASSUMPTION, causal, clamp, envelope, number, positive
from .stop_viability import assess as stop_viability
from .target_reachability import path, reachability
from .v2_evidence import location

SCORE_PROFILE = "bybitflow-v22-setup-1"
EVIDENCE_WEIGHTS = dict(location=30, structure=25, mechanism=30, context=15)
TRADEABILITY_WEIGHTS = dict(follow_through=30, target_reachability=30, stop_geometry=20, execution=20)


def combine(evidence, tradeability, confidence):
    base = 0.55 * evidence + 0.45 * tradeability
    return round(clamp(min(0.75 * base + 0.25 * min(evidence, tradeability), confidence)), 1)


def engine(components, weights):
    """Unknown has no supporting credit and keeps an explicit uncertainty range.

    Fixed weights are not redistributed. UNKNOWN is never relabeled adverse.
    """
    known = {k: v["score"] for k, v in components.items() if number(v.get("score"))}
    earned = sum(weights[k] * clamp(value) / 100 for k, value in known.items())
    unknown_weight = sum(weights[k] for k in weights if k not in known)
    return dict(
        score=earned,
        supported_score=earned,
        possible_score_upper=min(100, earned + unknown_weight),
        known_weight=100 - unknown_weight,
        components=components,
        weights=weights,
    )


def context(signal, now):
    e = signal.evidence
    alignment = e.get("market_alignment") or {}
    observations = {}
    if causal(alignment, signal, now):
        state = alignment.get("market_alignment_state", alignment.get("alignment"))
        if state in {"MARKET_ALIGNED", "IDIOSYNCRATIC_DIVERGENCE"}:
            observations["factors"] = 80.0
        elif state == "MARKET_NEUTRAL":
            observations["factors"] = 50.0
        elif alignment.get("blocked"):
            observations["factors"] = 0.0
    gate = e.get("v8_gate") or {}
    if causal(gate, signal, now):
        for key in ("spot_perp", "breadth"):
            row = gate.get("feature_states", {}).get(key, {})
            if row.get("gate_ready") and row.get("available_ms", now + 1) <= now:
                score = {"SUPPORT": 80.0, "NEUTRAL": 50.0, "ADVERSE": 0.0}.get(row.get("state"))
                if score is not None:
                    observations[key] = score
    remote = e.get("cross_exchange") or {}
    if remote.get("available") and remote.get("available_ms", remote.get("checked_ms", now + 1)) <= now:
        observations["cross_venue"] = 75.0 if remote.get("agreement") is True else 50.0
    return envelope(
        signal,
        "setup-context-v22",
        now,
        state="OBSERVED" if observations else "UNKNOWN",
        score=mean(observations.values()) if observations else None,
        observations=observations,
        explanation="Available factor/venue context; unknown is not adverse",
    )


def structure(signal, now):
    e = signal.evidence
    frozen = e.get("frozen_structure") or {}
    trigger = e.get("structural_trigger") or {}
    valid = bool(
        trigger.get("valid")
        and trigger.get("available_ms", now + 1) <= now
        and frozen.get("available_ms", now + 1) <= now
        and not frozen.get("reasons")
        and frozen.get("frozen_reference") is not None
    )
    interval = frozen.get("setup_interval_ms")
    age = max(0, now - trigger.get("available_ms", now)) if valid else None
    # Existing event identity/reclaim/retest gates remain authoritative.
    freshness = 1 / (1 + age / (2 * interval)) if valid and positive(interval) else 1
    score = 100 * freshness if valid else None
    return envelope(
        signal,
        "setup-structure-v22",
        now,
        state="VALID" if valid else "UNKNOWN",
        score=score,
        family=signal.family,
        structure_event_id=frozen.get("structure_event_id"),
        structural_age_ms=age,
        frozen_reference=frozen.get("frozen_reference"),
        reasons=frozen.get("reasons", []),
        explanation="Frozen family-specific structural premise",
    )


def execution(signal, now):
    q = signal.evidence.get("entry_quality") or {}
    if not causal(q, signal, now, 5000):
        return envelope(signal, "setup-execution-v22", now, state="UNKNOWN", score=None)
    rr = q.get("cost_adjusted_rr_now")
    cost = q.get("current_execution_cost_estimate", {}).get("cost_per_base")
    price = q.get("executable_price")
    distance = abs(price - signal.stop) if number(price) else None
    fraction = cost / distance if number(cost) and positive(distance) else None
    spread = q.get("quote_provenance", {}).get("spread_bps")
    normal = signal.evidence.get("normal_spread", {}).get("median_bps")
    relative_spread = spread / normal if positive(normal) and number(spread) else None
    if q.get("quote_state") != "QUOTE_FRESH" or not q.get("acceptable"):
        value = 0.0
    else:
        value = (65 * rr / (rr + 1) if positive(rr) else 0) + (
            35 / (1 + 2 * fraction) if number(fraction) else 0
        )
        if number(relative_spread) and relative_spread > 1:
            value /= 1 + 0.25 * (relative_spread - 1)
    return envelope(
        signal,
        "setup-execution-v22",
        now,
        state="ACCEPTABLE" if q.get("acceptable") else "INVALID",
        score=clamp(value),
        net_rr=rr,
        cost_risk_fraction=fraction,
        relative_spread=relative_spread,
        quote_state=q.get("quote_state"),
        zone_relation=q.get("zone_relation"),
        price=price,
    )


def edge_age(signal, now):
    e = signal.evidence
    trigger = e.get("structural_trigger", {}).get("available_ms")
    confirmed = e.get("confirmation", {}).get("available_ms")
    process = e.get("impact_efficiency") or {}
    ofi = e.get("event_ofi") or {}
    observed = e.get("edge_observations_v22") or {}
    if not causal(observed, signal, now):
        observed = {}
    strongest = observed.get("strongest_impact") or {}
    if (
        process.get("coverage_complete")
        and number(process.get("score"))
        and number(process.get("window_end_ms"))
        and process["window_end_ms"] <= now
        and (not strongest or process["score"] > strongest.get("score", -1))
    ):
        strongest = dict(score=process["score"], at_ms=process["window_end_ms"])
    aligned = observed.get("last_supportive_ofi_ms")
    touch = ofi.get("windows", {}).get("60", {}).get("touch", {})
    if (
        causal(ofi, signal, now, 5000)
        and ofi.get("state") == "READY"
        and number(touch.get("directional"))
        and touch["directional"] > 0
    ):
        aligned = ofi.get("source_ms")
    e["edge_observations_v22"] = envelope(
        signal,
        "edge-observations-v22",
        now,
        strongest_impact=strongest,
        last_supportive_ofi_ms=aligned,
        scope="Strongest observed response since this candidate was scored; not a search of future prices",
    )
    deadline = signal.holding_deadline_ms or signal.expires_ms
    return envelope(
        signal,
        "edge-age-v1",
        now,
        structural_age_ms=now - trigger if number(trigger) and trigger <= now else None,
        confirmation_age_ms=now - confirmed if number(confirmed) and confirmed <= now else None,
        strongest_impact_age_ms=now - strongest["at_ms"] if strongest else None,
        ofi_alignment_age_ms=now - aligned if number(aligned) and aligned <= now else None,
        remaining_horizon_ms=max(0, deadline - now),
    )


def score(signal, flow_confirmed, now):
    e = signal.evidence
    q = e.get("entry_quality") or {}
    price = q.get("executable_price", signal.entry)
    if not number(price):
        price = signal.entry
    copy = deepcopy(signal)
    if number(price):
        copy.entry = price
    where = location(copy, now)
    where.update(source=signal.source, symbol=signal.symbol, available_ms=now, policy="setup-location-v22")
    thesis = structure(signal, now)
    process = impact(signal, now, history=e.get("impact_response_history", []))
    e["impact_efficiency"] = process
    capacity = e.get("horizon_movement_capacity") or {}
    reach = reachability(signal, signal.tp1, price, capacity, now)
    friction = path(signal, signal.tp1, price, now)
    # A heavy volume-weighted path materially constrains the same target concept.
    target_scores = [r["score"] for r in (reach, friction) if number(r.get("score"))]
    target_score = min(target_scores) if target_scores else None
    if friction["state"] == "HEAVY_FRICTION" and target_score is not None:
        target_score = min(target_score, 35.0)
    target = envelope(
        signal,
        "target-reachability-v1",
        now,
        state=reach["state"],
        score=target_score,
        reachability=reach,
        path=friction,
    )
    stops = stop_viability(
        signal,
        price,
        now,
        minimum_rr=q.get("minimum_net_rr", 1.5),
        cost=q.get("current_execution_cost_estimate", {}).get("cost_per_base"),
    )
    execute = execution(signal, now)
    evidence = engine(
        dict(
            location=where,
            structure=thesis,
            mechanism=dict(score=process.get("mechanism_score"), state=process["state"]),
            context=context(signal, now),
        ),
        EVIDENCE_WEIGHTS,
    )
    tradeability = engine(
        dict(
            follow_through=dict(score=process.get("score"), state=process["state"]),
            target_reachability=target,
            stop_geometry=stops,
            execution=execute,
        ),
        TRADEABILITY_WEIGHTS,
    )
    required = bool(
        flow_confirmed
        and process.get("coverage_complete")
        and thesis["state"] == "VALID"
        and signal.risk.get("accepted")
        and q.get("quote_state") == "QUOTE_FRESH"
        and q.get("acceptable")
        and causal(q, signal, now, 5000)
    )
    optional = dict(
        ofi=(e.get("event_ofi") or {}).get("state") == "READY",
        profile=where["profile_ready"],
        capacity=reach["state"] != "UNKNOWN",
        path=friction["state"] != "UNKNOWN",
    )
    confidence = (
        min(100 * (process.get("flow_trust") or 0), 80 + 5 * sum(optional.values())) if required else 0
    )
    quality = combine(evidence["score"], tradeability["score"], confidence)
    strengths = []
    weaknesses = []
    for label, row in (
        ("Location", where),
        ("Structure", thesis),
        ("Flow response", process),
        ("Target path", target),
        ("Stop viability", stops),
        ("Execution", execute),
    ):
        value = row.get("score")
        if value is None:
            weaknesses.append(label + ": evidence unavailable")
        elif value >= 65:
            strengths.append(label + ": " + row["state"].replace("_", " ").lower())
        else:
            weaknesses.append(label + ": " + row["state"].replace("_", " ").lower())
    e["v22"] = envelope(
        signal,
        "setup-score-v22",
        now,
        score_profile=SCORE_PROFILE,
        setup_score=quality,
        evidence_score=evidence["score"],
        tradeability_score=tradeability["score"],
        evidence=evidence,
        tradeability=tradeability,
        confidence_cap=confidence,
        optional_readiness=optional,
        strengths=strengths[:4],
        weaknesses=weaknesses[:4],
        assumption=ASSUMPTION,
        grade=tier(quality),
        coverage_complete=required,
    )
    e.update(target_reachability=target, stop_viability=stops, edge_age=edge_age(signal, now))
    signal.quality = quality
    signal.raw_tier = "F" if signal.gates else tier(quality)
    signal.final_tier = "REJECTED" if signal.gates else "RESEARCH"
    e["score_profile"] = SCORE_PROFILE
    e["score_components"] = {
        "evidence": dict(weight=55, score=evidence["score"], earned=0.55 * evidence["score"]),
        "tradeability": dict(weight=45, score=tradeability["score"], earned=0.45 * tradeability["score"]),
    }
    e["score_reasons"] = strengths + weaknesses + [f"Evidence confidence cap: {confidence:g}/100", ASSUMPTION]
    signal.qualification = dict(
        status="Uncalibrated",
        probability=None,
        reason="Deterministic v2.2 Setup Score; research-grounded assumptions, not a win probability",
    )
    return signal
