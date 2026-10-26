"""Prospective, immutable policy observations. Never a production authority."""

import hashlib
import json
from copy import deepcopy
from statistics import mean, median

from .quality_v22 import enabled
from .setup_common import number

BANDS = (0, 20, 40, 50, 60, 70, 80, 90, 101)
POLICIES = (
    "v22_actual",
    "v211_frozen",
    "without_ofi",
    "without_impact_efficiency",
    "without_target_reachability",
    "without_stop_viability",
    "without_execution_target_replacement",
)
PROVENANCES = ("verified_prints", "recovered_prints", "monitored_ohlc_proxy", "research_only")
MIN_COHORT = 30  # Reporting safeguard only; never a trading or ML gate.


def initialize(store, now):
    key = "forward_evidence_epoch_v1"
    if store.get(key) is None:
        store.put(key, dict(start_ms=now, definition="prospective-policy-v1", historical_backfill=False))
    return store.get(key)


def variants(signal, now):
    """Freeze fixed-component controls
    shared entry/flow/liquidity gates survive.

    Controls are deliberately scoped, not a reconstruction of the entire older
    scanner. Unknown components stay unknown with unchanged fixed weights.
    """
    from .impact_efficiency import assess
    from .setup_scoring import EVIDENCE_WEIGHTS, TRADEABILITY_WEIGHTS, combine, engine, score

    e, full = signal.evidence, signal.evidence["v22"]
    controls = deepcopy(full.get("research_ablations", {}).get("variants", {}))
    result = {"v22_actual": dict(score=signal.quality, gates=list(signal.gates), target="execution_tp1")}
    for key in ("without_target_reachability", "without_stop_viability", "without_ofi"):
        result[key] = controls.get(key, dict(status="NOT_COMPARABLE", reason="Missing frozen assessment"))
        result[key]["target"] = "execution_tp1"
    # OFI changes the mechanism assessment
    # Preserve all shared non-mechanism gates.
    no_ofi = deepcopy(signal)
    no_ofi.evidence["event_ofi"] = dict(state="UNAVAILABLE")
    process = assess(no_ofi, now, history=e.get("impact_response_history", []))
    if result["without_ofi"].get("gates") is not None:
        gates = [g for g in signal.gates if g != "MECHANISM_CONTRADICTED"]
        if process.get("contradicted"):
            gates.append("MECHANISM_CONTRADICTED")
        result["without_ofi"]["gates"] = sorted(set(gates))
    pieces = deepcopy(full.get("evidence", {}).get("components", {}))
    trade = deepcopy(full.get("tradeability", {}).get("components", {}))
    if set(pieces) == set(EVIDENCE_WEIGHTS) and set(trade) == set(TRADEABILITY_WEIGHTS):
        pieces["mechanism"] = dict(state="RESEARCH_ABLATED", score=None)
        trade["follow_through"] = dict(state="RESEARCH_ABLATED", score=None)
        result["without_impact_efficiency"] = dict(
            score=combine(
                engine(pieces, EVIDENCE_WEIGHTS)["score"],
                engine(trade, TRADEABILITY_WEIGHTS)["score"],
                full["confidence_cap"],
            ),
            gates=[g for g in signal.gates if g != "MECHANISM_CONTRADICTED"],
            target="execution_tp1",
        )
    else:
        result["without_impact_efficiency"] = dict(status="NOT_COMPARABLE", reason="Missing components")
    baseline = e.get("v211_baseline_assessment") or {}
    result["v211_frozen"] = dict(
        gates=baseline.get("gates"),
        score=None,
        target="structural_tp1",
        scope=baseline.get("scope"),
        status="OBSERVED" if "gates" in baseline else "NOT_COMPARABLE",
    )
    # Alternative target checks use causal saved evidence on a COPY
    # No new book
    # or future candles, no production plan mutation, no alternate entry claim.
    structural = e.get("structural_tp1")
    if number(structural) and baseline.get("entry_quality"):
        trial = deepcopy(signal)
        trial.tp1 = structural
        trial.risk = deepcopy(e.get("v211_structural_risk", signal.risk))
        trial.evidence["entry_quality"] = deepcopy(baseline["entry_quality"])
        score(trial, bool(e.get("flow_confirmed", e.get("confirmation", {}).get("passed"))), now)
        shared = list(baseline.get("gates") or [])
        if e.get("impact_efficiency", {}).get("contradicted"):
            shared.append("MECHANISM_CONTRADICTED")
        if trial.evidence["target_reachability"]["state"] == "LOW_REACHABILITY":
            shared.append("TARGET_GEOMETRY_UNTRADEABLE")
        shared += trial.evidence["stop_viability"]["reasons"]
        result["without_execution_target_replacement"] = dict(
            score=trial.quality,
            gates=sorted(set(shared)),
            target="structural_tp1",
            scope="Frozen structural objective; shared v21.1 entry and v22 assessments; no alternate fill simulation",
        )
    else:
        result["without_execution_target_replacement"] = dict(
            status="NOT_COMPARABLE", reason="Missing structural control"
        )
    for row in result.values():
        row.setdefault("status", "OBSERVED")
        row["accepted"] = not row["gates"] if row.get("gates") is not None else None
        row["production_authority"] = False
    return result


def register(store, signal, now):
    epoch = store.get("forward_evidence_epoch_v1")
    e = signal.evidence
    decision = e.get("v22", {}).get("available_ms")
    if (
        not epoch
        or not enabled(signal)
        or signal.synthetic
        or not number(decision)
        or decision > now
        or signal.created_ms < epoch["start_ms"]
        or decision < epoch["start_ms"]
        or not e.get("flow_confirmed", e.get("confirmation", {}).get("passed", False))
    ):
        return
    # The existing causal path engine is mandatory
    # An observation is never
    # enrolled retrospectively after seeing its outcome.
    identity = store.db.execute(
        "SELECT candidate_identity FROM candidate_identities WHERE signal_id=?", (signal.id,)
    ).fetchone()
    identity = identity[0] if identity else signal.id
    ident = hashlib.sha256((signal.source + ":" + identity).encode()).hexdigest()[:32]
    if store.db.execute("SELECT 1 FROM policy_opportunities WHERE id=?", (ident,)).fetchone():
        return
    from .setup_research import register as register_path

    path_signal = deepcopy(signal)
    path_signal.id = signal.id + ":forward"
    register_path(store, path_signal, now)
    job = store.db.execute(
        "SELECT payload FROM setup_path_jobs WHERE signal_id=?", (path_signal.id,)
    ).fetchone()
    if not job:
        return
    plan = json.loads(job[0])
    if plan["decision_ms"] != decision:
        return
    full = e["v22"]
    payload = dict(
        signal_id=signal.id,
        path_id=path_signal.id,
        source=signal.source,
        symbol=signal.symbol,
        horizon=signal.horizon_profile,
        family=signal.family,
        direction=signal.direction,
        regime=signal.regime,
        decision_ms=decision,
        feature_schema=signal.feature_schema_version,
        score=signal.quality,
        evidence_score=full.get("evidence_score"),
        tradeability_score=full.get("tradeability_score"),
        entry=signal.entry,
        stop=signal.stop,
        execution_target=signal.tp1,
        structural_target=e.get("structural_tp1"),
        reachability_state=e.get("target_reachability", {}).get("state", "UNKNOWN"),
        stop_state=e.get("stop_viability", {}).get("state", "UNKNOWN"),
        impact_state=e.get("impact_efficiency", {}).get("state", "UNKNOWN"),
        ofi_state=e.get("event_ofi", {}).get("state", "UNKNOWN"),
        variants=variants(signal, decision),
        frozen_path=plan,
        research_only=True,
        production_authority=False,
    )
    store.db.execute(
        "INSERT OR IGNORE INTO policy_opportunities VALUES(?,?,?,?,?,?)",
        (ident, identity, signal.source, signal.symbol, decision, json.dumps(payload)),
    )


def ingest_paths(store, now, limit=200):
    """Incremental join to existing immutable OHLC research labels, no replay."""
    rows = store.db.execute(
        "SELECT o.id,o.payload,l.available_ms,l.payload FROM policy_opportunities o "
        "JOIN setup_path_labels l ON l.signal_id=json_extract(o.payload,'$.path_id') "
        "WHERE l.available_ms<=? AND NOT EXISTS (SELECT 1 FROM policy_results r WHERE r.opportunity_id=o.id "
        "AND r.provenance='research_only') ORDER BY l.available_ms LIMIT ?",
        (now, limit),
    ).fetchall()
    with store.db:
        for ident, raw, available, label in rows:
            o, p = json.loads(raw), json.loads(label)
            if p.get("source") != o["source"] or p.get("decision_ms") != o["decision_ms"]:
                continue
            for policy, variant in o["variants"].items():
                outcome = p.get("policies", {}).get(variant.get("target"), {})
                metrics = dict(
                    net_r=outcome.get("net_r") if outcome.get("complete") else None,
                    mfe=p.get("mfe_lower_bound_r"),
                    mae=p.get("mae_lower_bound_r"),
                    extrema_scope=p.get("extrema_scope"),
                    complete=outcome.get("complete", False),
                    stop_before_05=p.get("stop_before_0.5r"),
                    stop_before_1=p.get("stop_before_1r"),
                    execution_target_reached=p.get("policies", {}).get("execution_tp1", {}).get("outcome")
                    == "TARGET"
                    if p.get("coverage_complete")
                    else None,
                    structural_target_reached=p.get("policies", {}).get("structural_tp1", {}).get("outcome")
                    == "TARGET"
                    if p.get("coverage_complete")
                    else None,
                    time_exit_r=p.get("time_exit_r"),
                    milestones=p.get("milestones", {}),
                    holding_duration_ms=outcome.get("exit_ms", 0) - p["entered_ms"]
                    if p.get("entered_ms") and outcome.get("exit_ms")
                    else None,
                    time_to_execution_target_ms=p.get("time_to_execution_tp1_ms"),
                    time_to_structural_target_ms=p.get("time_to_structural_tp1_ms"),
                    accepted=variant["accepted"],
                    comparison_status=variant["status"],
                    account_fill_verified=False,
                )
                store.db.execute(
                    "INSERT OR IGNORE INTO policy_results VALUES(?,?,?,?,?)",
                    (ident, policy, "research_only", available, json.dumps(metrics)),
                )
    return len(rows)


def aggregate(rows):
    observed = [r for r in rows if number(r.get("net_r")) and r.get("complete")]

    def avg(key):
        values = [r[key] for r in observed if isinstance(r.get(key), (int, float))]
        return mean(values) if values else None

    mature = len(observed) >= MIN_COHORT
    return dict(
        samples=len(rows),
        complete_samples=len(observed),
        status="DESCRIPTIVE" if mature else "INSUFFICIENT SAMPLE",
        mean_net_r=avg("net_r"),
        median_net_r=median([r["net_r"] for r in observed]) if observed else None,
        positive_rate=mean([r["net_r"] > 0 for r in observed]) if mature else None,
        mean_mfe=avg("mfe"),
        mean_mae=avg("mae"),
        stop_before_05_rate=avg("stop_before_05") if mature else None,
        reach_05_rate=mean(["0.5" in r["milestones"] for r in observed if "milestones" in r])
        if mature and any("milestones" in r for r in observed)
        else None,
        reach_1_rate=mean(["1.0" in r["milestones"] for r in observed if "milestones" in r])
        if mature and any("milestones" in r for r in observed)
        else None,
        target_reach_rate=avg("execution_target_reached") if mature else None,
    )


def report(store, now, limit=5000):
    rows = store.db.execute(
        "SELECT o.payload,r.policy_id,r.provenance,r.payload FROM policy_results r "
        "JOIN policy_opportunities o ON o.id=r.opportunity_id WHERE r.available_ms<=? "
        "ORDER BY r.available_ms DESC LIMIT ?",
        (now, limit + 1),
    ).fetchall()
    groups = {}
    bands = {}
    cohorts = {}
    paired = {}
    for raw, policy, provenance, metrics in rows[:limit]:
        o, m = json.loads(raw), json.loads(metrics)
        paired.setdefault((o["signal_id"], provenance, o["source"]), {})[policy] = m
        key = policy + ":" + provenance + ":" + o["source"]
        groups.setdefault(key, []).append(m)
        if m.get("accepted") is not True or m.get("comparison_status") != "OBSERVED":
            continue
        value = o["variants"].get(policy, {}).get("score")
        if not number(value):
            continue
        band = next((f"{a}–{min(b, 100)}" for a, b in zip(BANDS, BANDS[1:]) if a <= value < b), "UNKNOWN")
        bands.setdefault(key + ":" + band, []).append(m)
        for dimension in (
            "source",
            "family",
            "horizon",
            "direction",
            "regime",
            "reachability_state",
            "stop_state",
            "impact_state",
            "ofi_state",
        ):
            cohorts.setdefault(key + ":" + dimension + ":" + str(o.get(dimension, "UNKNOWN")), []).append(m)
    return dict(
        label="FORWARD OBSERVATION / NOT VERIFIED ACCOUNT PERFORMANCE",
        epoch=store.get("forward_evidence_epoch_v1"),
        opportunities=store.db.execute("SELECT count(*) FROM policy_opportunities").fetchone()[0],
        policies={
            k: dict(
                all_observed=len(v),
                accepted=aggregate(
                    [r for r in v if r.get("accepted") is True and r.get("comparison_status") == "OBSERVED"]
                ),
            )
            for k, v in groups.items()
        },
        score_bands={k: aggregate(v) for k, v in bands.items()},
        cohorts={k: aggregate(v) for k, v in cohorts.items()},
        paired_comparisons=paired_report(paired),
        minimum_cohort=MIN_COHORT,
        truncated=len(rows) > limit,
        limit=limit,
        provenance_groups=list(PROVENANCES),
        limitation="OHLC research paths are lower bounds, not prints or fills. No pooling across policy, source or provenance. Missing metrics remain unknown.",
    )


def record_promotion(store, record, now):
    required = {
        "policy_id",
        "parent_policy",
        "development_period",
        "forward_start",
        "evaluation_end",
        "sample_counts",
        "net_r",
        "mfe",
        "mae",
        "loss_avoidance",
        "winner_rejection",
        "subgroup_stability",
        "attempted_variants",
        "code_commit",
    }
    if required - record.keys() or record.get("automatic") or record["evaluation_end"] > now:
        raise ValueError("Complete manual research review required; no automatic promotion")
    ident = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()[:32]
    with store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO policy_promotions VALUES(?,?,?)",
            (ident, now, json.dumps(record | dict(manual=True, production_authority=False))),
        )
    return ident


def ingest_recorded_labels(store, now, limit=200):
    """Actual-policy outcomes only. Each methodology remains a separate group.

    Prints/proxy labels cannot evaluate alternate targets without their own
    causal replay. Consequently they never populate shadow variant results.
    """
    rows = store.db.execute(
        "SELECT o.id,o.payload,l.policy,l.available_ms,l.payload,s.payload FROM policy_opportunities o "
        "JOIN ml_snapshots s ON s.signal_id=json_extract(o.payload,'$.signal_id') AND s.stage='decision' "
        "JOIN ml_labels l ON l.snapshot_id=s.id WHERE l.available_ms<=? AND l.policy IN "
        "('prints-v1','prints-recovered-v1','monitored-ohlc-v1') AND json_extract(l.payload,'$.complete')=1 "
        "AND NOT EXISTS(SELECT 1 FROM policy_results r WHERE r.opportunity_id=o.id AND r.policy_id='v22_actual' AND r.provenance="
        "CASE l.policy WHEN 'prints-v1' THEN 'verified_prints' WHEN 'prints-recovered-v1' THEN 'recovered_prints' ELSE 'monitored_ohlc_proxy' END) "
        "ORDER BY l.available_ms LIMIT ?",
        (now, limit),
    ).fetchall()
    count = 0
    with store.db:
        for ident, raw, policy, available, label, snapshot in rows:
            o, p, s = json.loads(raw), json.loads(label), json.loads(snapshot)
            plan = s.get("signal", {})
            if s.get("source") != o["source"] or any(
                plan.get(k) != o[v]
                for k, v in (("entry", "entry"), ("stop", "stop"), ("tp1", "execution_target"))
            ):
                continue
            # A different decision time is a different causal opportunity.
            if s.get("decision_ms") != o["decision_ms"]:
                continue
            if policy == "prints-recovered-v1" and p.get("certificate", {}).get("status") != "VERIFIED":
                continue
            provenance = {
                "prints-v1": "verified_prints",
                "prints-recovered-v1": "recovered_prints",
                "monitored-ohlc-v1": "monitored_ohlc_proxy",
            }[policy]
            research = p.get("research_path", {})
            metrics = dict(
                net_r=p.get("net_r"),
                complete=True,
                accepted=o["variants"]["v22_actual"]["accepted"],
                comparison_status="OBSERVED",
                mfe=p.get("mfe_r")
                if policy != "monitored-ohlc-v1"
                else research.get("observed_mfe_lower_bound_r"),
                mae=p.get("mae_r")
                if policy != "monitored-ohlc-v1"
                else -research["observed_mae_lower_bound_r"]
                if number(research.get("observed_mae_lower_bound_r"))
                else None,
                holding_duration_ms=p["exit_ms"] - p["entry_ms"]
                if p.get("exit_ms") and p.get("entry_ms")
                else None,
                account_fill_verified=False,
                metric_scope="Existing immutable " + policy + "; unavailable path metrics remain unknown",
            )
            store.db.execute(
                "INSERT OR IGNORE INTO policy_results VALUES(?,?,?,?,?)",
                (ident, "v22_actual", provenance, available, json.dumps(metrics)),
            )
            count += 1
    return count


def paired_report(pairs):
    groups = {}
    for (_, provenance, source), policies in pairs.items():
        parent = policies.get("v22_actual")
        if (
            not parent
            or parent.get("comparison_status") != "OBSERVED"
            or not parent.get("complete")
            or not number(parent.get("net_r"))
        ):
            continue
        for policy, trial in policies.items():
            if (
                policy == "v22_actual"
                or trial.get("comparison_status") != "OBSERVED"
                or not trial.get("complete")
                or not number(trial.get("net_r"))
            ):
                continue
            groups.setdefault(policy + ":" + provenance + ":" + source, []).append((parent, trial))
    result = {}
    for key, rows in groups.items():
        result[key] = dict(
            paired_samples=len(rows),
            status="DESCRIPTIVE" if len(rows) >= MIN_COHORT else "INSUFFICIENT SAMPLE",
            loss_avoidance_count=sum(
                a["accepted"] is True and b["accepted"] is False and a["net_r"] < 0 for a, b in rows
            ),
            winner_rejection_count=sum(
                a["accepted"] is True and b["accepted"] is False and a["net_r"] > 0 for a, b in rows
            ),
            incremental_net_r=sum(
                (b["net_r"] if b["accepted"] is True else 0) - (a["net_r"] if a["accepted"] is True else 0)
                for a, b in rows
            ),
            scope="Paired fixed-plan hypothetical outcomes; abstention is zero exposure, not an observed trade",
        )
    return result
