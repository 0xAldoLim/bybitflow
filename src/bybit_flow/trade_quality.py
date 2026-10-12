"""Bounded decision/outcome audit. No tape scans, fitted cutoffs or rewritten labels."""

import json
import math
from collections import Counter, defaultdict
from statistics import mean, median

from .horizons import PROFILES

POLICIES = ("prints-v1", "monitored-ohlc-v1")
FAILURES = (
    "BAD_REGIME_MATCH",
    "BAD_LOCATION",
    "FALSE_BREAKOUT",
    "FAILED_RECLAIM",
    "LATE_ENTRY",
    "CHASED_ENTRY",
    "POOR_FLOW_RESPONSE",
    "ABSORPTION_AGAINST_TRADE",
    "EXHAUSTED_MOVE",
    "LIQUIDITY_STRESS",
    "INSUFFICIENT_EXECUTABLE_DEPTH",
    "COST_TOO_LARGE_VS_STOP",
    "TARGET_PATH_TOO_DIFFICULT",
    "STOP_INSIDE_NORMAL_NOISE",
    "OVERWIDE_STOP_POOR_OFFER",
    "BTC_ETH_LEADER_CONFLICT",
    "SPOT_PERP_WEAKNESS",
    "REPEATED_FAILED_ATTEMPT",
    "CORRELATED_DUPLICATE_EXPOSURE",
    "HORIZON_SIGNAL_LIFE_MISMATCH",
    "DATA_QUALITY_FAILURE",
    "UNKNOWN",
)
ENTRY_FAILURES = (
    "ENTRY_PRICE_CHASED",
    "ENTRY_ZONE_BROKEN_ADVERSE",
    "ENTRY_TRIGGER_EXPIRED",
    "ENTRY_PLAN_EXPIRED",
    "RR_DEGRADED",
    "EXECUTION_COST_UNKNOWN",
    "EXECUTION_COST_INVALID",
    "INVALID_EXECUTION_GEOMETRY",
    "STOP_ALREADY_BREACHED",
    "TARGET_ALREADY_PASSED",
    "EXECUTION_QUOTE_STALE",
)
TAXONOMY = {
    code: dict(
        status="actively detectable",
        meaning="Causal decision diagnostic; association is not a proven cause of loss",
    )
    for code in (*FAILURES, *ENTRY_FAILURES, "RANGE_ACCEPTANCE_OUTSIDE", "RETEST_FAILED", "RETEST_EXPIRED")
}
for _code in (
    "FALSE_BREAKOUT",
    "FAILED_RECLAIM",
    "LATE_ENTRY",
    "EXHAUSTED_MOVE",
    "LIQUIDITY_STRESS",
    "OVERWIDE_STOP_POOR_OFFER",
    "CORRELATED_DUPLICATE_EXPOSURE",
    "HORIZON_SIGNAL_LIFE_MISMATCH",
):
    TAXONOMY[_code]["status"] = "not yet implemented"  # No retrospective loss-cause detector.
for _code in ("FAILED_RECLAIM", "LIQUIDITY_STRESS"):
    TAXONOMY[_code]["status"] = "actively detectable"
TAXONOMY["CHASED_ENTRY"] = dict(
    status="obsolete for v21.1", meaning="Legacy v21 overall entry check; not proof of chasing"
)
TAXONOMY["REPEATED_FAILED_ATTEMPT"]["status"] = "research-only"


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def bucket(value):
    return (
        f"{min(80, int(value // 20) * 20)}–{min(100, int(value // 20) * 20 + 20)}"
        if number(value)
        else "UNKNOWN"
    )


def failure_classes(s):
    """Observable causal associations, not proof of why a particular trade lost."""
    e = s.get("evidence", {})
    v = e.get("v2", {})
    sign = 1 if s["direction"] == "LONG" else -1
    flags = []
    if s["family"] == "range_rejection" and s["regime"] != "range":
        flags.append("BAD_REGIME_MATCH")
    if v.get("location", {}).get("score", 100) < 20:
        flags.append("BAD_LOCATION")
    flow = e.get("flow", {})
    if v.get("mechanism", {}).get("state") == "INEFFICIENT_AGGRESSION":
        flags.append("POOR_FLOW_RESPONSE")
    if flow.get("absorption_short" if sign > 0 else "absorption_long"):
        flags.append("ABSORPTION_AGAINST_TRADE")
    risk = abs(s["entry"] - s["stop"])
    cost = s.get("risk", {}).get("cost_per_base")
    if risk and number(cost) and cost / risk >= 0.5:
        flags.append("COST_TOO_LARGE_VS_STOP")
    if e.get("execution", {}).get("stop_noise_ratio", 1) < 1:
        flags.append("STOP_INSIDE_NORMAL_NOISE")
    if "impact" in s.get("risk", {}) and s["risk"]["impact"] is None:
        flags.append("INSUFFICIENT_EXECUTABLE_DEPTH")
    if v.get("trade_offer", {}).get("path_state") == "HEAVY_FRICTION":
        flags.append("TARGET_PATH_TOO_DIFFICULT")
    gate = e.get("v8_gate", {})
    for name, code in (("spot_perp", "SPOT_PERP_WEAKNESS"), ("breadth", "BTC_ETH_LEADER_CONFLICT")):
        if gate.get("feature_states", {}).get(name, {}).get("state") == "ADVERSE":
            flags.append(code)
    q = e.get("entry_quality", {})
    if q.get("schema") == "entry-quality-v2":
        flags.extend(q.get("reasons", []))
    elif q.get("entry_not_chased") is False:
        flags.append("CHASED_ENTRY")
    structure = e.get("frozen_structure", {})
    flags.extend(code for code in structure.get("reasons", []) if code in TAXONOMY)
    if e.get("liquidity_stress", {}).get("state") == "LIQUIDITY_STRESSED":
        flags.append("LIQUIDITY_STRESS")
    if e.get("attempt_memory", {}).get("prior_failed_attempts", 0):
        flags.append("REPEATED_FAILED_ATTEMPT")
    if not s.get("coverage", {}).get("trade_window_complete"):
        flags.append("DATA_QUALITY_FAILURE")
    return sorted(set(flags)) or ["UNKNOWN"]


def load_rows(db, now, days=30, source=None, horizon=None, family=None, limit=5000):
    if not 1 <= days <= 365:
        raise ValueError("days must be between 1 and 365")
    conditions = [
        "m.stage='decision'",
        "m.schema_version='candidate-v20'",
        "m.decision_ms>=?",
        "m.decision_ms<=?",
    ]
    args = [now - days * 86400000, now]
    for key, value in (("source", source), ("horizon_profile", horizon), ("family", family)):
        if value:
            conditions.append(f"json_extract(m.payload,'$.signal.{key}')=?")
            args.append(value)
    result = []
    payload_bytes = 0
    truncated = False
    for ident, raw, current, status, message, identity in db.execute(
        "SELECT m.id,json_remove(m.payload,'$.values','$.sequence','$.feature_metadata',"
        "'$.signal.evidence.flow.profile','$.signal.evidence.flow.cvd_path',"
        "'$.signal.evidence.volume_profile.raw_volume_profile',"
        "'$.signal.evidence.volume_profile.effective_volume_profile',"
        "'$.signal.evidence.flow.quality.effective_volume_profile'),NULL,o.status,o.message_id,coalesce(c.candidate_identity,m.signal_id) FROM ml_snapshots m "
        "LEFT JOIN candidate_identities c ON c.signal_id=m.signal_id "
        "LEFT JOIN outbox o "
        "ON o.key='research:'||m.signal_id||':initial' WHERE "
        + " AND ".join(conditions)
        + " ORDER BY m.decision_ms DESC,m.id LIMIT ?",
        (*args, limit + 1),
    ):
        payload_bytes += len(raw.encode("utf-8"))
        if len(result) >= limit or payload_bytes > 32 * 1024 * 1024:
            truncated = True
            break
        labels = [
            dict(policy=p, payload=json.loads(v))
            for p, v in db.execute(
                "SELECT policy,json_remove(payload,'$.fills','$.trades','$.path') FROM ml_labels WHERE snapshot_id=? AND available_ms<=?",
                (ident, now),
            )
        ]
        result.append(
            dict(
                snapshot_id=ident,
                candidate_identity=identity,
                snapshot=json.loads(raw),
                current=json.loads(current) if current else {},
                receipt=[status, message] if status else None,
                labels=labels,
            )
        )
    return result, truncated


def metrics(rows, policy):
    complete, entered = [], 0
    for x in rows:
        label = next((label_row["payload"] for label_row in x["labels"] if label_row["policy"] == policy), {})
        entry = label.get("actual_entry_ms") if policy == "prints-v1" else label.get("entry_available_ms")
        entered += bool(entry or label.get("complete") and number(label.get("net_r")))
        if label.get("complete") and number(label.get("net_r")):
            complete.append((x, label))
    values = [label_row["net_r"] for _, label_row in complete]
    wins, losses = [r for r in values if r > 0], [r for r in values if r < 0]
    outcomes = Counter(
        str(label_row.get("outcome", label_row.get("exit_reason", "UNKNOWN"))).upper()
        for _, label_row in complete
    )
    streak = largest = 0
    cumulative = peak = drawdown = 0
    for x, label_row in sorted(complete, key=lambda p: (p[1].get("exit_ms", 0), p[0]["snapshot_id"])):
        streak = streak + 1 if label_row["net_r"] < 0 else 0
        largest = max(largest, streak)
        cumulative += label_row["net_r"]
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)

    def observed(key, outcome=None):
        vals = [
            label_row[key]
            for _, label_row in complete
            if number(label_row.get(key))
            and (
                not outcome
                or str(label_row.get("outcome", label_row.get("exit_reason", ""))).upper() == outcome
            )
        ]
        return dict(samples=len(vals), mean=mean(vals) if vals else None)

    def duration(outcome):
        vals = [
            (label_row["exit_ms"] - (label_row.get("actual_entry_ms") or label_row.get("entry_available_ms")))
            / 60000
            for _, label_row in complete
            if str(label_row.get("outcome", label_row.get("exit_reason", ""))).upper() == outcome
            and label_row.get("exit_ms")
            and (label_row.get("actual_entry_ms") or label_row.get("entry_available_ms"))
        ]
        return dict(samples=len(vals), median_minutes=median(vals) if vals else None)

    costs = [
        100
        * x["snapshot"]["signal"]["risk"]["cost_per_base"]
        / abs(x["snapshot"]["signal"]["entry"] - x["snapshot"]["signal"]["stop"])
        for x, _ in complete
        if abs(x["snapshot"]["signal"]["entry"] - x["snapshot"]["signal"]["stop"]) > 0
        and number(x["snapshot"]["signal"].get("risk", {}).get("cost_per_base"))
    ]
    days = len({x["snapshot"]["decision_ms"] // 86400000 for x, _ in complete})
    markouts = {}
    for basis in ("decision", "entry"):
        for checkpoint in ("execution_1", "execution_3", "setup_1"):
            observed_values = [
                label_row.get("research_path", {})
                .get("markouts", {})
                .get(basis, {})
                .get(checkpoint, {})
                .get("original_r")
                for _, label_row in complete
            ]
            valid = [value for value in observed_values if number(value)]
            markouts[basis + ":" + checkpoint] = dict(
                samples=len(valid), mean_original_r=mean(valid) if valid else None
            )
    return dict(
        signals=len(rows),
        entered_signals=entered,
        complete_outcomes=len(values),
        wins=len(wins),
        losses=len(losses),
        target_rate=outcomes["TARGET"] / len(values) if values else None,
        stop_rate=outcomes["STOP"] / len(values) if values else None,
        time_exit_rate=(outcomes["TIME_EXIT"] + outcomes["TIME_STOP"]) / len(values) if values else None,
        mean_net_r=mean(values) if values else None,
        median_net_r=median(values) if values else None,
        profit_factor=sum(wins) / abs(sum(losses)) if losses else None,
        mean_winner_r=mean(wins) if wins else None,
        mean_loser_r=mean(losses) if losses else None,
        mfe=observed("mfe_r"),
        mae=observed("mae_r"),
        mfe_before_stop=observed("mfe_r", "STOP"),
        mae_before_target=observed("mae_r", "TARGET"),
        hypothetical_candle_markouts=markouts,
        time_to_tp1=duration("TARGET"),
        time_to_stop=duration("STOP"),
        mean_cost_pct_original_risk=mean(costs) if costs else None,
        cost_samples=len(costs),
        largest_losing_streak=largest,
        largest_loss_r=min(losses) if losses else None,
        cumulative_r_drawdown=drawdown if values else None,
        drawdown_basis="Chronological additive original R, not account equity or exposure-adjusted drawdown",
        independent_days_represented=days,
        symbols_represented=len({x["snapshot"]["signal"]["symbol"] for x, _ in complete}),
        evidence="INSUFFICIENT" if len(values) < 30 or days < 10 else "EXPLORATORY",
        limitation="Hypothetical outcomes, assumed costs; calendar days are not guaranteed independent",
    )


def counterfactual_attribution(rows, policy):
    """One canonical opportunity; overlapping reason groups never sum to benefit."""
    unique = {}
    for row in sorted(rows, key=lambda x: (x["snapshot"]["decision_ms"], x["snapshot_id"])):
        reasons = (
            row["snapshot"]["signal"].get("evidence", {}).get("v21_counterfactual", {}).get("reasons", [])
        )
        if reasons:
            unique.setdefault(row.get("candidate_identity", row["snapshot"]["signal"]["id"]), row)
    groups, combinations = defaultdict(list), defaultdict(list)
    for row in unique.values():
        reasons = sorted(set(row["snapshot"]["signal"]["evidence"]["v21_counterfactual"]["reasons"]))
        combinations[" + ".join(reasons)].append(row)
        for reason in reasons:
            groups[reason].append(row)
    by_reason, ablations = {}, {}
    for reason, containing in sorted(groups.items()):
        sole = [
            x
            for x in containing
            if len(set(x["snapshot"]["signal"]["evidence"]["v21_counterfactual"]["reasons"])) == 1
        ]
        shared = [x for x in containing if x not in sole]
        sole_stats, shared_stats = metrics(sole, policy), metrics(shared, policy)
        by_reason[reason] = dict(
            containing=metrics(containing, policy),
            sole=sole_stats,
            shared=shared_stats,
            sole_reason_avoided_losers=sole_stats["losses"],
            sole_reason_missed_winners=sole_stats["wins"],
            shared_avoided_losers=shared_stats["losses"],
            shared_missed_winners=shared_stats["wins"],
        )
        ablations[reason] = dict(
            uniquely_changed_decisions=len(sole),
            hypothetical_outcomes=sole_stats,
            still_rejected_by_other_reasons=len(shared),
        )
    return dict(
        unique=metrics(list(unique.values()), policy),
        by_reason=by_reason,
        reason_combinations={k: metrics(v, policy) for k, v in sorted(combinations.items())},
        leave_one_filter_out=ablations,
        limitation="Shared groups overlap and must not be summed. Removing a sole recorded reason changes only the recorded decision, not verified execution; unknown/unobserved gates remain unknown.",
    )


def research_selectors():
    selectors = {
        "validity_policy": lambda s: (
            s.get("evidence", {}).get("production_policies", {}).get("trade_quality", "LEGACY")
        )
    }
    for key in ("zone_relation", "time_state", "rr_state", "cost_state", "geometry_state", "acceptable"):
        selectors["entry_" + key] = lambda s, k=key: str(
            s.get("evidence", {}).get("entry_quality", {}).get(k, "UNKNOWN")
        )
    for key in (
        "reclaim_subtype",
        "reclaim_delay_bars",
        "accepted",
        "breakout_state",
        "retest_delay_bars",
        "research_state",
        "pullback_attempt_index_since_qualification",
        "compression_state",
        "expansion_state",
    ):
        selectors[key] = lambda s, k=key: str(
            s.get("evidence", {}).get("frozen_structure", {}).get(k, "UNKNOWN")
        )
    selectors["reference_efficiency_bucket"] = lambda s: (
        bucket(100 * s["evidence"]["frozen_structure"]["reference_efficiency_ratio"])
        if number(s.get("evidence", {}).get("frozen_structure", {}).get("reference_efficiency_ratio"))
        else "UNKNOWN"
    )
    selectors["reference_width_atr_bucket"] = lambda s: (
        str(int(s["evidence"]["frozen_structure"]["reference_width_atr"] // 2) * 2)
        + "–"
        + str(int(s["evidence"]["frozen_structure"]["reference_width_atr"] // 2) * 2 + 2)
        if number(s.get("evidence", {}).get("frozen_structure", {}).get("reference_width_atr"))
        else "UNKNOWN"
    )
    selectors["liquidity_subtype"] = lambda s: (
        s.get("evidence", {}).get("liquidity_stress", {}).get("subtype", "UNKNOWN")
    )
    for variant in ("joint", "spread_only", "depth_only", "either"):
        selectors["liquidity_variant_" + variant] = lambda s, k=variant: str(
            s.get("evidence", {}).get("liquidity_stress", {}).get("variants", {}).get(k, "UNKNOWN")
        )
    return selectors


def execution_stress(rows, policy):
    variants = {}
    for multiplier in (1, 1.5, 2):
        values = []
        for row in rows:
            label = next((item["payload"] for item in row["labels"] if item["policy"] == policy), {})
            s = row["snapshot"]["signal"]
            cost = s.get("risk", {}).get("cost_per_base")
            risk = abs(s["entry"] - s["stop"])
            if (
                label.get("complete")
                and number(label.get("net_r"))
                and number(cost)
                and cost >= 0
                and risk > 0
            ):
                values.append(label["net_r"] - (multiplier - 1) * cost / risk)
        variants[str(multiplier)] = dict(
            samples=len(values),
            mean_net_r=mean(values) if values else None,
            wins=sum(v > 0 for v in values),
            losses=sum(v < 0 for v in values),
        )
    return dict(
        cost_multipliers=variants,
        delayed_execution="UNKNOWN",
        basis="Net R minus (multiplier−1)×frozen fees/slippage/funding reserve/original risk; no added spread. Sensitivity, not a rerun of fills or actual account costs.",
    )


def summarize(rows, now, truncated=False):
    delivered = [x for x in rows if x.get("receipt") and x["receipt"][0] == "sent" and x["receipt"][1]]
    delivered_ids = {x["snapshot_id"] for x in delivered}
    result = dict(
        at_ms=now,
        policy="trade-quality-audit-v211",
        decisions=len(rows),
        delivered=len(delivered),
        overall={},
        cohorts={},
        research_cohorts={},
        execution_stress={},
        failure_taxonomy=TAXONOMY,
        filter_counterfactuals={},
        losing_trade_diagnostics=[],
        data_quality=dict(
            truncated=truncated,
            max_decisions=5000,
            max_snapshot_bytes=32 * 1024 * 1024,
            missing_metrics_are_null=True,
            labels_not_pooled=True,
            account_fills_verified=False,
            profit_factor_definition="sum positive net R / absolute sum negative net R",
        ),
    )
    selectors = {
        key: (lambda s, k=key: s.get(k, "UNKNOWN"))
        for key in (
            "horizon_profile",
            "family",
            "regime",
            "source",
            "direction",
            "entry_session",
            "symbol",
            "raw_tier",
        )
    }
    selectors["score_bucket"] = lambda s: bucket(s.get("quality"))
    for name in ("location", "mechanism", "trade_offer"):
        selectors[name] = lambda s, k=name: bucket(
            s.get("evidence", {}).get("v2", {}).get(k, {}).get("score")
        )
    selectors["confidence"] = lambda s: bucket(s.get("evidence", {}).get("v2", {}).get("evidence_confidence"))
    selectors["entry_quality"] = lambda s: str(
        s.get("evidence", {}).get("entry_quality", {}).get("entry_not_chased", "UNKNOWN")
    )
    selectors["liquidity"] = lambda s: (
        s.get("evidence", {}).get("liquidity_stress", {}).get("state", "UNKNOWN")
    )
    selectors["v8_state"] = lambda s: str(
        s.get("evidence", {}).get("v8_gate", {}).get("hard_adverse", "UNKNOWN")
    )
    for policy in POLICIES:
        result["overall"][policy] = dict(
            all_decisions=metrics(rows, policy), delivered=metrics(delivered, policy)
        )
        result["cohorts"][policy] = {}
        for name, key in selectors.items():
            groups = defaultdict(list)
            if name == "horizon_profile":
                for h in PROFILES:
                    if h != "LEGACY":
                        groups[h] = []
            for x in delivered:
                groups[key(x["snapshot"]["signal"])].append(x)
            result["cohorts"][policy][name] = {k: metrics(v, policy) for k, v in sorted(groups.items())}
        # Keep horizon interactions separate; a pooled score bucket is not universal evidence.
        joint = defaultdict(list)
        failures = defaultdict(list)
        for x in rows:
            s = x["snapshot"]["signal"]
            if x["snapshot_id"] not in delivered_ids:
                continue
            joint[f"{s['horizon_profile']}|{s['family']}|{s['regime']}|{bucket(s.get('quality'))}"].append(x)
            label = next(
                (label_row["payload"] for label_row in x["labels"] if label_row["policy"] == policy), {}
            )
            if label.get("complete") and number(label.get("net_r")) and label["net_r"] < 0:
                codes = failure_classes(s)
                for code in codes:
                    failures[code].append(x)
                result["losing_trade_diagnostics"].append(
                    dict(
                        signal_id=s["id"],
                        policy=policy,
                        codes=codes,
                        v8_states=s.get("evidence", {}).get("v8_gate", {}).get("feature_states", {}),
                    )
                )
        result["cohorts"][policy]["horizon_family_regime_score"] = {
            k: metrics(v, policy) for k, v in sorted(joint.items())
        }
        interactions = defaultdict(list)
        diagnostics = defaultdict(list)
        sensitivity = defaultdict(list)
        for x in delivered:
            s = x["snapshot"]["signal"]
            for component in ("location", "mechanism", "trade_offer", "confidence"):
                interactions[
                    f"{s['horizon_profile']}|{s['family']}|{s['regime']}|{component}|{selectors[component](s)}"
                ].append(x)
            for code in failure_classes(s):
                diagnostics[code].append(x)
            risk = abs(s["entry"] - s["stop"])
            cost = s.get("risk", {}).get("cost_per_base")
            for cutoff in (0.5, 0.75, 1.0):
                if risk and number(cost) and cost / risk >= cutoff:
                    sensitivity[str(cutoff)].append(x)
        result["cohorts"][policy]["component_interactions"] = {
            k: metrics(v, policy) for k, v in sorted(interactions.items())
        }
        result["cohorts"][policy]["diagnostic_present_winners_and_losers"] = {
            k: metrics(v, policy) for k, v in diagnostics.items()
        }
        result["cohorts"][policy]["cost_sensitivity_descriptive_not_optimized"] = {
            str(k): metrics(sensitivity[str(k)], policy) for k in (0.5, 0.75, 1.0)
        }
        result["cohorts"][policy]["failure_class"] = {k: metrics(failures[k], policy) for k in FAILURES}
        result["filter_counterfactuals"][policy] = counterfactual_attribution(rows, policy)
        result["research_cohorts"][policy] = {}
        for name, selector in research_selectors().items():
            groups = defaultdict(list)
            for x in rows:
                groups[str(selector(x["snapshot"]["signal"]))].append(x)
            result["research_cohorts"][policy][name] = {
                k: metrics(v, policy) for k, v in sorted(groups.items())
            }
        result["execution_stress"][policy] = execution_stress(rows, policy)
    return result


def audit(store, now, **filters):
    rows, truncated = load_rows(store.db, now, **filters)
    report = summarize(rows, now, truncated)
    # Status readers consume this compact summary, never trigger a tape replay.
    store.put(
        "trade_quality_summary",
        {k: report[k] for k in ("at_ms", "policy", "decisions", "delivered", "overall", "data_quality")},
    )
    return report
