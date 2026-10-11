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
    if q.get("entry_not_chased") is False:
        flags.append("CHASED_ENTRY")
    if e.get("attempt_memory", {}).get("prior_failed_attempts", 0):
        flags.append("REPEATED_FAILED_ATTEMPT")
    if not s.get("coverage", {}).get("trade_window_complete"):
        flags.append("DATA_QUALITY_FAILURE")
    return flags or ["UNKNOWN"]


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
    for ident, raw, current, status, message in db.execute(
        "SELECT m.id,m.payload,s.payload,o.status,o.message_id FROM ml_snapshots m "
        "LEFT JOIN signals s ON s.id=m.signal_id LEFT JOIN outbox o "
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
    for x, label_row in sorted(complete, key=lambda p: (p[1].get("exit_ms", 0), p[0]["snapshot_id"])):
        streak = streak + 1 if label_row["net_r"] < 0 else 0
        largest = max(largest, streak)

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
        time_to_tp1=duration("TARGET"),
        time_to_stop=duration("STOP"),
        mean_cost_pct_original_risk=mean(costs) if costs else None,
        cost_samples=len(costs),
        largest_losing_streak=largest,
        independent_days_represented=days,
        symbols_represented=len({x["snapshot"]["signal"]["symbol"] for x, _ in complete}),
        evidence="INSUFFICIENT" if len(values) < 30 or days < 10 else "EXPLORATORY",
        limitation="Hypothetical outcomes, assumed costs; calendar days are not guaranteed independent",
    )


def summarize(rows, now, truncated=False):
    delivered = [x for x in rows if x.get("receipt") and x["receipt"][0] == "sent" and x["receipt"][1]]
    delivered_ids = {x["snapshot_id"] for x in delivered}
    result = dict(
        at_ms=now,
        policy="trade-quality-audit-v1",
        decisions=len(rows),
        delivered=len(delivered),
        overall={},
        cohorts={},
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
        filters = defaultdict(list)
        for x in rows:
            s = x["snapshot"]["signal"]
            for reason in s.get("evidence", {}).get("v21_counterfactual", {}).get("reasons", []):
                filters[reason].append(x)
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
        result["filter_counterfactuals"][policy] = {
            k: dict(
                metrics(v, policy),
                avoided_losers=metrics(v, policy)["losses"],
                missed_winners=metrics(v, policy)["wins"],
            )
            for k, v in filters.items()
        }
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
