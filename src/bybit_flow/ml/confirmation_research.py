"""Purged chronological policy comparisons. Research output never promotes a gate."""

import json
import math
from statistics import mean, pstdev


def assess(rows, selected):
    returns = [r["net_r"] for r in rows if selected(r)]
    equity = peak = drawdown = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    sorted_returns = sorted(returns)
    positives = [r for r in rows if r["net_r"] > 0]
    accepted = [r for r in rows if selected(r)]
    n = len(returns)
    error = 1.96 * pstdev(returns) / math.sqrt(n) if n > 1 else None
    return dict(
        n=n,
        mean_net_r=mean(returns) if n else None,
        descriptive_normal_interval=[mean(returns) - error, mean(returns) + error]
        if error is not None
        else None,
        interval_limitation="IID approximation; not sufficient for promotion under correlated market samples",
        max_drawdown_r=drawdown,
        tail_5pct_r=sorted_returns[int((n - 1) * 0.05)] if n else None,
        false_positive_fraction=sum(r["net_r"] <= 0 for r in accepted) / len(accepted) if accepted else None,
        false_negative_fraction=sum(not selected(r) for r in positives) / len(positives)
        if positives
        else None,
    )


def partitions(rows, minimum=500):
    rows = sorted(rows, key=lambda r: (r["decision_ms"], r["id"]))
    if len(rows) < minimum:
        return None
    a, b, c = (int(len(rows) * v) for v in (0.4, 0.6, 0.8))
    embargo = 86400000
    parts = [
        [r for r in rows[lo:hi] if r["available_ms"] < rows[hi]["decision_ms"] - embargo]
        for lo, hi in [(0, a), (a, b), (b, c)]
    ] + [rows[c:]]
    return parts if min(map(len, parts)) >= 30 else None


def run(store, asof):
    boundary = store.get("confirmation_policy_holdout_end", -1)
    rows = []
    seen = set()
    # Only small identifiers are sorted; large snapshots are fetched individually.
    query = "SELECT s.id,s.signal_id,s.decision_ms,l.available_ms,c.candidate_identity FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id WHERE s.stage='decision' AND s.schema_version='candidate-v6' AND l.policy='prints-v1' AND s.decision_ms>? AND l.available_ms<=? ORDER BY s.decision_ms,s.id"
    for ident, signal_id, decision, available, identity in store.db.execute(
        query, (boundary, asof)
    ).fetchall():
        if (identity or signal_id) in seen:
            continue
        seen.add(identity or signal_id)
        outcome = json.loads(
            store.db.execute(
                "SELECT payload FROM ml_labels WHERE snapshot_id=? AND policy='prints-v1'", (ident,)
            ).fetchone()[0]
        )
        if not outcome.get("complete") or outcome.get("net_r") is None:
            continue
        s = json.loads(
            store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
        )["signal"]
        if s.get("horizon_profile") not in {"SHORT_INTRADAY", "CORE_INTRADAY"}:
            continue
        e = s["evidence"]
        norm = e.get("session_metrics", {})
        factor = e.get("market_factor", {})
        sign = 1 if s["direction"] == "LONG" else -1
        required = ["volume_percentile", "trade_count_percentile", "delta_magnitude_percentile"]
        if any(norm.get(k) is None for k in required) or not factor.get("available"):
            continue
        flow = e.get("flow", {})
        alignment = e.get("market_alignment", {}).get("alignment")
        participation = sum(norm[k] >= 0.75 for k in required) >= 2
        residual = sign * factor.get("residual_return", 0) > max(
            0.001, abs(factor.get("expected_return_btc", 0)) * 0.5
        )
        domains = sum(
            [
                participation,
                residual,
                flow.get("delta_persistence", 0) >= 0.75,
                sign * e.get("book", {}).get("obi_10bps", 0) > 0.2,
            ]
        )
        incumbent = not s.get("gates") and s.get("risk", {}).get("accepted", False)
        replaceable = {
            "executed order flow did not confirm family trigger",
            "UNCONFIRMED_LIQUIDITY_SWEEP",
            "MARKET_CONFLICT_WITHOUT_IDIOSYNCRATIC_CONFIRMATION",
        }
        hard_valid = s.get("risk", {}).get("accepted", False) and not any(
            g not in replaceable for g in s.get("gates", [])
        )
        orderflow_support = bool(flow.get("available")) and (
            (sign * flow.get("cvd_slope", 0) > 0 and flow.get("delta_persistence", 0) >= 0.5)
            or (
                flow.get("absorption_long" if sign == 1 else "absorption_short")
                and sign * flow.get("price_change", 0) > 0
            )
        )
        rows.append(
            dict(
                id=ident,
                decision_ms=decision,
                available_ms=available,
                net_r=outcome["net_r"],
                A=incumbent,
                B=hard_valid and orderflow_support and participation,
                C=hard_valid
                and orderflow_support
                and participation
                and (alignment not in {"MARKET-CONTRARIAN", "IDIOSYNCRATIC"} or residual),
                D=hard_valid and orderflow_support and domains >= 3,
            )
        )
    report = dict(
        policy="confirmation-shadow-v1",
        samples=len(rows),
        minimum=500,
        status="INSUFFICIENT_EVIDENCE",
        production_enabled=False,
        comparison=[
            "A incumbent",
            "B normalized participation and executed-flow response",
            "C participation plus contrarian residual evidence",
            "D three-domain confirmation with executed flow",
        ],
        limitation="Fixed shadow hypotheses; unchanged coverage, macro, entry-zone and risk gates; no causal profitability claim or automatic promotion",
        decision="Incumbent retained; no holdout-backed promotion",
    )
    parts = partitions(rows)
    if parts:
        # Reserve the cohort before inspecting its held-out outcomes.
        store.put("confirmation_policy_holdout_end", max(r["decision_ms"] for r in rows))
        report.update(
            status="SHADOW_EVALUATED",
            partition_sizes=list(map(len, parts)),
            embargo_ms=86400000,
            validation={p: assess(parts[2], lambda r, p=p: r[p]) for p in "ABCD"},
            holdout={p: assess(parts[3], lambda r, p=p: r[p]) for p in "ABCD"},
        )
    if parts or not store.get("confirmation_policy_research", {}).get("holdout"):
        store.put("confirmation_policy_research", report)
    return report
