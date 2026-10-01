"""Conservative, causal V8 policy for new decisions; never active lifecycle authority."""

import math
from collections import Counter
from statistics import mean

from .production import INTRADAY, REVERSALS

POLICY = "v8-production-gating-v1"
FAMILIES = (
    "spot_perp",
    "ofi",
    "breadth",
    "anchored",
    "liquidation",
    "volatility",
    "context_ev",
    "opportunity",
)
CONTINUATIONS = {"trend_pullback", "breakout_retest", "breakout_acceptance"}
TTLS = dict(
    spot_perp=90_000,
    ofi=5000,
    breadth=1_200_000,
    anchored=960_000,
    liquidation=120_000,
    volatility=3_660_000,
    context_ev=1_800_000,
    opportunity=60_000,
)


def invalid_reason(value, source, asof):
    """Malformed/future evidence disables only its feature for this process."""
    if value.get("source") not in {None, source}:
        return "SOURCE_MISMATCH"
    if value.get("epoch_contradiction"):
        return "CONTINUITY_EPOCH_CONTRADICTION"

    def walk(row):
        if isinstance(row, dict):
            for key, item in row.items():
                if isinstance(item, (int, float)) and not math.isfinite(item):
                    return "NON_FINITE"
                if key in {"source_ms", "available_ms", "known_ms", "max_label_available_ms"}:
                    if item is not None and (not isinstance(item, (int, float)) or item > asof):
                        return "FUTURE_TIMESTAMP"
                if "coverage" in key and isinstance(item, (int, float)) and item < 0:
                    return "NEGATIVE_COVERAGE"
                reason = walk(item)
                if reason:
                    return reason
        elif isinstance(row, (list, tuple)):
            for item in row:
                reason = walk(item)
                if reason:
                    return reason
        return None

    return walk(value)


def evaluate(signal, asof_ms, settings, *, disabled=None, active=(), portfolio=None):
    evidence = signal.evidence
    disabled = disabled if disabled is not None else {}
    sign = 1 if signal.direction == "LONG" else -1
    continuation = signal.family in CONTINUATIONS
    reversal = signal.family in REVERSALS
    alt_intraday = signal.symbol not in {"BTCUSDT", "ETHUSDT"} and signal.horizon_profile in INTRADAY
    states = {}

    def set_state(name, state="UNAVAILABLE", reason="INSUFFICIENT_EVIDENCE", ready=False):
        value = evidence.get("opportunity_priority" if name == "opportunity" else name, {})
        states[name] = dict(
            state=state,
            gate_ready=ready,
            reason=reason,
            policy=POLICY,
            evidence_policy=value.get("policy"),
            source_ms=value.get("source_ms"),
            available_ms=value.get("available_ms"),
        )
        return states[name]

    for name in FAMILIES:
        value = evidence.get("opportunity_priority" if name == "opportunity" else name, {})
        set_state(name)
        bad = invalid_reason(value, signal.source, asof_ms)
        if bad:
            disabled[name] = bad
        if name in disabled:
            set_state(name, reason="DEGRADED:" + disabled[name])
            continue
        if not getattr(settings, "v8_gate_" + name, True):
            set_state(name, reason="DISABLED")
            continue
        if (
            not value
            or value.get("source") != signal.source
            or value.get("source_ms") is None
            or value.get("available_ms") is None
            or not 0 <= asof_ms - value["available_ms"] <= TTLS[name]
        ):
            continue
        # Price/book families must also be fresh at their underlying observation time.
        source_ttl = {
            "spot_perp": 90_000,
            "ofi": settings.book_stale_ms,
            "breadth": 3_660_000,
            "anchored": 960_000,
            "volatility": 3_660_000,
        }.get(name)
        if source_ttl is not None and not 0 <= asof_ms - value["source_ms"] <= source_ttl:
            continue

        state = value.get("state")
        if name == "spot_perp":
            if not value.get("price_coverage_complete") or value.get("symbol") != signal.symbol:
                continue
            set_state(name, "NEUTRAL", "PRICE_CONTEXT", True)
            sr, pr = value.get("spot_return_1m"), value.get("perp_return_1m")
            if sr is None or pr is None:
                set_state(name)
                continue
            if alt_intraday and continuation:
                if (
                    state == "LEVERAGED_PERP_ONLY_MOVE"
                    and sign * pr > 0
                    and abs(pr) >= 0.002
                    and abs(sr) <= abs(pr) * 0.25
                    and (value.get("perp_oi_change_pct") or 0) > 0
                    and sign * (value.get("basis_change_bps") or 0) > 0
                ):
                    set_state(name, "ADVERSE", "LEVERAGED_PERP_MOVE_WITHOUT_SPOT_CONFIRMATION", True)
                elif sr * pr < 0 and min(abs(sr), abs(pr)) >= 0.001:
                    set_state(name, "ADVERSE", "SPOT_PERP_DIRECTIONAL_DIVERGENCE", True)
            if states[name]["state"] == "NEUTRAL" and sign * sr > 0 and sign * pr > 0:
                set_state(name, "SUPPORT", "SPOT_PERP_PRICE_CONFIRMED", True)

        elif name == "ofi":
            if not value.get("production_coverage_ready") or value.get("ofi_coverage_seconds", 0) < 45:
                continue
            if value.get("baseline_samples", 0) < 20 or value.get("ofi_strength_percentile") is None:
                set_state(name, reason="BASELINE_IMMATURE")
                continue
            set_state(name, "NEUTRAL", "WEAK_OR_MIXED_BOOK_FLOW", True)
            if value["ofi_strength_percentile"] >= 0.70 and value.get("ofi_persistence", 0) >= 0.65:
                book_sign = sign * (value.get("ofi_normalized_l1") or 0)
                micro_sign = sign * (value.get("microprice_response_bps") or 0)
                band_sign = sign * (value.get("ofi_normalized_5bps") or 0)
                if book_sign < 0 and band_sign < 0 and micro_sign < 0:
                    set_state(name, "ADVERSE", "STRONG_ADVERSE_BOOK_ORDER_FLOW", True)
                elif book_sign > 0 and band_sign > 0 and micro_sign > 0:
                    set_state(name, "SUPPORT", "STRONG_SUPPORTIVE_BOOK_ORDER_FLOW", True)

        elif name == "breadth":
            if not value.get("production_ready") or value.get("denominator", 0) < 20:
                continue
            set_state(name, "NEUTRAL", "BREADTH_CONTEXT", True)
            factor = evidence.get("market_factor", {})
            alignment = evidence.get("market_alignment", {})
            independent = alignment.get(
                "alignment", alignment.get("market_alignment_state")
            ) == "IDIOSYNCRATIC_DIVERGENCE" and alignment.get("contrarian_override_passed")
            stable = (
                factor.get("available")
                and (factor.get("beta_to_btc") or 0) > 0
                and (factor.get("correlation_to_btc") or 0) >= 0.5
                and factor.get("beta_stability_btc") is not None
                and factor["beta_stability_btc"] <= 0.5
                and 0 <= asof_ms - factor.get("available_ms", asof_ms + 1) <= 3_660_000
            )
            if alt_intraday and continuation and stable and not independent:
                opposing = "BROAD_RISK_OFF" if sign == 1 else "BROAD_RISK_ON"
                if state == opposing:
                    set_state(name, "ADVERSE", "BROAD_MARKET_OPPOSES_ALT_" + signal.direction, True)
                elif state in {"BTC_LED_RALLY", "BTC_LED_SELLOFF"}:
                    if value.get("residual_samples_btc", 0) < 15:
                        set_state(name, reason="RESIDUAL_UNIVERSE_TOO_SMALL")
                    elif (
                        sign == 1
                        and state == "BTC_LED_RALLY"
                        and (value.get("negative_btc_residual_fraction") or 0) >= 0.65
                    ):
                        set_state(name, "ADVERSE", "BTC_RALLY_WITH_ALT_WEAKNESS", True)
                    elif (
                        sign == -1
                        and state == "BTC_LED_SELLOFF"
                        and (value.get("positive_btc_residual_fraction") or 0) >= 0.65
                    ):
                        set_state(name, "ADVERSE", "BTC_SELLOFF_WITH_ALT_STRENGTH", True)
                elif state == ("BROAD_RISK_ON" if sign == 1 else "BROAD_RISK_OFF"):
                    set_state(name, "SUPPORT", "BROAD_MARKET_SUPPORTS_DIRECTION", True)

        elif name == "anchored":
            anchor = value.get("anchors", {}).get("setup_trigger", {})
            if not value.get("production_ready") or anchor.get("bars", 0) < 5:
                continue
            accepted = value.get("acceptance", {}).get("setup_avwap", {}).get("state")
            wrong = "ACCEPTED_BELOW" if sign == 1 else "ACCEPTED_ABOVE"
            right = "ACCEPTED_ABOVE" if sign == 1 else "ACCEPTED_BELOW"
            profile_ready = 0 <= asof_ms - (value.get("profile_ms") or 0) <= 60_000
            area = value.get("val_acceptance_state") if sign == 1 else value.get("vah_acceptance_state")
            failed = anchor.get("failed_reclaim_avwap") if sign == 1 else anchor.get("reclaim_avwap")
            reclaimed = anchor.get("reclaim_avwap") if sign == 1 else anchor.get("failed_reclaim_avwap")
            structural = evidence.get("execution_features", {})
            adverse_structure = structural.get("bos") == ("down" if sign == 1 else "up") or structural.get(
                "choch"
            ) == ("down" if sign == 1 else "up")
            set_state(name, "NEUTRAL", "ANCHORED_CONTEXT", True)
            if (
                signal.family in {"breakout_retest", "breakout_acceptance"}
                and failed
                and (accepted == wrong or profile_ready and area == wrong)
            ):
                set_state(name, "ADVERSE", "FAILED_ACCEPTANCE_AFTER_BREAKOUT", True)
            elif signal.family == "trend_pullback" and accepted == wrong and adverse_structure:
                set_state(name, "ADVERSE", "ADVERSE_PULLBACK_ACCEPTANCE", True)
            elif reversal:
                if (
                    reclaimed
                    or profile_ready
                    and area == ("REJECTED_BELOW" if sign == 1 else "REJECTED_ABOVE")
                ):
                    set_state(name, "SUPPORT", "REVERSAL_RECLAIM_CONFIRMED", True)
                elif accepted == wrong and profile_ready and area == wrong and adverse_structure:
                    set_state(name, "ADVERSE", "REVERSAL_RECLAIM_FAILED", True)
            elif accepted == right:
                set_state(name, "SUPPORT", "ANCHORED_ACCEPTANCE_CONFIRMED", True)

        elif name == "liquidation":
            if (
                not value.get("production_ready")
                or not value.get("event_window_oi_ready")
                or not value.get("trusted_flow")
                or value.get("baseline_samples", 0) < 20
                or value.get("liquidation_price_response_bps") is None
            ):
                continue
            set_state(name, "NEUTRAL", "LIQUIDATION_CONTEXT", True)
            cascade = "LONG_LIQUIDATION_CASCADE" if sign == 1 else "SHORT_LIQUIDATION_CASCADE"
            opposing_exhaustion = (
                "SHORT_LIQUIDATION_EXHAUSTION" if sign == 1 else "LONG_LIQUIDATION_EXHAUSTION"
            )
            supportive_exhaustion = (
                "LONG_LIQUIDATION_EXHAUSTION" if sign == 1 else "SHORT_LIQUIDATION_EXHAUSTION"
            )
            bearish_deleverage = (
                state == "DELEVERAGING_CONTINUATION" and value["liquidation_price_response_bps"] < 0
            )
            if state == cascade or sign == 1 and bearish_deleverage:
                set_state(name, "ADVERSE", "LIQUIDATION_CONTINUATION_OPPOSES_SETUP", True)
            elif continuation and state == opposing_exhaustion:
                set_state(name, "ADVERSE", "LIQUIDATION_EXHAUSTION_OPPOSES_CONTINUATION", True)
            elif reversal and state == supportive_exhaustion:
                set_state(name, "SUPPORT", "LIQUIDATION_EXHAUSTION_SUPPORTS_REVERSAL", True)

        elif name == "volatility":
            if state in {None, "INSUFFICIENT"}:
                continue
            set_state(name, "NEUTRAL", "VOLATILITY_CONTEXT", True)
            if state == "JUMP_SHOCK":
                execution = evidence.get("execution", {})
                flow = evidence.get("flow", {})
                structure = evidence.get("execution_features", {})
                stabilized = structure.get("bos") == ("up" if sign == 1 else "down") or structure.get(
                    "choch"
                ) == ("up" if sign == 1 else "down")
                response = evidence.get("v8_structure_response", {})
                stabilized = stabilized or response.get("supportive", False)
                after_shock = value["source_ms"] < response.get("source_ms", 0) <= asof_ms
                safe_reversal = (
                    reversal
                    and stabilized
                    and after_shock
                    and response.get("available_ms", asof_ms + 1) <= asof_ms
                    and execution.get("stop_noise_ratio", 0) >= 0.5
                    and evidence.get("flow_confirmed")
                    and flow.get("available")
                    and evidence.get("entry_not_chased") is True
                )
                if not safe_reversal:
                    set_state(name, "ADVERSE", "JUMP_SHOCK_EXECUTION_UNSTABLE", True)
            elif state == "POST_SHOCK_NORMALIZATION":
                set_state(name, "SUPPORT", "POST_SHOCK_NORMALIZATION", True)

        elif name == "context_ev":
            if (
                value.get("status") != "AVAILABLE"
                or value.get("confidence") != "MATURE"
                or value.get("effective_samples", 0) < 200
                or not value.get("source_specific")
                or value.get("max_label_available_ms", asof_ms) >= asof_ms
                or value.get("cache_age_ms", 1_800_001) > 1_800_000
                or value.get("lower_confidence_bound_r") is None
            ):
                continue
            lower = value["lower_confidence_bound_r"]
            set_state(
                name,
                "ADVERSE" if lower <= -0.10 else "SUPPORT" if lower > 0 else "NEUTRAL",
                "MATURE_CONTEXT_NEGATIVE_EXPECTANCY" if lower <= -0.10 else "MATURE_CONTEXT_EXPECTANCY",
                True,
            )

        elif name == "opportunity":
            set_state(name, "NEUTRAL", "INFORMATIONAL_PRIORITY", True)
            # Manual correlated exposure policy, never guessed from alerts or paper positions.
            fresh_portfolio = (
                settings.equity and portfolio and 0 <= asof_ms - portfolio.get("at_ms", 0) <= 86_400_000
            )
            exposure = (
                sum(p.get("risk_fraction", 0) for p in portfolio.get("positions", []))
                if fresh_portfolio
                else 0
            )
            capacity_used = fresh_portfolio and exposure >= 0.9 * settings.correlated_risk_limit
            if capacity_used:
                for other in active:
                    if (
                        other["id"] in value.get("related_ids", [])
                        and other.get("source") == signal.source
                        and other.get("state") in {"CONFIRMED", "ALERTED"}
                        and other.get("direction") == signal.direction
                    ):
                        priority = other.get("evidence", {}).get("opportunity_priority", {})
                        if (
                            priority.get("cluster_id") == value.get("cluster_id")
                            and priority.get("priority_score_0_100", 0)
                            - value.get("priority_score_0_100", 100)
                            >= 10
                        ):
                            set_state(name, "ADVERSE", "REDUNDANT_OPPORTUNITY", True)
                            break
    blocking = [
        r["reason"]
        for name, r in states.items()
        if name != "opportunity" and r["gate_ready"] and r["state"] == "ADVERSE"
    ]
    supporting = [r["reason"] for r in states.values() if r["gate_ready"] and r["state"] == "SUPPORT"]
    return dict(
        policy=POLICY,
        source=signal.source,
        source_ms=asof_ms,
        available_ms=asof_ms,
        overall_state="ADVERSE"
        if blocking
        else "SUPPORT"
        if supporting
        else "NEUTRAL"
        if any(r["gate_ready"] for r in states.values())
        else "UNAVAILABLE",
        gate_ready=any(r["gate_ready"] for r in states.values()),
        blocking_reasons=blocking,
        supporting_reasons=supporting,
        feature_states=states,
        production_enabled=settings.v8_production_gating,
        hard_adverse=bool(blocking),
        blocked=bool(settings.v8_production_gating and blocking),
        delivery_suppressed=bool(
            settings.v8_production_gating and states["opportunity"]["state"] == "ADVERSE"
        ),
        v7_valid=not signal.gates,
        degraded=dict(disabled),
    )


def apply_new_candidate(scanner, signal, asof_ms, *, portfolio=None):
    """Only the first new, V7-valid pending confirmation can freeze a production gate."""
    if (
        signal.evidence.get("confirmation_policy") != POLICY
        or signal.state != "PENDING CONFIRMATION"
        or signal.gates
        or "v8_gate" in signal.evidence
    ):
        return None
    gate = evaluate(
        signal,
        asof_ms,
        scanner.settings,
        disabled=scanner.v8_disabled,
        active=scanner.store.active_signals(),
        portfolio=portfolio,
    )
    signal.evidence["v8_gate"] = gate
    record(scanner.store, signal, gate, asof_ms)
    if gate["blocked"]:
        signal.evidence["v8_shadow"] = dict(
            policy=POLICY,
            v7_valid=True,
            original_state="CONFIRMED",
            reasons=gate["blocking_reasons"],
            outcome_policy="prints-v1",
            research_only=True,
        )
        signal.gates.extend(gate["blocking_reasons"])
    if gate["delivery_suppressed"]:
        signal.evidence["delivery_policy"] = "REDUNDANT_OPPORTUNITY"
    return gate


def record(store, signal, gate, asof_ms):
    """Small immutable association; complete evidence lives in existing ML snapshots."""
    import json

    from .funnel import emit

    store.db.execute("""CREATE TABLE IF NOT EXISTS v8_gate_decisions(
        signal_id TEXT PRIMARY KEY, decision_ms INTEGER, source TEXT, blocked INTEGER, reasons TEXT)""")
    with store.db:
        inserted = store.db.execute(
            "INSERT OR IGNORE INTO v8_gate_decisions VALUES(?,?,?,?,?)",
            (signal.id, asof_ms, signal.source, int(gate["blocked"]), json.dumps(gate["blocking_reasons"])),
        ).rowcount
    if not inserted:
        return
    emit(store, "v8_evaluated", asof_ms, signal=signal, key="v8-evaluated:" + signal.id)
    emit(
        store,
        "v8_blocked" if gate["blocked"] else "v8_passed",
        asof_ms,
        signal=signal,
        key="v8-result:" + signal.id,
    )
    for reason in gate["blocking_reasons"]:
        emit(
            store,
            "v8_reason" if gate["blocked"] else "v8_shadow_reason",
            asof_ms,
            signal=signal,
            reason=reason,
            key="v8-reason:" + signal.id + reason,
        )
    for name, row in gate["feature_states"].items():
        if not row["gate_ready"]:
            emit(
                store,
                "v8_unavailable",
                asof_ms,
                signal=signal,
                reason=name,
                key="v8-unavailable:" + signal.id + name,
            )
    store.put("v8_production_gate", status(store, asof_ms) | dict(degraded=gate["degraded"]))


def status(store, asof_ms):
    rows = store.db.execute(
        "SELECT metric,dimension,value,sum(n) FROM funnel_minutes "
        "WHERE minute_ms>=? AND metric LIKE 'v8_%' GROUP BY metric,dimension,value",
        (asof_ms - 3_600_000,),
    ).fetchall()
    totals = {(m, d, v): n for m, d, v, n in rows}
    return dict(
        policy=POLICY,
        evaluated_1h=totals.get(("v8_evaluated", "all", "all"), 0),
        blocked_1h=totals.get(("v8_blocked", "all", "all"), 0),
        passed_1h=totals.get(("v8_passed", "all", "all"), 0),
        by_reason={v: n for m, d, v, n in rows if m == "v8_reason" and d == "reason"},
        unavailable={v: n for m, d, v, n in rows if m == "v8_unavailable" and d == "reason"},
    )


def effectiveness(store, asof_ms):
    """Matched source/horizon/family/direction/regime, causal complete prints-v1 only."""
    import json

    from .ml import SCHEMA_VERSION
    from .ml.store import FeatureStore

    exists = store.db.execute("SELECT 1 FROM sqlite_master WHERE name='v8_gate_decisions'").fetchone()
    gates = (
        {
            r[0]: (r[1], json.loads(r[2]))
            for r in store.db.execute("SELECT signal_id,blocked,reasons FROM v8_gate_decisions")
        }
        if exists
        else {}
    )
    rows = FeatureStore(store).dataset(asof_ms - 1, schema_version=SCHEMA_VERSION, limit=10_000)
    blocked = [r for r in rows if gates.get(r["signal_id"], (False, []))[0]]

    def comparable(row):
        s = row["signal"]
        return tuple(s.get(k) for k in ("source", "horizon_profile", "family", "direction", "regime"))

    groups = {comparable(r) for r in blocked}
    passed = [
        r
        for r in rows
        if r["signal_id"] in gates and not gates[r["signal_id"]][0] and comparable(r) in groups
    ]

    def summary(sample):
        n = len(sample)
        return dict(
            complete_outcomes=n,
            mean_net_r=mean(r["label"]["net_r"] for r in sample) if n else None,
            stop_rate=sum("stop" in str(r["label"].get("exit_reason", "")).lower() for r in sample) / n
            if n
            else None,
            target_rate=sum("target" in str(r["label"].get("exit_reason", "")).lower() for r in sample) / n
            if n
            else None,
        )

    b = summary(blocked)
    reasons = Counter(reason for flag, reasons in gates.values() if flag for reason in reasons)
    return dict(
        policy=POLICY,
        status="DESCRIPTIVE" if len(blocked) >= 30 and len(passed) >= 30 else "INSUFFICIENT_EVIDENCE",
        blocked_samples=sum(bool(flag) for flag, _ in gates.values()),
        complete_blocked_outcomes=len(blocked),
        avoided_stop_rate=b["stop_rate"],
        missed_target_rate=b["target_rate"],
        blocked_mean_net_r=b["mean_net_r"],
        comparable_passed_samples=len(passed),
        comparable_passed_mean_net_r=summary(passed)["mean_net_r"],
        by_reason={
            reason: dict(
                blocked_samples=count, **summary([r for r in blocked if reason in gates[r["signal_id"]][1]])
            )
            for reason, count in reasons.items()
        },
        research_only=True,
        automatic_tuning=False,
    )
