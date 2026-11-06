"""Bounded reversal timing inside the existing confirmation engine.

Observed pressure/failed impact precedes a separate reclaim/flow-turn window.
No new feed, recorder, model, or lifecycle authority is introduced.
"""

from .evidence import flow_response
from .flow_quality import assess as quality
from .orderflow import footprint
from .setup_common import causal, envelope, number, positive

POLICY = "exhaustion-reclaim-v1"
CONTEXT_MS = 60_000
TRIGGER_MS = 15_000
TTL_MS = 15_000
FAMILIES = {"liquidity_sweep", "range_rejection"}


def enabled(signal):
    return (
        signal.family in FAMILIES
        and signal.evidence.get("production_policies", {}).get("reversal_timing") == POLICY
    )


def valid(signal, row, now):
    return bool(
        enabled(signal)
        and row.get("policy") == POLICY
        and row.get("state") == "CONFIRMED"
        and row.get("coverage_complete")
        and causal(row, signal, now, TTL_MS)
        and number(row.get("window_end_ms"))
        and 0 <= now - row["window_end_ms"] <= TTL_MS
    )


def selected_flow(signal, now):
    """Scoring may use the confirmed trigger; raw context remains unchanged."""
    row = signal.evidence.get("reversal_sequence", {})
    if signal.evidence.get("flow_confirmation_mode") == "LOCAL_STAGED_REVERSAL" and valid(signal, row, now):
        return row["trigger_flow"]
    return None


def delivery_ready(signal, now, book=None, tape=None):
    if signal.evidence.get("flow_confirmation_mode") != "LOCAL_STAGED_REVERSAL":
        return True
    row = signal.coverage.get("delivery_reversal_sequence", signal.evidence.get("reversal_sequence", {}))
    return (
        valid(signal, row, now)
        and (book is None or tape is not None)
        and (tape is None or 0 < tape.coverage_start <= row.get("window_start_ms", 0))
        and (
            book is None
            or (
                book.valid
                and row.get("epoch") == book.event_ofi_epoch
                and 0 < book.event_ofi_start_ms <= row.get("window_start_ms", 0)
            )
        )
    )


def _compact(flow):
    keys = (
        "available",
        "delta_pct",
        "delta_notional",
        "cvd_slope",
        "cvd_acceleration",
        "delta_persistence",
        "impact_persistence",
        "price_change",
        "vwap",
        "buy_notional",
        "sell_notional",
        "initiative_long",
        "initiative_short",
        "absorption_long",
        "absorption_short",
        "defended_notional",
        "stacked_buy",
        "stacked_sell",
        "same_side_buy_run",
        "same_side_sell_run",
    )
    qkeys = (
        "available_ms",
        "window_start_ms",
        "window_end_ms",
        "flow_quality_state",
        "flow_trust_score",
        "gross_notional",
        "net_signed_notional",
        "price_displacement_bps",
        "impact_persistence",
    )
    return {k: flow[k] for k in keys if k in flow} | {"quality": {k: flow["quality"].get(k) for k in qkeys}}


def observe(
    signal,
    trades,
    tick,
    atr,
    book,
    book_features,
    event_ofi,
    now,
    *,
    coverage_start,
    book_start,
    liquidation=None,
):
    """Pure worker over one retained 75s slice and immutable book observations.

    The first 45s pressure, last 15s decay and final 15s trigger do not overlap.
    Missing/reset history cannot inherit a previous process's confirmation.
    """
    from .production import flow_support

    end = now // 1000 * 1000
    turn = end - TRIGGER_MS
    start = turn - CONTEXT_MS
    split = turn - TRIGGER_MS
    result = envelope(
        signal,
        POLICY,
        now,
        state="UNAVAILABLE",
        coverage_complete=False,
        window_start_ms=start,
        context_end_ms=turn,
        window_end_ms=end,
        baseline_comparison="Existing family confirmation is evaluated independently",
    )
    if not enabled(signal):
        return result | dict(reason="POLICY_NOT_ASSIGNED")
    structural = signal.evidence.get("structural_trigger", {})
    frozen = signal.evidence.get("frozen_structure", {})
    level = structural.get("level")
    known = structural.get("available_ms")
    if not (
        structural.get("valid")
        and positive(level)
        and number(known)
        and known <= start
        and causal(frozen, signal, now)
        and frozen["available_ms"] <= start
        and frozen.get("frozen_reference") == level
        and frozen.get("valid", True)
        and not frozen.get("reasons")
        and positive(atr)
        and 0 < coverage_start <= start
        and 0 < book_start <= start
        and event_ofi.get("source") == signal.source
        and event_ofi.get("symbol") == signal.symbol
        and causal(event_ofi, signal, now, 5000)
    ):
        return result | dict(reason="CONTEXT_OR_CONTINUITY_UNAVAILABLE")
    if any(t.exchange != signal.source or t.symbol != signal.symbol for t in trades):
        return result | dict(reason="SOURCE_MISMATCH")
    rows = sorted(
        (t for t in trades if start <= t.event_ms < end and t.receipt_ms <= now),
        key=lambda t: (t.event_ms, t.trade_id),
    )
    context = [t for t in rows if t.event_ms < turn]
    early = [t for t in context if t.event_ms < split]
    late = [t for t in context if t.event_ms >= split]
    trigger = [t for t in rows if t.event_ms >= turn]
    # Observed trades must reach both ends of each phase. Quiet gaps are unknown.
    if any(
        not r or r[0].event_ms > a + 5000 or r[-1].event_ms < b - 5000
        for r, a, b in ((early, start, split), (late, split, turn), (trigger, turn, end))
    ):
        return result | dict(reason="PHASE_TRADE_COVERAGE_UNAVAILABLE")
    ofi = event_ofi.get("windows", {}).get("15", {})
    touch = ofi.get("touch", {})
    if not (
        ofi.get("coverage_complete")
        and ofi.get("start_ms") == turn
        and ofi.get("end_ms") == end
        and ofi.get("source") == signal.source
        and ofi.get("symbol") == signal.symbol
    ):
        return result | dict(reason="TRIGGER_OFI_UNAVAILABLE")
    sign = 1 if signal.direction == "LONG" else -1
    opposing = "sell_notional" if sign > 0 else "buy_notional"
    side = "long" if sign > 0 else "short"
    # A defense observed during the later trigger cannot retroactively establish
    # absorption in the earlier pressure phase.
    from .orderflow import Book

    context_book = Book()
    context_book.valid = book.valid
    context_book.changes.extend(x for x in book.changes if x[0] <= turn)
    eflow = flow_response(early, book_features)
    lflow = flow_response(late, book_features)
    gross = eflow["aggressive_buy_notional"] + eflow["aggressive_sell_notional"]
    pressure = (
        gross > 0
        and 100 * sign * eflow["signed_aggressor_notional"] / gross <= -10
        and eflow["delta_persistence"] >= 0.6
    )
    outside = any(sign * (float(t.price) - level) < 0 for t in early)
    rate_key = "aggressive_sell_notional" if sign > 0 else "aggressive_buy_notional"
    early_rate = eflow[rate_key] / 45
    late_rate = lflow[rate_key] / 15
    early_extreme = min(sign * float(t.price) for t in early)
    late_extreme = min(sign * float(t.price) for t in late)
    failed_extension = late_extreme >= early_extreme and sign * lflow["price_change"] >= 0
    exhaustion = bool(early_rate > 0 and late_rate < early_rate and failed_extension)
    hold = [t for t in trigger if t.event_ms >= end - 5000]
    reclaimed = bool(
        hold
        and hold[0].event_ms <= end - 4000
        and all(sign * (float(t.price) - level) > 0 for t in hold)
        and sign * (float(trigger[-1].price) - float(trigger[0].price)) > 0
    )
    quote_turn = bool(
        number(touch.get("directional"))
        and touch["directional"] > 0
        and number(touch.get("persistence"))
        and touch["persistence"] >= 0.6
    )
    liquidation = liquidation or {}
    cascade = bool(
        causal(liquidation, signal, now, 120000)
        and liquidation.get("state")
        == ("LONG_LIQUIDATION_CASCADE" if sign > 0 else "SHORT_LIQUIDATION_CASCADE")
    )
    # Avoid two footprint/profile computations for an ordinary continuation or
    # a missing reclaim. Only a causally plausible reversal reaches that work.
    waiting = (
        "CONTINUING_CASCADE"
        if cascade
        else "NO_PRESSURE_AT_LEVEL"
        if not (pressure and outside)
        else "WAITING_FAILED_EXTENSION"
        if not failed_extension
        else "WAITING_RECLAIM_HOLD"
        if not reclaimed
        else "WAITING_FLOW_TURN"
        if not quote_turn
        else None
    )
    result.update(
        coverage_complete=True,
        level=level,
        level_known_ms=known,
        pressure=pressure,
        failed_extension=failed_extension,
        exhaustion=exhaustion,
        reclaimed_and_held=reclaimed,
        pressure_rate=early_rate,
        decay_rate=late_rate,
        epoch=event_ofi.get("epoch"),
    )
    if waiting:
        return result | dict(state=waiting)
    cflow = footprint(context, tick, atr, context_book)
    cflow.update(flow_response(context, book_features))
    cflow["quality"] = quality(context, tick, book_features, cflow, now, start, turn)
    tflow = footprint(trigger, tick, atr, book)
    tflow.update(flow_response(trigger, book_features, cflow))
    tflow["quality"] = quality(trigger, tick, book_features, tflow, now, turn, end)
    absorption = bool(
        cflow.get("absorption_" + side)
        and cflow.get("defended_notional", {}).get(signal.direction, 0) > 0
        and cflow.get(opposing, 0) > 0
    )
    support = flow_support(tflow, sign, reclaimed)
    local_turn = bool(support["raw_fallback"] and support["state"] == "FLOW_SUPPORTIVE" and quote_turn)
    trusted = all((f["quality"].get("flow_trust_score") or 0) >= 0.6 for f in (cflow, tflow))
    state = (
        "CONTINUING_CASCADE"
        if cascade
        else "UNTRUSTED"
        if not trusted
        else "NO_PRESSURE_AT_LEVEL"
        if not (pressure and outside)
        else "WAITING_FAILED_EXTENSION"
        if not (failed_extension and (absorption or exhaustion))
        else "WAITING_RECLAIM_HOLD"
        if not reclaimed
        else "WAITING_FLOW_TURN"
        if not local_turn
        else "CONFIRMED"
    )
    return result | dict(
        state=state,
        coverage_complete=True,
        level=level,
        level_known_ms=known,
        pressure=pressure,
        failed_extension=failed_extension,
        absorption=absorption,
        exhaustion=exhaustion,
        reclaimed_and_held=reclaimed,
        local_flow_turn=local_turn,
        pressure_rate=early_rate,
        decay_rate=late_rate,
        trigger_support=support,
        trigger_ofi=dict(directional=touch.get("directional"), persistence=touch.get("persistence")),
        context_flow=_compact(cflow),
        trigger_flow=_compact(tflow),
        epoch=event_ofi.get("epoch"),
        limitation="Observed exhaustion/defense followed by a turn; not participant intent or validated alpha",
    )


def augment(signal, baseline, now, *, factor_ok=True, hold_ok=False, profile_ok=False):
    """An alternative mechanism, never an override of risk, stress or macro gates."""
    if not enabled(signal):
        return baseline
    row = signal.evidence.get("reversal_sequence", {})
    from .production import requires_reclaim_hold

    passed = bool(
        valid(signal, row, now)
        and factor_ok
        and (not requires_reclaim_hold(signal) or hold_ok and profile_ok)
    )
    result = baseline | dict(
        sequence_policy=POLICY,
        baseline_passed=baseline["passed"],
        sequence_passed=passed,
        sequence_state=row.get("state", "UNAVAILABLE"),
    )
    if passed and not baseline["passed"]:
        result.update(
            passed=True,
            reason_codes=[],
            structure_support=True,
            reclaim_hold_support=True,
            flow_support="FLOW_SUPPORTIVE",
            participation_support=True,
            flow_confirmation_mode="LOCAL_STAGED_REVERSAL",
        )
    return result
