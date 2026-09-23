"""Lightweight original-venue executed-path monitoring of immutable plans."""

import asyncio

from .models import Signal
from .storage import now_ms

TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


def source_feed(scanner, source):
    if source == scanner.exchange:
        return scanner.api, scanner.streams
    manager = getattr(scanner, "cross_venue", None)
    return manager.peers.get(source, (None, None)) if manager else (None, None)


def advance_trades(signal, tape, now):
    cursor = signal.coverage.get("monitor_cursor_event_ms", signal.created_ms)
    last_id = signal.coverage.get("last_checked_trade_id")
    path = []
    for trade in reversed(tape.trades):
        if trade.trade_id == last_id or trade.event_ms < cursor:
            break
        path.append(trade)
    path.reverse()
    deadline = (
        (signal.holding_deadline_ms or signal.expires_ms)
        if signal.state == "ALERTED"
        else (signal.trigger_expires_ms or signal.expires_ms)
    )
    for trade in path:
        if trade.receipt_ms > now or trade.event_ms > now or trade.event_ms > deadline:
            continue
        price = float(trade.price)
        signal.coverage.update(
            monitor_cursor_event_ms=trade.event_ms,
            monitor_cursor_receipt_ms=trade.receipt_ms,
            last_checked_trade_id=trade.trade_id,
        )
        signal.evidence["latest_observed_price"] = price
        if signal.zone[0] <= price <= signal.zone[1] and trade.event_ms <= (
            signal.trigger_expires_ms or signal.expires_ms
        ):
            signal.evidence.setdefault("observed_entry_ms", trade.event_ms)
        stop = price <= signal.stop if signal.direction == "LONG" else price >= signal.stop
        entered = signal.evidence.get("observed_entry_ms") is not None
        tp1 = entered and (price >= signal.tp1 if signal.direction == "LONG" else price <= signal.tp1)
        tp2 = entered and (price >= signal.tp2 if signal.direction == "LONG" else price <= signal.tp2)
        for name, touched in (("tp1", tp1), ("tp2", tp2)):
            if touched:
                signal.evidence.setdefault(name + "_touch_ms", trade.event_ms)
        if stop or (tp1 and signal.state == "ALERTED" and signal.horizon_profile != "LEGACY"):
            signal.state = "INVALIDATED" if stop else "RESOLVED"
            reason = "PLANNED_STOP_CROSSED" if stop else "PLANNED_TARGET_CROSSED"
            signal.coverage["terminal_reason"] = signal.invalidation = reason
            signal.evidence["primary_outcome"] = "STOP" if stop else "TARGET"
            signal.evidence["terminal_event"] = dict(
                effective_ms=trade.event_ms,
                detected_ms=now,
                event_price=price,
                reference_price=signal.stop if stop else signal.tp1,
                stop_price=signal.stop,
                source=signal.source,
                method="LIVE_EXECUTED_TRADE",
                trigger_basis="LAST_EXECUTED_TRADE",
            )
            break
    signal.coverage["last_monitor_ms"] = now


async def bootstrap(scanner):
    """Restore original-venue subscriptions before broad discovery and reconcile gaps."""
    scanner.store.put("active_lifecycle", dict(status="BOOTSTRAPPING", at_ms=now_ms()))
    active = scanner.store.active_signals()
    native = [s for s in active if s["source"] != "tradingview"]
    if native and not scanner.source_ready:
        # Prefer the original venue; let its stream reconnect before considering rotation.
        from .exchanges import VenueAPI
        from .native_streams import NativeStreams
        from .streams import Streams

        source = next((s["source"] for s in native if s["state"] == "ALERTED"), native[0]["source"])
        await scanner.api.close()
        scanner.api = VenueAPI(source, scanner.settings, scanner.recorder)
        scanner.streams = (
            Streams(scanner.settings, scanner.store, scanner.recorder)
            if source == "bybit"
            else NativeStreams(scanner.settings, scanner.store, scanner.recorder, scanner.api)
        )
        scanner.source_ready = True
    scanner.streams.required_symbols = set(scanner.pending_symbols())
    scanner.recorder.critical_symbols = {s["symbol"] for s in native} | {"BTCUSDT", "ETHUSDT"}
    await scanner.streams.select(scanner.pending_symbols())
    if getattr(scanner, "cross_venue", None):
        await scanner.cross_venue.restore_required()
    for payload in native:
        signal = Signal.model_validate(payload)
        signal.coverage.update(monitoring="paused", monitor_status="RECONCILIATION_PENDING")
        scanner.store.signal(signal, "Startup reconciliation required")
    scanner.lifecycle_bootstrapped.set()


async def tick(scanner, now):
    active = [
        Signal.model_validate(p) for p in scanner.store.active_signals() if p["source"] != "tradingview"
    ]
    required = {s.symbol for s in active if s.source == scanner.exchange}
    scanner.store.put(
        "required_candidate_symbols:" + scanner.exchange,
        sorted({s.symbol for s in active if s.source == scanner.exchange and s.state != "ALERTED"}),
    )
    scanner.store.put(
        "active_lifecycle_symbols:" + scanner.exchange,
        sorted({s.symbol for s in active if s.source == scanner.exchange and s.state == "ALERTED"}),
    )
    scanner.streams.required_symbols = required
    scanner.recorder.critical_symbols = {s.symbol for s in active} | {"BTCUSDT", "ETHUSDT"}
    await scanner.streams.select(list(required) + list(scanner.streams.selected))
    if getattr(scanner, "cross_venue", None):
        await scanner.cross_venue.restore_required()
    ready = pending = degraded = 0
    for signal in active:
        try:
            _, streams = source_feed(scanner, signal.source)
            tape = streams.tapes.get(signal.symbol) if streams else None
            cursor = signal.coverage.get("monitor_cursor_event_ms", signal.created_ms)
            gap = not tape or tape.coverage_start is None or tape.coverage_start > cursor
            fresh = bool(
                tape
                and 0 <= now - tape.last_receipt <= scanner.settings.trade_stale_ms
                and -1000 <= now - tape.last_event <= scanner.settings.trade_stale_ms
            )
            if gap:
                scanner.reconcile_pending.add(signal.id)
            if signal.id in scanner.reconcile_pending:
                signal.coverage.update(monitoring="paused", monitor_status="RECONCILIATION_PENDING")
                signal.coverage["reason_codes"] = [
                    "COVERAGE_SOURCE_RECONNECT_GAP" if gap else "COVERAGE_RECONCILIATION_PENDING"
                ]
                pending += 1
            else:
                advance_trades(signal, tape, now)
                signal.coverage.update(
                    monitoring="active" if fresh else "paused",
                    monitor_status="FRESH" if fresh else "SOURCE_UNAVAILABLE",
                )
                ready += int(fresh)
                degraded += int(not fresh)
                if signal.state not in TERMINAL and now >= (
                    (signal.holding_deadline_ms or signal.expires_ms)
                    if signal.state == "ALERTED"
                    else (signal.trigger_expires_ms or signal.expires_ms)
                ):
                    signal.state = "EXPIRED"
                    signal.invalidation = "Original lifecycle deadline elapsed"
            if signal.state in TERMINAL:
                if signal.coverage.get("terminal_reason") == "PLANNED_STOP_CROSSED":
                    scanner.store.put(
                        "thesis_health:" + signal.id,
                        dict(
                            policy="thesis-health-v2",
                            state="HARD_FAILURE",
                            checked_ms=now,
                            reason="PLANNED_STOP_CROSSED",
                        ),
                    )
                scanner.store.signal(signal, signal.invalidation)
            else:
                scanner.store.monitor(signal)
        except Exception as exc:
            degraded += 1
            scanner.store.put("lifecycle_error:" + signal.id, dict(at_ms=now, error_type=type(exc).__name__))
    scanner.store.put(
        "active_lifecycle",
        dict(
            at_ms=now,
            active_total=len(active),
            unique_symbols=len({s.symbol for s in active}),
            monitor_ready=ready,
            reconciliation_pending=pending,
            degraded=degraded,
            status="RECONCILING" if pending else "DEGRADED" if degraded else "FRESH",
            oldest_monitor_age_ms=max(
                (now - s.coverage.get("last_monitor_ms", now) for s in active), default=0
            ),
        ),
    )


async def run(scanner):
    await bootstrap(scanner)
    while True:
        await tick(scanner, now_ms())
        await asyncio.sleep(1)


async def reconcile_loop(scanner):
    from .reconciliation import reconcile

    await scanner.lifecycle_bootstrapped.wait()
    while True:
        for payload in scanner.store.active_signals():
            if payload["id"] not in scanner.reconcile_pending:
                continue
            try:
                await reconcile(scanner, Signal.model_validate(payload), now_ms())
            except Exception as exc:
                scanner.store.put("reconciliation_error", dict(at_ms=now_ms(), error_type=type(exc).__name__))
        await asyncio.sleep(1)
