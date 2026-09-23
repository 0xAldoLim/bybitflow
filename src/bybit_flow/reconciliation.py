"""Reconstruct immutable setup paths from original-venue closed one-minute bars.

OHLC cannot reconstruct DOM or account fills. Ambiguous stop/target bars use
stop first; effective timestamps are candle-end bounds, not invented tick times.
"""

import math


def advance(signal, bars, cursor, until, detected_ms):
    deadline = signal.holding_deadline_ms or signal.expires_ms
    if signal.state != "ALERTED":
        deadline = min(deadline, signal.trigger_expires_ms or signal.expires_ms)
    entered = signal.evidence.get("observed_entry_ms")
    result = dict(
        policy="downtime-ohlc-v1",
        source=signal.source,
        method="closed-1m-OHLC",
        cursor_ms=cursor,
        detected_ms=detected_ms,
        coverage_complete=False,
    )
    expected = cursor // 60_000 * 60_000
    for bar in sorted(bars, key=lambda b: b.start):
        if bar.end <= cursor or bar.end > min(until, deadline):
            continue
        if bar.start != expected or bar.interval != 60_000:
            result["reason"] = "Missing historical candle; reconciliation pending"
            return result
        values = (bar.open, bar.high, bar.low, bar.close)
        if not all(math.isfinite(x) and x > 0 for x in values) or not (
            bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
        ):
            result["reason"] = "Invalid historical OHLC; reconciliation pending"
            return result
        stop = bar.low <= signal.stop if signal.direction == "LONG" else bar.high >= signal.stop
        target = bar.high >= signal.tp1 if signal.direction == "LONG" else bar.low <= signal.tp1
        # Entry inferred in a candle does not prove a target was later in that candle.
        entered_before_bar = entered is not None
        if (
            entered is None
            and bar.start <= (signal.trigger_expires_ms or signal.expires_ms)
            and bar.low <= signal.zone[1]
            and bar.high >= signal.zone[0]
        ):
            entered = bar.end
        result.update(cursor_ms=bar.end, observed_entry_ms=entered)
        if stop or (target and entered_before_bar and signal.horizon_profile != "LEGACY"):
            result.update(
                state="INVALIDATED" if stop else "RESOLVED",
                outcome="STOP" if stop else "TARGET",
                effective_ms=bar.end,
                coverage_complete=True,
                ambiguous=bool(stop and target) or bar.start < cursor,
                reason="Stop observed during downtime" if stop else "Target observed during downtime",
            )
            return result
        expected = bar.end
    result["coverage_complete"] = result["cursor_ms"] >= min(until, deadline) // 60_000 * 60_000
    if result["coverage_complete"] and until >= deadline:
        result.update(
            state="EXPIRED",
            outcome="EXPIRED",
            effective_ms=deadline,
            reason="Original tracking deadline elapsed during downtime",
        )
    return result


async def reconcile(scanner, signal, now):
    """One bounded page per evaluation. Persist progress; never resume over a gap."""
    key = "reconciliation:" + signal.id
    saved = scanner.store.get(key, {})
    initial = (
        signal.coverage.get("monitor_cursor_event_ms")
        or signal.coverage.get("last_trustworthy_ms")
        or signal.coverage.get("pause_since_ms")
        or signal.coverage.get("checked_ms")
        or signal.created_ms
    )
    cursor = max(initial, saved.get("cursor_ms", 0))
    from .lifecycle import source_feed

    original_api, streams = source_feed(scanner, signal.source)
    book = streams.books.get(signal.symbol) if streams else None
    tape = streams.tapes.get(signal.symbol) if streams else None
    fresh = bool(
        scanner.recorder.healthy
        and book
        and book.fresh(now, scanner.settings.book_stale_ms)
        and tape
        and 0 <= now - tape.last_receipt <= scanner.settings.trade_stale_ms
    )
    live_covers_tail = bool(fresh and tape.coverage_start is not None and tape.coverage_start <= cursor)
    if live_covers_tail:
        signal.coverage.update(
            monitor_cursor_event_ms=cursor,
            last_monitor_ms=now,
            monitoring="active",
            monitor_status="RECONCILED_PENDING_PATH",
        )
        signal.coverage.pop("last_checked_trade_id", None)
        # Keep the outage key until the live path has been checked. Health may
        # emit one resume only if that path did not discover a terminal event.
        scanner.store.signal(signal, "Historical gap covered; live path resumes")
        scanner.reconcile_pending.discard(signal.id)
        scanner.store.put(key, saved | dict(status="RECONCILED", cursor_ms=cursor, detected_ms=now))
        return True
    if now - saved.get("last_attempt_ms", 0) < 15_000:
        return False
    from .exchanges import VenueAPI

    api = original_api or VenueAPI(signal.source, scanner.settings)
    try:
        bars = await api.candles(signal.symbol, "1", now, start=cursor // 60_000 * 60_000, limit=300)
        result = advance(signal, bars, cursor, now, now)
    except Exception as exc:
        result = dict(cursor_ms=cursor, coverage_complete=False, error_type=type(exc).__name__)
    finally:
        if api is not original_api:
            await api.close()
    result.update(last_attempt_ms=now, status="CATCHING_UP_LIFECYCLE")
    scanner.store.put(key, result)
    signal.coverage.update(
        monitoring="paused",
        pause_since_ms=signal.coverage.get("pause_since_ms", initial),
        reconciliation=result,
        monitoring_event=None,
    )
    if result.get("observed_entry_ms"):
        signal.evidence.setdefault("observed_entry_ms", result["observed_entry_ms"])
    if result.get("state"):
        alerted = signal.state == "ALERTED"
        signal.state = result["state"]
        signal.coverage["terminal_reason"] = result["reason"]
        if result.get("outcome") == "STOP":
            signal.coverage["terminal_reason"] = "PLANNED_STOP_CROSSED"
        signal.evidence["terminal_event"] = dict(
            effective_ms=result["effective_ms"],
            detected_ms=now,
            event_price=None,
            reference_price=signal.stop if result.get("outcome") == "STOP" else signal.tp1,
            source=signal.source,
            method="CLOSED_1M_OHLC",
            ambiguous=result.get("ambiguous", False),
        )
        signal.evidence["primary_outcome"] = "UNCLEAR" if result.get("ambiguous") else result["outcome"]
        scanner.store.signal(signal, result["reason"])
        if alerted:
            await scanner.notifier.send_research(signal, update=True)
        scanner.reconcile_pending.discard(signal.id)
        return False
    scanner.store.signal(signal, "Downtime reconciliation pending")
    return False
