"""Optional, bounded V8 research refresh; never part of socket callbacks or gates."""

import asyncio
from collections import deque
from copy import deepcopy

from . import anchored, context_ev, liquidation, ofi, opportunity, spot_perp, volatility
from .storage import now_ms


def _perp_observation(scanner, symbol, asof):
    book = scanner.streams.books.get(symbol)
    tape = scanner.streams.tapes.get(symbol)
    context = scanner.context.get(symbol, {})
    if not book or not tape or not book.fresh(asof, scanner.settings.book_stale_ms):
        return None
    recent = tape.window(asof - 300_000, asof + 1)
    if not recent:
        return None
    minute = [trade for trade in recent if trade.event_ms >= asof - 60_000]
    price = book.features(asof)["mid"]
    minute_complete = bool(
        tape.coverage_start
        and tape.coverage_start <= asof - 60_000
        and minute
        and minute[0].event_ms <= asof - 55_000
        and minute[-1].event_ms >= asof - 5_000
    )

    def observed_return(start_ms):
        sample = [trade for trade in recent if trade.event_ms >= start_ms]
        return float(sample[-1].price / sample[0].price - 1) if len(sample) >= 2 else None

    first_price = float(minute[0].price) if minute_complete else None
    minute_move = float(minute[-1].price) / first_price - 1 if first_price else None
    impulse = next(
        (
            trade.event_ms
            for trade in minute
            if minute_move is not None
            and abs(minute_move) >= 0.001
            and (float(trade.price) / first_price - 1) * minute_move > 0
            and abs(float(trade.price) / first_price - 1) >= 0.001
        ),
        None,
    )

    return dict(
        source=scanner.exchange,
        source_ms=min(book.event_ms, tape.last_event),
        available_ms=asof,
        price=price,
        return_1m=observed_return(asof - 60_000) if minute_complete else None,
        return_5m=observed_return(asof - 300_000),
        delta=sum(float(t.size) * (1 if t.side == "Buy" else -1) for t in minute) if minute else None,
        flow_trust=None,
        oi_change_pct=context.get("derivatives", {}).get("oi_change_pct"),
        impulse_ms=impulse,
    )


async def refresh(scanner):
    asof = now_ms()
    source = scanner.exchange
    symbols = list(
        dict.fromkeys(scanner.pending_symbols() + list(scanner.streams.selected)[:4] + ["BTCUSDT", "ETHUSDT"])
    )[:10]
    errors = {}
    refreshed = {}

    async def one(symbol):
        try:
            book = scanner.streams.books.get(symbol)
            tape = scanner.streams.tapes.get(symbol)
            context = scanner.context.get(symbol, {})
            prior = scanner.v8_cache.get(symbol, {})
            row = dict(source=source, available_ms=asof)
            row["ofi"] = ofi.assess(book, asof, source)
            h1 = scanner.candle_cache.get((symbol, "60"), (None, []))[1]
            m15 = scanner.candle_cache.get((symbol, "15"), (None, []))[1]
            row["volatility"] = volatility.assess(h1, asof, source)
            row["anchored"] = anchored.assess(m15, asof, source)
            b = book.features(asof) if book and book.fresh(asof) else {}
            price = b.get("mid")
            minute = tape.window(asof - 60_000, asof + 1) if tape else []
            first = [t for t in minute if t.event_ms < asof - 30_000]
            second = [t for t in minute if t.event_ms >= asof - 30_000]

            def delta(trades):
                buy = sum(float(t.size) for t in trades if t.side == "Buy")
                sell = sum(float(t.size) for t in trades if t.side != "Buy")
                return 100 * (buy - sell) / (buy + sell) if buy + sell else None

            flow = dict(delta_pct=delta(minute), cvd_acceleration=(delta(second) or 0) - (delta(first) or 0))
            baseline = scanner.liquidation_baseline.setdefault(symbol, deque(maxlen=120))
            events = scanner.streams.liquidations.get(symbol) if source != "okx" else None
            recent_events = [
                event for event in (events or ()) if asof - 60_000 <= event.get("event_ms", 0) <= asof
            ]
            if recent_events and tape:
                first_event_ms = min(event["event_ms"] for event in recent_events)
                reference = tape.window(first_event_ms - 5_000, first_event_ms + 1)
                if reference:
                    flow["liquidation_reference_price"] = float(reference[-1].price)
                flow["post_liquidation_delta"] = delta(tape.window(first_event_ms, asof + 1))
            row["liquidation"] = liquidation.assess(
                events,
                asof,
                source,
                price=price,
                flow=flow,
                derivatives=context.get("derivatives"),
                book=b,
                baseline=baseline,
            )
            if events is not None:
                baseline.append(
                    row["liquidation"].get("long_liquidation_notional_1m", 0)
                    + row["liquidation"].get("short_liquidation_notional_1m", 0)
                )
            perp = _perp_observation(scanner, symbol, asof)
            spot = await scanner.bounded_rest(spot_perp.collect(symbol, asof))
            row["spot_perp"] = spot_perp.compare(spot, perp, now_ms(), prior.get("spot_perp"))
            row["available_ms"] = now_ms()
            scanner.v8_cache[symbol] = row
            refreshed[symbol] = row
        except Exception as exc:
            errors[symbol] = type(exc).__name__

    # Only a small selected set, with the same semaphore as scanner REST work.
    await asyncio.gather(*(one(symbol) for symbol in symbols))
    scanner.v8_cache = {symbol: scanner.v8_cache[symbol] for symbol in symbols if symbol in scanner.v8_cache}
    ready = list(refreshed.values())
    breadth = scanner.store.get("v8_breadth", {})
    context_cache = scanner.store.get("v8_context_ev", {})
    scanner.store.put(
        "v8_research",
        dict(
            at_ms=now_ms(),
            source=source,
            context_ev_status=context_cache.get("status", "INSUFFICIENT"),
            context_ev_samples=context_cache.get("primary_outcomes", 0),
            context_ev_cache_age_ms=now_ms() - context_cache.get("built_ms", now_ms()),
            liquidation_symbols=len(symbols),
            liquidation_available=sum(
                row.get("liquidation", {}).get("state") != "UNAVAILABLE" for row in ready
            ),
            breadth_state=breadth.get("state", "INSUFFICIENT"),
            breadth_samples=breadth.get("denominator", 0),
            ofi_symbols_ready=sum(bool(row.get("ofi", {}).get("ofi_available")) for row in ready),
            spot_symbols=len(symbols),
            spot_perp_ready=sum(row.get("spot_perp", {}).get("state") != "UNAVAILABLE" for row in ready),
            opportunity_candidates=len(scanner.store.active_signals()),
            volatility_state_counts={
                state: sum(row.get("volatility", {}).get("state") == state for row in ready)
                for state in (
                    "INSUFFICIENT",
                    "NORMAL",
                    "VOLATILITY_COMPRESSION",
                    "VOLATILITY_EXPANSION",
                    "JUMP_SHOCK",
                    "POST_SHOCK_NORMALIZATION",
                )
            },
            errors=errors,
        ),
    )


def decorate_signal(scanner, signal, context, decision_ms):
    """Freeze optional causal observations on a *new* decision, after V7 scoring."""
    source = signal.source
    cache = scanner.v8_cache.get(signal.symbol, {})
    limits = dict(ofi=30_000, liquidation=120_000, spot_perp=90_000, volatility=3_660_000)
    for name, ttl in limits.items():
        value = cache.get(name, {}) if cache.get("source") == source else {}
        if value and 0 <= decision_ms - value.get("available_ms", decision_ms + 1) <= ttl:
            signal.evidence[name] = deepcopy(value)
    breadth = scanner.store.get("v8_breadth", {})
    if breadth.get("source") == source and 0 <= decision_ms - breadth.get("available_ms", 0) <= 1_200_000:
        signal.evidence["breadth"] = deepcopy(breadth)
    bars = context.get("m15", [])
    anchors = {
        "setup_trigger": dict(
            anchor_ms=signal.created_ms, anchor_price=signal.entry, known_ms=signal.created_ms
        )
    }
    flow = signal.evidence.get("flow", {})
    levels = {name: flow.get(name) for name in ("poc", "vah", "val") if flow.get(name) is not None}
    signal.evidence["anchored"] = anchored.assess(
        bars,
        decision_ms,
        source,
        atr=signal.evidence.get("execution_features", {}).get("atr"),
        anchors=anchors,
        levels=levels,
    )
    signal.evidence["context_ev"] = context_ev.assess(
        scanner.store.get("v8_context_ev", {}), signal, decision_ms
    )
    active = [
        row
        for row in scanner.store.active_signals()
        if row.get("id") != signal.id
        and row.get("source") == source
        and row.get("state") in {"CONFIRMED", "ALERTED", "PENDING CONFIRMATION"}
    ][:29]
    bars_by_symbol = {
        symbol: value[1]
        for (symbol, timeframe), value in scanner.candle_cache.items()
        if timeframe == signal.setup_timeframe
    }
    ranks = opportunity.rank([signal, *active], decision_ms, bars_by_symbol)
    signal.evidence["opportunity_priority"] = ranks.get(signal.id, {})
