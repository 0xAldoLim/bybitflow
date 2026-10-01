"""Optional bounded V8 observations; production policy lives in v8_gating."""

import asyncio
from collections import deque
from copy import deepcopy

from . import anchored, context_ev, liquidation, ofi, opportunity, spot_perp, volatility
from .storage import now_ms


def _perp_observation(scanner, symbol, asof):
    book, tape = scanner.streams.books.get(symbol), scanner.streams.tapes.get(symbol)
    if not book or not tape or not book.fresh(asof, scanner.settings.book_stale_ms):
        return None
    # Align last observed executions to the same closed minute as spot klines.
    end = asof // 60_000 * 60_000 - 1
    recent = tape.window(end - 305_000, end + 1)

    def close_at(boundary):
        rows = [t for t in recent if t.event_ms <= boundary and t.receipt_ms <= asof]
        return float(rows[-1].price) if rows and boundary - rows[-1].event_ms <= 5000 else None

    last, one, five = (close_at(t) for t in (end, end - 60_000, end - 300_000))
    oi = liquidation.oi_changes(scanner.oi_series.get((scanner.exchange, symbol), ()), asof)
    minute = [t for t in recent if end - 60_000 < t.event_ms <= end]
    return dict(
        source=scanner.exchange,
        symbol=symbol,
        source_ms=end,
        available_ms=asof,
        price=last,
        return_1m=last / one - 1 if last and one else None,
        return_5m=last / five - 1 if last and five else None,
        price_coverage_complete=bool(last and one),
        flow_trust=None,
        delta=sum(float(t.size) * (1 if t.side == "Buy" else -1) for t in minute),
        oi_change_1m_pct=oi["oi_change_1m_pct"],
    )


async def refresh(scanner):
    asof = now_ms()
    source = scanner.exchange
    symbols = list(
        dict.fromkeys(["BTCUSDT", "ETHUSDT"] + scanner.pending_symbols() + list(scanner.streams.selected)[:4])
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
            from .horizons import session_context

            baseline_key = f"v8_ofi_baseline:{source}:{symbol}:{session_context(asof)['primary']}"
            ofi_prior = scanner.store.get(baseline_key, [])
            row["ofi"] = ofi.assess(book, asof, source, baseline=ofi_prior)
            if row["ofi"].get("production_coverage_ready"):
                compact = {
                    k: row["ofi"][k] for k in ("ofi_normalized_l1", "ofi_normalized_5bps", "ofi_persistence")
                }
                if all(v is not None for v in compact.values()):
                    compact["available_ms"] = asof
                    scanner.store.put(baseline_key, (ofi_prior + [compact])[-120:])
            h1 = scanner.candle_cache.get((symbol, "60"), (None, []))[1]
            row["volatility"] = volatility.assess(h1, asof, source)
            b = book.features(asof) if book and book.fresh(asof) else {}
            price = b.get("mid")
            minute = [t for t in tape.window(asof - 60_000, asof + 1) if t.receipt_ms <= asof] if tape else []
            first = [t for t in minute if t.event_ms < asof - 30_000]
            second = [t for t in minute if t.event_ms >= asof - 30_000]

            def delta(trades):
                buy = sum(float(t.size) for t in trades if t.side == "Buy")
                sell = sum(float(t.size) for t in trades if t.side != "Buy")
                return 100 * (buy - sell) / (buy + sell) if buy + sell else None

            flow = dict(delta_pct=delta(minute), cvd_acceleration=(delta(second) or 0) - (delta(first) or 0))
            inst = context.get("instrument")
            complete = bool(
                tape
                and minute
                and (
                    tape.coverage_start <= asof - 60_000
                    and 0 <= asof - minute[-1].event_ms <= 15_000
                    and 0 <= asof - minute[-1].receipt_ms <= 15_000
                )
            )
            if inst and complete:
                from .flow_quality import assess as assess_flow
                from .orderflow import footprint

                raw = footprint(
                    minute, inst.tick, scanner.cached_features(symbol, "60", h1, asof)["atr"], book
                )
                quality = assess_flow(minute, inst.tick, b, raw, asof, asof - 60_000, asof)
                flow.update(
                    absorption_long=raw.get("absorption_long"),
                    absorption_short=raw.get("absorption_short"),
                    flow_trust_score=quality.get("flow_trust_score"),
                )
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
                oi_series=scanner.oi_series.get((source, symbol), ()),
            )
            if events is not None:
                baseline.append(
                    row["liquidation"].get("long_liquidation_notional_1m", 0)
                    + row["liquidation"].get("short_liquidation_notional_1m", 0)
                )
            perp = _perp_observation(scanner, symbol, asof)
            spot = await scanner.spot_collector.collect(symbol, asof)
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
    context_cache = scanner.store.get("v8_context_ev:" + source, {})
    lookup = scanner.store.get("v8_context_ev_lookup:" + source, {})
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

    from statistics import median

    scanner.store.put(
        "v8_feature_readiness",
        dict(
            at_ms=now_ms(),
            source=source,
            degraded=dict(getattr(scanner, "v8_disabled", {})),
            spot_perp=dict(
                symbols=len(symbols),
                price_ready=sum(bool(r.get("spot_perp", {}).get("price_coverage_complete")) for r in ready),
                delta_ready=sum(
                    bool(r.get("spot_perp", {}).get("spot_trade_window_complete")) for r in ready
                ),
                errors=errors,
                **(scanner.spot_collector.metrics() if hasattr(scanner, "spot_collector") else {}),
            ),
            ofi=dict(
                ready=sum(bool(r.get("ofi", {}).get("production_coverage_ready")) for r in ready),
                median_coverage_seconds=median(
                    [r.get("ofi", {}).get("ofi_coverage_seconds", 0) for r in ready]
                )
                if ready
                else 0,
                baseline_mature=sum(r.get("ofi", {}).get("baseline_samples", 0) >= 20 for r in ready),
            ),
            breadth=dict(
                denominator=breadth.get("denominator", 0),
                state=breadth.get("state"),
                production_ready=breadth.get("production_ready", False),
                history_points=len(scanner.store.get("v8_breadth_history:" + source, [])),
            ),
            liquidation=dict(
                symbols_ready=sum(bool(r.get("liquidation", {}).get("production_ready")) for r in ready),
                oi_window_ready=sum(
                    bool(r.get("liquidation", {}).get("event_window_oi_ready")) for r in ready
                ),
                baseline_ready=sum(r.get("liquidation", {}).get("baseline_samples", 0) >= 20 for r in ready),
            ),
            context_ev=dict(
                source=source,
                status=context_cache.get("status", "INSUFFICIENT"),
                confidence=lookup.get("confidence", "INSUFFICIENT"),
                effective_samples=lookup.get("effective_samples", 0),
                cache_age_ms=now_ms() - context_cache.get("available_ms", now_ms()),
            ),
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
    from .horizons import session_context

    baseline_key = f"v8_ofi_baseline:{source}:{signal.symbol}:{session_context(decision_ms)['primary']}"
    priors = scanner.store.get(baseline_key, [])
    # Refresh appended the current window; only strictly earlier windows form the baseline.
    signal.evidence["ofi"] = ofi.assess(
        scanner.streams.books.get(signal.symbol),
        decision_ms,
        source,
        baseline=[r for r in priors if r.get("available_ms", decision_ms) <= decision_ms - 60_000],
    )
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
        h1=context.get("h1", []),
        profile_ms=decision_ms if flow.get("available") else None,
    )
    signal.evidence["context_ev"] = context_ev.assess(
        scanner.store.get("v8_context_ev:" + source, {}), signal, decision_ms
    )
    scanner.store.put(
        "v8_context_ev_lookup:" + source,
        {
            k: signal.evidence["context_ev"].get(k)
            for k in ("source", "confidence", "effective_samples", "available_ms")
        },
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
