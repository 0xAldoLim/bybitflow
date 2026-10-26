"""ResearchScheduler ownership; the orchestration context supplies explicit runtime ports.

Trading algorithms are unchanged. This service owns scheduling/logic, while
shared session state remains on Scanner during the incremental extraction.
"""

import asyncio
import json
import logging
from typing import Any, Protocol

from .exchanges import VenueAPI
from .features import validate_bars
from .horizons import DURATIONS
from .orderflow import footprint
from .storage import now_ms

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class RuntimePorts(Protocol):
    api: Any
    bounded_rest: Any
    cached_features: Any
    candle_cache: Any
    context: Any
    exchange: Any
    lifecycle_bootstrapped: Any
    recorder: Any
    settings: Any
    source_ready: Any
    store: Any
    streams: Any
    swing_profiles: Any
    volume_profiles: Any
    volume_research_attempts: Any


class ResearchScheduler:
    def __init__(self, runtime: RuntimePorts):
        self.runtime = runtime

    async def profile_loop(self):
        runtime = self.runtime
        from .orderflow import Tape
        from .profiles import build

        runtime.volume_profiles = {}
        runtime.swing_profiles = {}
        while True:
            if runtime.source_ready and runtime.recorder.healthy:
                for symbol, context in list(runtime.context.items()):
                    tape = runtime.streams.tapes.get(symbol)
                    if tape is None:
                        continue
                    now = now_ms()
                    end = now // 60000 * 60000
                    snapshot = Tape()
                    snapshot.trades = list(tape.trades)
                    snapshot.coverage_start = tape.coverage_start
                    snapshot.last_event = tape.last_event
                    inst = context["instrument"]
                    book = runtime.streams.books.get(symbol)
                    book_features = (
                        book.features(now) if book and book.fresh(now, runtime.settings.book_stale_ms) else {}
                    )
                    if book_features.get("available"):
                        from .horizons import session_context
                        from .quality import liquidity_update

                        depths = book_features["depth"]["10"]
                        impacts = [
                            book.impact(side, runtime.settings.hypothetical_notional)
                            for side in ("LONG", "SHORT")
                        ]
                        sample = dict(
                            spread=book_features["spread_bps"],
                            depth=min(depths["bid"], depths["ask"]),
                            impact=max(x["impact_bps"] for x in impacts) if all(impacts) else None,
                            epoch=book.ofi_epoch,
                            session=session_context(now)["primary"],
                        )
                        key = f"liquidity-v21:{runtime.exchange}:{symbol}"
                        runtime.store.put(key, liquidity_update(runtime.store.get(key, {}), sample, now))
                    atr = runtime.cached_features(symbol, "60", context["h1"], now)["atr"]
                    result = await asyncio.to_thread(
                        build,
                        snapshot,
                        inst.tick,
                        atr,
                        end - 1800000,
                        end,
                        now,
                        runtime.exchange,
                        symbol,
                        runtime.volume_profiles.get(symbol),
                        book_features,
                    )
                    result["available_ms"] = now_ms()
                    runtime.volume_profiles[symbol] = result
                    swing_atr = runtime.cached_features(symbol, "240", context["h4"], now)["atr"]
                    swing = await asyncio.to_thread(
                        build,
                        snapshot,
                        inst.tick,
                        swing_atr,
                        end - 14400000,
                        end,
                        now,
                        runtime.exchange,
                        symbol,
                        runtime.swing_profiles.get(symbol),
                        book_features,
                    )
                    swing["available_ms"] = now_ms()
                    runtime.swing_profiles[symbol] = swing
                    # Baselines mature from every complete selected-market window,
                    # independent of whether the strategy generated a candidate.
                    window = runtime.settings.execution_window_seconds * 1000
                    start = end - window
                    if (
                        snapshot.coverage_start <= start
                        and snapshot.last_event >= end - 10000
                        and book
                        and book.fresh(now, runtime.settings.book_stale_ms)
                    ):
                        from types import SimpleNamespace

                        from .evidence import flow_response, session_baseline
                        from .flow_quality import assess as assess_flow_quality
                        from .flow_quality import baseline as flow_baseline
                        from .flow_quality import remember
                        from .horizons import session_context

                        trades = snapshot.window(start, end)
                        flow = await asyncio.to_thread(footprint, trades, inst.tick, atr)
                        if flow.get("available"):
                            bf = book.features(now)
                            flow.update(await asyncio.to_thread(flow_response, trades, bf))
                            for horizon in runtime.settings.horizon_profiles:
                                identity = SimpleNamespace(
                                    source=runtime.exchange,
                                    symbol=symbol,
                                    horizon_profile=horizon,
                                    entry_session=session_context(now)["primary"],
                                )
                                key, history = flow_baseline(
                                    runtime.store,
                                    identity.source,
                                    symbol,
                                    identity.entry_session,
                                    horizon,
                                    end,
                                )
                                quality = await asyncio.to_thread(
                                    assess_flow_quality,
                                    trades,
                                    inst.tick,
                                    bf,
                                    flow,
                                    now,
                                    start,
                                    end,
                                    history,
                                    runtime.store.get("cross:" + symbol, {}),
                                )
                                flow["quality"] = quality
                                remember(runtime.store, key, history, quality)
                                session_baseline(runtime.store, identity, flow, bf, now, window_end_ms=end)
                    await asyncio.sleep(0.05)
                runtime.store.put(
                    "profile_health",
                    dict(
                        state="HEALTHY",
                        last_success_ms=now_ms(),
                        symbols=len(runtime.volume_profiles),
                        complete=sum(bool(p.get("available")) for p in runtime.volume_profiles.values()),
                    ),
                )
            await asyncio.sleep(30)

    async def volume_research(self, instrument, timeframe, bars, asof):
        runtime = self.runtime
        from .research_v212 import DAY, remember_volume

        interval = DURATIONS[timeframe]
        remember_volume(runtime.store, instrument.exchange, instrument.symbol, interval, bars, asof)
        attempts = getattr(runtime, "volume_research_attempts", {})
        key = (instrument.exchange, instrument.symbol, timeframe)
        if asof - attempts.get(key, 0) < 6 * 3_600_000:
            return
        attempts[key] = asof
        runtime.volume_research_attempts = dict(list(attempts.items())[-128:])
        try:
            # Reuse available candles; optional longer history must not replace
            # the production cache or change the bars used by strategy/lifecycle.
            limit = min(1000, 10 * DAY // interval + 1)
            cached = runtime.candle_cache.get((instrument.symbol, timeframe))
            if cached and cached[0] == asof // interval and len(cached[1]) >= limit:
                history = cached[1]
            else:
                history = await runtime.bounded_rest(
                    runtime.api.candles(instrument.symbol, timeframe, asof, limit=limit)
                )
                validate_bars(history, asof)
            remember_volume(runtime.store, instrument.exchange, instrument.symbol, interval, history, asof)
        except Exception as exc:
            from .diagnostics import record_failure

            record_failure(
                runtime.store,
                exc,
                scan_id=f"volume:{asof}",
                phase="research",
                operation="comparable-session warmup",
                source=instrument.exchange,
                symbol=instrument.symbol,
                range_start_ms=asof - 10 * DAY,
                range_end_ms=asof,
            )

    async def research_loop(self):
        runtime = self.runtime
        """Compact late observations use shared REST candles, never additional DOM streams."""
        from .observations import advance_batch, refresh_recommendations

        while True:
            try:
                from .operational import Priority, allowed

                if (
                    runtime.source_ready
                    and runtime.settings.post_terminal_enabled
                    and allowed(runtime.store, Priority.FORWARD_RESEARCH)
                ):
                    rows = runtime.store.db.execute(
                        "SELECT signal_id,payload FROM observations WHERE status='FOLLOWING_LATE_OUTCOME' AND next_ms<=? ORDER BY next_ms LIMIT 1000",
                        (now_ms(),),
                    ).fetchall()
                    from collections import defaultdict

                    groups = defaultdict(list)
                    for ident, raw in rows:
                        p = json.loads(raw)
                        groups[(p["signal"]["source"], p["signal"]["symbol"])].append((ident, p))
                    for (source, symbol), group in groups.items():
                        if source not in {"binance", "bybit", "okx"}:
                            continue
                        start = min(p["cursor_ms"] for _, p in group) // 60_000 * 60_000
                        api = (
                            runtime.api if source == runtime.exchange else VenueAPI(source, runtime.settings)
                        )
                        try:
                            bars = await api.candles(symbol, "1", now_ms(), limit=300, start=start)
                            await asyncio.to_thread(
                                advance_batch,
                                runtime.settings.data_dir,
                                [ident for ident, _ in group],
                                bars,
                                now_ms(),
                            )
                        finally:
                            if api is not runtime.api:
                                await api.close()
                    from .setup_research import advance_mixed_batch

                    # Oldest due market across both research policies; neither
                    # queue may starve the other. This bounds research I/O only.
                    path_rows = runtime.store.db.execute(
                        "SELECT signal_id,source,symbol,cursor_ms,kind FROM ("
                        "SELECT signal_id,source,symbol,cursor_ms,next_ms,'swing' AS kind "
                        "FROM swing_path_jobs WHERE next_ms<=? UNION ALL "
                        "SELECT signal_id,source,symbol,cursor_ms,next_ms,'v22' AS kind "
                        "FROM setup_path_jobs WHERE next_ms<=?) ORDER BY next_ms LIMIT 50",
                        (now_ms(), now_ms()),
                    ).fetchall()
                    path_groups = defaultdict(list)
                    for ident, source, symbol, cursor, kind in path_rows:
                        path_groups[(source, symbol)].append((ident, cursor, kind))
                    for (source, symbol), group in list(path_groups.items())[:5]:
                        api = (
                            runtime.api if source == runtime.exchange else VenueAPI(source, runtime.settings)
                        )
                        try:
                            bars = await api.candles(
                                symbol, "1", now_ms(), limit=300, start=min(cursor for _, cursor, _ in group)
                            )
                            await asyncio.to_thread(
                                advance_mixed_batch,
                                runtime.settings.data_dir,
                                [(ident, kind) for ident, _, kind in group],
                                bars,
                                now_ms(),
                            )
                        finally:
                            if api is not runtime.api:
                                await api.close()
                    await asyncio.to_thread(refresh_recommendations, runtime.settings.data_dir, now_ms())
                    from .forward_evidence import ingest_paths, ingest_recorded_labels

                    with runtime.store.db:
                        ingest_paths(runtime.store, now_ms())
                        ingest_recorded_labels(runtime.store, now_ms())
                    runtime.store.put("observation_health", dict(at_ms=now_ms(), processed=len(rows)))
            except Exception as exc:
                runtime.store.put("observation_health", dict(at_ms=now_ms(), error_type=type(exc).__name__))
            await asyncio.sleep(60)

    async def v8_loop(self):
        runtime = self.runtime
        from .v8_runtime import refresh

        await runtime.lifecycle_bootstrapped.wait()
        while True:
            try:
                if runtime.source_ready and runtime.recorder.healthy:
                    await refresh(runtime)
            except Exception as exc:
                runtime.store.put("v8_research", dict(at_ms=now_ms(), error_type=type(exc).__name__))
            await asyncio.sleep(60)

    async def context_ev_loop(self):
        runtime = self.runtime
        from .context_ev import refresh
        from .storage import Store

        def build():
            store = Store(runtime.settings.data_dir)
            try:
                at = now_ms()
                result = {}
                for source in ("binance", "bybit", "okx"):
                    result[source] = refresh(store, at, source=source)
                    refresh(store, at, source=source, production=True)
                from .v8_gating import effectiveness

                store.put("v8_gate_effectiveness", effectiveness(store, at))
                from .trade_quality import audit

                audit(store, at)
                return result
            finally:
                store.close()

        while True:
            try:
                from .operational import Priority, allowed

                if allowed(runtime.store, Priority.FORWARD_RESEARCH):
                    await asyncio.to_thread(build)
            except Exception as exc:
                runtime.store.put("v8_context_ev_health", dict(at_ms=now_ms(), error_type=type(exc).__name__))
            await asyncio.sleep(900)
