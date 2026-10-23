"""Bounded v2.2 delivery checks and current health, isolated from frozen cards."""

import asyncio
from collections import OrderedDict
from copy import deepcopy
from types import SimpleNamespace

from .impact_efficiency import assess as assess_impact
from .ofi import event_assess
from .quality_v22 import apply, enabled, frozen_delivery
from .setup_common import envelope
from .storage import now_ms
from .target_reachability import movement_capacity


class Runtime:
    def __init__(self, scanner):
        self.scanner = scanner
        self.windows = OrderedDict()

    async def fresh_flow(self, signal, book, tape, now):
        from .evidence import flow_response
        from .flow_quality import assess, baseline
        from .orderflow import footprint

        end = now // 60000 * 60000
        start = end - signal.evidence.get("execution_window_ms", 60000)
        if not (
            tape.coverage_start is not None
            and 0 < tape.coverage_start <= start
            and 0 <= now - tape.last_receipt <= self.scanner.settings.trade_stale_ms
            and -2000 <= now - tape.last_event <= self.scanner.settings.trade_stale_ms
        ):
            return None
        key = (signal.source, signal.symbol, signal.horizon_profile, end)
        for stale in [k for k, v in self.windows.items() if now - v["available_ms"] > 120000]:
            del self.windows[stale]
        cached = self.windows.get(key)
        if cached and 0 <= now - cached["available_ms"] <= 2000:
            self.windows.move_to_end(key)
            return deepcopy(cached["flow"])
        trades = [t for t in tape.window(start, end) if t.receipt_ms <= now]
        atr = signal.evidence.get("execution_features", {}).get("atr")
        instrument = self.scanner.context.get(signal.symbol, {}).get("instrument")
        if not instrument or not atr or not trades:
            return None
        bf = book.features(now)
        _, history = baseline(
            self.scanner.store,
            signal.source,
            signal.symbol,
            signal.entry_session,
            signal.horizon_profile,
            end,
        )

        # Store reads stay on the SQLite-owning event loop.
        cross = self.scanner.store.get("cross:" + signal.symbol, {})
        frozen_book = SimpleNamespace(valid=book.valid, changes=tuple(c for c in book.changes if c[0] <= now))

        def calculate():
            flow = footprint(trades, instrument.tick, atr, book=frozen_book)
            flow.update(flow_response(trades, bf))
            flow["quality"] = assess(trades, instrument.tick, bf, flow, now, start, end, history, cross)
            return flow

        flow = await asyncio.to_thread(calculate)
        self.windows[key] = dict(available_ms=now, flow=deepcopy(flow))
        self.windows.move_to_end(key)
        while len(self.windows) > 128:
            self.windows.popitem(last=False)
        return flow

    def check(self, signal, now):
        scanner = self.scanner
        if signal.source != scanner.exchange:
            return "blocked:v22-original-source-unavailable"
        connected = (
            scanner.streams.connected_for(signal.symbol)
            if hasattr(scanner.streams, "connected_for")
            else getattr(scanner.streams, "connected", False)
        )
        if not connected:
            return "blocked:v22-feed-disconnected"
        book = scanner.streams.books.get(signal.symbol)
        if not book or not book.valid or not scanner.recorder.healthy:
            return "blocked:v22-live-evidence-unavailable"
        copy = deepcopy(signal)
        copy.evidence["book"] = book.features(now)
        copy.evidence["event_ofi"] = event_assess(book, now, signal.source, signal.symbol, signal.direction)
        from .production import INTRADAY, REVERSALS, evaluate_intraday_confirmation, structure_response
        from .strategy import confirm

        execution_bars = scanner.candle_cache.get((signal.symbol, signal.execution_timeframe), (None, []))[1]
        if not execution_bars:
            return "blocked:V22_CONFIRMATION_CONTEXT_UNAVAILABLE"
        copy.evidence["v8_structure_response"] = dict(
            supportive=structure_response(copy, execution_bars, now),
            source_ms=execution_bars[-1].end,
            available_ms=now,
        )
        if copy.horizon_profile in INTRADAY and copy.family in REVERSALS:
            confirmation = evaluate_intraday_confirmation(
                copy, copy.evidence["flow"], execution_bars, now, copy.evidence.get("market_alignment", {})
            )
            copy.evidence["confirmation"] = confirmation
            copy.evidence["flow_confirmed"] = confirmation["passed"]
        else:
            copy.evidence["flow_confirmed"] = confirm(copy, copy.evidence["flow"], execution_bars)
        price = float(min(book.asks) if signal.direction == "LONG" else max(book.bids))
        liquidity = scanner.store.get(f"liquidity-v21:{signal.source}:{signal.symbol}", {})
        if not 0 <= now - liquidity.get("at_ms", 0) <= 120000:
            liquidity = {}
        apply(copy, price, now, liquidity, scanner.settings.min_net_rr, book=book, settings=scanner.settings)
        process = copy.evidence.get("impact_efficiency", {})
        if not copy.evidence.get("flow_confirmed") or not process.get("coverage_complete"):
            copy.gates = sorted(set(copy.gates + ["V22_FLOW_RESPONSE_UNAVAILABLE"]))
        if not copy.evidence.get("v22", {}).get("coverage_complete"):
            copy.gates = sorted(set(copy.gates + ["V22_REQUIRED_EVIDENCE_UNAVAILABLE"]))
        if not scanner.settings.research_alerts and scanner.settings.sss_research:
            if copy.quality < 95:
                copy.gates = sorted(set(copy.gates + ["CONFIGURED_SSS_TIER_NOT_MET"]))
            else:
                copy.final_tier = "SSS RESEARCH · UNCALIBRATED"
        if copy.gates:
            signal.coverage["v22_delivery_check"] = envelope(
                signal, "delivery-execution-v22", now, state="BLOCKED", reasons=copy.gates
            )
            return "blocked:" + copy.gates[0]
        # Only a valid current check replaces the prospective card fields.
        for key in (
            "book",
            "event_ofi",
            "entry_quality",
            "v22",
            "score_profile",
            "score_components",
            "score_reasons",
            "target_reachability",
            "stop_viability",
            "edge_age",
            "edge_observations_v22",
            "impact_efficiency",
            "v211_baseline_assessment",
        ):
            signal.evidence[key] = deepcopy(copy.evidence[key])
        signal.evidence["delivery_confirmation"] = deepcopy(copy.evidence.get("confirmation", {})) | dict(
            source=signal.source, symbol=signal.symbol, available_ms=now
        )
        signal.quality, signal.raw_tier, signal.final_tier = copy.quality, copy.raw_tier, copy.final_tier
        signal.qualification = copy.qualification
        signal.evidence["delivery_execution_snapshot"] = frozen_delivery(signal, now)
        return None

    async def prepare_delivery(self, signal):
        if not enabled(signal):
            return None
        scanner = self.scanner
        if signal.source != scanner.exchange:
            return "blocked:v22-original-source-unavailable"
        book, tape = (getattr(scanner.streams, k).get(signal.symbol) for k in ("books", "tapes"))
        if not book or not tape or not book.valid or not scanner.recorder.healthy:
            return "blocked:v22-live-evidence-unavailable"
        flow = await self.fresh_flow(signal, book, tape, now_ms())
        if flow is None:
            return "blocked:v22-flow-coverage-unavailable"
        signal.evidence["flow"] = flow
        signal.evidence["flow_quality"] = flow["quality"]
        bars = scanner.candle_cache.get((signal.symbol, signal.setup_timeframe), (None, []))[1]
        signal.evidence["horizon_movement_capacity"] = movement_capacity(signal, bars, now_ms())
        return self.check(signal, now_ms())

    def finalize_delivery(self, signal):
        """No awaits between this quote observation, persistence and dispatch."""
        if not enabled(signal):
            return None
        result = self.check(signal, now_ms())
        if not result:
            from .setup_ranking import rank_signal

            rank_signal(self.scanner, signal, now_ms())
        return result

    def dispatch_guard(self, signal):
        book = self.scanner.streams.books.get(signal.symbol)
        now = now_ms()
        snap = signal.evidence.get("delivery_execution_snapshot") or {}
        if not book or not book.fresh(now, self.scanner.settings.book_stale_ms):
            return "blocked:EXECUTION_QUOTE_STALE"
        if not 0 <= now - snap.get("available_ms", 0) <= self.scanner.settings.book_stale_ms:
            return "blocked:DELIVERY_SNAPSHOT_STALE"
        if book.event_ms > now or book.receipt_ms > now:
            return "blocked:EXECUTION_QUOTE_FUTURE"
        return None

    async def observe_health(self, signal, flow, book_features, covered, book, now):
        if not enabled(signal) or signal.state != "ALERTED" or not covered:
            return
        # Existing terminal/lifecycle checks run before notification of this state.
        copy = deepcopy(signal)
        copy.evidence.update(flow=flow, flow_quality=flow.get("quality", {}), book=book_features)
        copy.coverage["trade_window_complete"] = covered
        copy.evidence["event_ofi"] = event_assess(book, now, signal.source, signal.symbol, signal.direction)
        history = signal.coverage.get("v22_current", {}).get("history", [])
        process = assess_impact(copy, now, history=history)
        window = now // 60000 * 60000
        previous = signal.coverage.get("v22_current", {})
        if previous.get("window_ms") == window:
            return
        if not process.get("coverage_complete"):
            signal.coverage["v22_current"] = previous | dict(state="UNKNOWN", available_ms=now)
            return
        bad = bool(
            process["contradicted"]
            or (
                process.get("directional_aggression", 0) <= -20
                and (process.get("displacement_atr") or 0) <= -0.15
                and process.get("impact_persistence", 0) >= 0.75
            )
        )
        contiguous = 0 < now - previous.get("available_ms", 0) <= 120000
        run = previous.get("bad_windows", 0) + 1 if bad and contiguous else int(bad)
        episode = (previous.get("episode_ms") if contiguous and previous.get("bad_windows") else None) or now
        state = "DETERIORATING" if run >= 2 else "WATCH" if bad else "STABLE"
        current = envelope(
            signal,
            "setup-health-v22",
            now,
            state=state,
            window_ms=window,
            bad_windows=run,
            episode_ms=episode,
            flow_response=process,
            history=(history + [process])[-3:],
            reason="Persistent adverse price/flow response; original stop, targets and deadline unchanged",
        )
        signal.coverage["v22_current"] = current
        if state == "DETERIORATING":
            await self.scanner.notifier.send_deterioration(signal, episode)
