"""Independent core-venue collectors. Never merge tapes or award unvalidated score credit."""

import asyncio
import contextlib
from statistics import mean

from .exchanges import VENUES, VenueAPI
from .native_streams import NativeStreams
from .orderflow import footprint
from .storage import now_ms
from .streams import Streams


class ScopedStore:
    def __init__(self, store, venue):
        self.store, self.prefix = store, "secondary:" + venue + ":"

    def put(self, key, value):
        self.store.put(self.prefix + key, value)


def fresh_comparison(result, at_ms, primary):
    if (
        result.get("available")
        and primary in result.get("exchanges", [])
        and 0 <= at_ms - result.get("event_ms", 0) <= 15_000
        and 0 <= at_ms - result.get("receipt_ms", 0) <= 15_000
    ):
        return result
    return {"available": False, "reason": "Missing, stale or incompatible primary-venue comparison"}


def compare(observations, at_ms, max_age=15_000):
    rows = [r for r in observations if -1000 <= at_ms - r["event_ms"] <= max_age and r["mid"] > 0]
    if (
        len({r["exchange"] for r in rows}) < 2
        or max(r["event_ms"] for r in rows) - min(r["event_ms"] for r in rows) > 5000
    ):
        return {"available": False, "reason": "Need two independently fresh, time-aligned venue observations"}
    mids = [r["mid"] for r in rows]
    spreads = [r["spread_bps"] for r in rows]
    deltas = [r["flow"]["delta_pct"] for r in rows if r.get("flow", {}).get("available")]
    aligned_flow = len(deltas) == len(rows) and len({r.get("window_end") for r in rows}) == 1
    return dict(
        available=True,
        event_ms=max(r["event_ms"] for r in rows),
        receipt_ms=at_ms,
        exchanges=[r["exchange"] for r in rows],
        observations=rows,
        price_dislocation_bps=(max(mids) - min(mids)) / mean(mids) * 10000,
        spread_dispersion_bps=max(spreads) - min(spreads),
        delta_agreement=(all(d > 0 for d in deltas) or all(d < 0 for d in deltas)) if aligned_flow else None,
        cvd_agreement=None,
        funding_dispersion=None,
        oi_agreement=None,
        liquidation_cluster_strength=None,
        predictive_weight=0,
        limitation="Independent venue windows; funding/OI and inventory not inferred; no established OOS weight",
    )


class CrossVenue:
    def __init__(self, scanner):
        self.scanner, self.peers, self.jobs = scanner, {}, {}

    async def run(self):
        try:
            while True:
                if self.scanner.source_ready:
                    desired = set(VENUES) - {self.scanner.exchange}
                    for name in set(self.jobs) - desired:
                        self.jobs[name].cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await self.jobs.pop(name)
                    for name in desired - set(self.jobs):
                        self.jobs[name] = asyncio.create_task(self.collect(name))
                    self.publish()
                await asyncio.sleep(30)
        finally:
            for task in self.jobs.values():
                task.cancel()
            for task in self.jobs.values():
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def collect(self, name):
        settings, store, recorder = self.scanner.settings, self.scanner.store, self.scanner.recorder
        api = VenueAPI(name, settings, recorder)
        scoped = ScopedStore(store, name)
        streams = (
            Streams(settings, scoped, recorder)
            if name == "bybit"
            else NativeStreams(settings, scoped, recorder, api)
        )
        self.peers[name] = (api, streams)
        try:
            while True:
                try:
                    rows = await api.instruments()
                    available = {r["symbol"] for r in rows if api.parse(r, now_ms())}
                    selected = [s for s in settings.core_watchlist[:2] if s in available]
                    await streams.select(selected)
                    store.put(
                        "cross_health:" + name,
                        {"status": "COLLECTING", "at_ms": now_ms(), "symbols": selected},
                    )
                    await asyncio.sleep(3600)
                except Exception as exc:
                    await streams.select([])
                    store.put(
                        "cross_health:" + name,
                        {"status": "UNAVAILABLE", "at_ms": now_ms(), "error_type": type(exc).__name__},
                    )
                    await asyncio.sleep(600)
        finally:
            await streams.stop()
            await api.close()
            self.peers.pop(name, None)

    def publish(self):
        scanner, now = self.scanner, now_ms()
        for symbol in scanner.settings.core_watchlist[:2]:
            context = scanner.context.get(symbol)
            if not context:
                continue
            end = context["m15"][-1].end
            from .features import candle_features

            atr = candle_features(context["m15"], now)["atr"]
            observations = []
            sources = [(scanner.exchange, scanner.api, scanner.streams)] + [
                (n, a, s) for n, (a, s) in self.peers.items()
            ]
            for name, api, streams in sources:
                book, tape = streams.books.get(symbol), streams.tapes.get(symbol)
                if not book or not tape or not book.fresh(now, scanner.settings.book_stale_ms):
                    continue
                flow = {"available": False}
                if (
                    tape.coverage_start > 0
                    and tape.coverage_start <= end - 900_000
                    and 0 <= now - tape.last_receipt <= scanner.settings.trade_stale_ms
                ):
                    tick = (
                        api.metadata[symbol].tick
                        if symbol in getattr(api, "metadata", {})
                        else context["instrument"].tick
                    )
                    flow = footprint(tape.window(end - 900_000, end), tick, atr)
                bf = book.features(now)
                observations.append(
                    dict(
                        exchange=name,
                        symbol=symbol,
                        event_ms=book.event_ms,
                        mid=bf["mid"],
                        spread_bps=bf["spread_bps"],
                        window_end=end,
                        flow={k: flow[k] for k in ("available", "delta_pct", "cvd") if k in flow},
                    )
                )
            result = compare(observations, now)
            scanner.store.put("cross:" + symbol, result)
            scanner.recorder.offer("cross/observations", symbol, now, result)
