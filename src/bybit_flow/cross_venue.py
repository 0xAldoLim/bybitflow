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
        and -2000 <= at_ms - result.get("event_ms", 0) <= 15_000
        and 0 <= at_ms - result.get("receipt_ms", 0) <= 15_000
    ):
        return result
    return {"available": False, "reason": "Missing, stale or incompatible primary-venue comparison"}


SUBSTITUTION_POLICY = "cross-venue-flow-substitution-v1"


def substitution(signal, local_flow, comparison, start, end, at_ms, structure_valid):
    """Return independent remote proof for one closed decision window, or no proof."""
    quality = local_flow.get("quality", {})
    sign = 1 if signal.direction == "LONG" else -1
    result = dict(
        passed=False,
        policy=SUBSTITUTION_POLICY,
        local_flow_quality_state=quality.get("flow_quality_state"),
        local_flow_trust_score=quality.get("flow_trust_score"),
        confirming_venues=[],
        trusted_flow_venues=0,
        trusted_consensus_delta_sign=comparison.get("trusted_consensus_delta_sign"),
        cross_venue_effective_delta_sign=None,
        cross_venue_price_response={},
        cross_venue_observation_ms=comparison.get("event_ms"),
        primary_source=signal.source,
        decision_ms=end,
    )
    opposite = (
        (quality.get("flow_trust_score") or 0) >= 0.6
        and sign * (quality.get("effective_delta_notional") or 0) < 0
        and sign * (local_flow.get("delta_pct") or 0) <= -10
        and sign * (local_flow.get("cvd_slope") or 0) < 0
        and (local_flow.get("delta_persistence") or 0) >= 0.6
    )
    if opposite:
        return result | {"reason": "CROSS_VENUE_DISAGREEMENT"}
    if (
        quality.get("flow_quality_state") not in {"LOW_INFORMATION_VOLUME", "REPETITIVE_TWO_SIDED_CHURN"}
        or (quality.get("flow_trust_score") or 0) >= 0.6
    ):
        return result | {"reason": "LOCAL_FLOW_NOT_SUBSTITUTABLE"}
    if not structure_valid:
        return result | {"reason": "LOCAL_STRUCTURE_UNCONFIRMED"}
    if (
        not comparison.get("available")
        or signal.source not in comparison.get("exchanges", [])
        or not comparison.get("cross_venue_trusted_flow_agreement")
        or comparison.get("trusted_consensus_delta_sign") != ("POSITIVE" if sign > 0 else "NEGATIVE")
        or not 0 <= at_ms - comparison.get("receipt_ms", -1) <= 15_000
    ):
        return result | {"reason": "CROSS_VENUE_EVIDENCE_UNAVAILABLE"}
    confirmed = []
    for row in comparison.get("observations", []):
        if row.get("exchange") == signal.source or not row.get("flow", {}).get("available"):
            continue
        q = row["flow"].get("quality", {})
        if (
            row.get("window_start") != start
            or row.get("window_end") != end
            or not -2_000 <= at_ms - row.get("event_ms", 0) <= 15_000
            or (q.get("flow_trust_score") or 0) < 0.6
            or sign * (q.get("effective_delta_notional") or 0) <= 0
            or sign * (q.get("price_displacement_bps") or 0) < 3
            or (q.get("book_response_consistency") or 0) < 0.4
        ):
            continue
        confirmed.append(row)
    if len({row["exchange"] for row in confirmed}) < 2:
        return result | {"reason": "TWO_TRUSTED_REMOTE_VENUES_REQUIRED"}
    result.update(
        passed=True,
        confirming_venues=[row["exchange"] for row in confirmed],
        trusted_flow_venues=len(confirmed),
        cross_venue_effective_delta_sign="POSITIVE" if sign > 0 else "NEGATIVE",
        cross_venue_price_response={
            row["exchange"]: row["flow"]["quality"]["price_displacement_bps"] for row in confirmed
        },
        observations=[
            dict(
                exchange=row["exchange"],
                flow_trust_score=row["flow"]["quality"]["flow_trust_score"],
                effective_delta_notional=row["flow"]["quality"]["effective_delta_notional"],
                price_displacement_bps=row["flow"]["quality"]["price_displacement_bps"],
                book_response_consistency=row["flow"]["quality"]["book_response_consistency"],
                event_ms=row["event_ms"],
                window_start_ms=start,
                window_end_ms=end,
            )
            for row in confirmed
        ],
    )
    return result


def compare(observations, at_ms, max_age=15_000):
    rows = [r for r in observations if -2000 <= at_ms - r["event_ms"] <= max_age and r["mid"] > 0]
    event_skew = max(r["event_ms"] for r in rows) - min(r["event_ms"] for r in rows) if rows else None
    if len({r["exchange"] for r in rows}) < 2 or event_skew > 5000:
        return {
            "available": False,
            "reason": "Need two independently fresh, time-aligned venue observations",
            "observed_exchanges": [r["exchange"] for r in observations],
            "fresh_exchanges": [r["exchange"] for r in rows],
            "event_skew_ms": event_skew,
            "at_ms": at_ms,
        }
    mids = [r["mid"] for r in rows]
    spreads = [r["spread_bps"] for r in rows]
    deltas = [r["flow"]["delta_pct"] for r in rows if r.get("flow", {}).get("available")]
    aligned_flow = (
        len(deltas) == len(rows)
        and len({r.get("window_start") for r in rows}) == 1
        and len({r.get("window_end") for r in rows}) == 1
        and all(
            r.get("window_start") is not None
            and r.get("window_end", at_ms + 1) <= at_ms
            and r["window_start"] < r["window_end"]
            for r in rows
        )
    )
    consensus = (
        (
            "NEGATIVE"
            if all(d < 0 for d in deltas)
            else "POSITIVE"
            if all(d > 0 for d in deltas)
            else "ZERO"
            if all(d == 0 for d in deltas)
            else "MIXED"
        )
        if aligned_flow
        else "UNAVAILABLE"
    )
    trusted = [
        r
        for r in rows
        if r.get("flow", {}).get("quality", {}).get("flow_trust_score", 0) >= 0.6
        and (
            r["flow"]["quality"].get("effective_delta_notional", 0)
            * r["flow"]["quality"].get("price_displacement_bps", 0)
            > 0
            or r["flow"]["quality"].get("flow_quality_state") == "GENUINE_ABSORPTION"
        )
    ]
    trusted_signs = [
        1
        if r["flow"]["quality"].get("effective_delta_notional", 0) > 0
        else -1
        if r["flow"]["quality"].get("effective_delta_notional", 0) < 0
        else 0
        for r in trusted
    ]
    trusted_consensus = (
        (
            "POSITIVE"
            if len(trusted_signs) >= 2 and all(s > 0 for s in trusted_signs)
            else "NEGATIVE"
            if len(trusted_signs) >= 2 and all(s < 0 for s in trusted_signs)
            else "MIXED"
            if len(trusted_signs) >= 2
            else "UNAVAILABLE"
        )
        if aligned_flow
        else "UNAVAILABLE"
    )
    return dict(
        available=True,
        mean_delta_pct=mean(deltas) if aligned_flow else None,
        consensus_delta_sign=consensus,
        trusted_consensus_delta_sign=trusted_consensus,
        cross_venue_trusted_flow_agreement=trusted_consensus in {"POSITIVE", "NEGATIVE"},
        trusted_flow_venues=len(trusted) if aligned_flow else 0,
        aligned_flow_venues=len(rows) if aligned_flow else 0,
        wick_context="CROSS_VENUE_DISAGREEMENT"
        if trusted_consensus == "MIXED"
        else "LOCAL_VENUE_SPIKE"
        if (max(mids) - min(mids)) / mean(mids) * 10000 > max(10, 3 * max(spreads))
        else "MARKET_WIDE_SWEEP"
        if trusted_consensus in {"POSITIVE", "NEGATIVE"}
        and all(
            r.get("flow", {}).get("potential_trapped_buyers")
            or r.get("flow", {}).get("potential_trapped_sellers")
            for r in rows
        )
        else "CROSS_VENUE_UNAVAILABLE",
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


def observed_flow(recent, tick, atr, book_features, now, start, end):
    """Compute a compact cross-venue observation away from the socket event loop."""
    from .evidence import flow_response
    from .flow_quality import assess as assess_flow_quality

    flow = footprint(recent, tick, atr)
    if not flow.get("available"):
        return flow
    flow.update(flow_response(recent, book_features))
    quality = assess_flow_quality(recent, tick, book_features, flow, now, start, end)
    flow["quality"] = {
        key: quality.get(key)
        for key in (
            "flow_quality_state",
            "flow_trust_score",
            "effective_delta_notional",
            "price_displacement_bps",
            "book_response_consistency",
            "microprice_displacement",
            "effective_volume_ratio",
        )
    }
    return flow


class CrossVenue:
    def __init__(self, scanner):
        self.scanner, self.peers, self.jobs = scanner, {}, {}
        self.contexts, self.candles = {}, {}

    async def refresh_active_context(self, name, api, rows):
        from .features import validate_bars

        now = now_ms()
        instruments = {r["symbol"]: api.parse(r, now) for r in rows}
        active = [s for s in self.scanner.store.active_signals() if s["source"] == name]
        symbols = {s["symbol"] for s in active}
        for cache in (self.contexts, self.candles):
            for key in list(cache):
                if key[0] == name and key[1] not in symbols:
                    cache.pop(key)
        for symbol in self.required(name):
            inst = instruments.get(symbol)
            if not inst:
                continue
            intervals = {"240", "60", "15"} | {
                s.get(key, "60")
                for s in active
                if s["symbol"] == symbol
                for key in ("setup_timeframe", "context_timeframe")
            }
            bars = {}
            for interval in intervals:
                bars[interval] = await api.candles(symbol, interval, now, limit=200)
                validate_bars(bars[interval], now)
                self.candles[name, symbol, interval] = bars[interval]
            self.contexts[name, symbol] = dict(
                instrument=inst, asof=now, h4=bars["240"], h1=bars["60"], m15=bars["15"], derivatives={}
            )

    def required(self, name):
        active = [s for s in self.scanner.store.active_signals() if s["source"] == name]
        lifecycle = {s["symbol"] for s in active if s["state"] == "ALERTED"}
        pending = {s["symbol"] for s in active if s["state"] != "ALERTED"}
        self.scanner.store.put("required_candidate_symbols:" + name, sorted(pending))
        self.scanner.store.put("active_lifecycle_symbols:" + name, sorted(lifecycle))
        return lifecycle | pending

    def ensure_peer(self, name):
        if name not in self.peers:
            scanner = self.scanner
            api = VenueAPI(name, scanner.settings, scanner.recorder)
            scoped = ScopedStore(scanner.store, name)
            streams = (
                Streams(scanner.settings, scoped, scanner.recorder)
                if name == "bybit"
                else NativeStreams(scanner.settings, scoped, scanner.recorder, api)
            )
            self.peers[name] = (api, streams)
        return self.peers[name]

    async def restore_required(self):
        for name in VENUES:
            if name == self.scanner.exchange:
                continue
            required = self.required(name)
            if not required and name not in self.peers:
                continue
            _, streams = self.ensure_peer(name)
            streams.required_symbols = required
            await streams.select(sorted(required) + list(streams.selected))

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
                    await self.publish()
                await asyncio.sleep(30)
        finally:
            for task in self.jobs.values():
                task.cancel()
            for task in self.jobs.values():
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def collect(self, name):
        store = self.scanner.store
        api, streams = self.ensure_peer(name)
        try:
            while True:
                try:
                    rows = await api.instruments()
                    available = {r["symbol"] for r in rows if api.parse(r, now_ms())}
                    scoped_symbols = self.scanner.pending_symbols() + list(self.scanner.streams.selected)
                    selected = [s for s in dict.fromkeys(scoped_symbols) if s in available][:8]
                    streams.required_symbols = self.required(name)
                    await streams.select(selected)
                    await self.refresh_active_context(name, api, rows)
                    store.put(
                        "cross_health:" + name,
                        {"status": "COLLECTING", "at_ms": now_ms(), "symbols": selected},
                    )
                    await asyncio.sleep(60)
                except Exception as exc:
                    streams.required_symbols = self.required(name)
                    await streams.select(sorted(streams.required_symbols))
                    store.put(
                        "cross_health:" + name,
                        {"status": "UNAVAILABLE", "at_ms": now_ms(), "error_type": type(exc).__name__},
                    )
                    await asyncio.sleep(30)
        finally:
            if self.peers.get(name) == (api, streams):
                await streams.stop()
                await api.close()
                self.peers.pop(name, None)

    async def publish(self):
        scanner, now = self.scanner, now_ms()
        for symbol in dict.fromkeys(scanner.pending_symbols() + list(scanner.streams.selected)):
            context = scanner.context.get(symbol)
            if not context:
                continue
            end = now // 60000 * 60000
            window_ms = scanner.settings.execution_window_seconds * 1000
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
                bf = book.features(now)
                if (
                    tape.coverage_start > 0
                    and tape.coverage_start <= end - window_ms
                    and 0 <= now - tape.last_receipt <= scanner.settings.trade_stale_ms
                ):
                    tick = (
                        api.metadata[symbol].tick
                        if symbol in getattr(api, "metadata", {})
                        else context["instrument"].tick
                    )
                    recent = tape.window(end - window_ms, end)
                    flow = await asyncio.to_thread(
                        observed_flow, recent, tick, atr, bf, now, end - window_ms, end
                    )
                observations.append(
                    dict(
                        exchange=name,
                        symbol=symbol,
                        event_ms=book.event_ms,
                        mid=bf["mid"],
                        spread_bps=bf["spread_bps"],
                        window_end=end,
                        window_start=end - window_ms,
                        flow={
                            k: flow[k]
                            for k in (
                                "available",
                                "delta_pct",
                                "cvd",
                                "potential_trapped_buyers",
                                "potential_trapped_sellers",
                                "quality",
                            )
                            if k in flow
                        },
                    )
                )
            result = compare(observations, now)
            scanner.store.put("cross:" + symbol, result)
            scanner.recorder.offer("cross/observations", symbol, now, result)
