"""MarketDataCoordinator ownership; the orchestration context supplies explicit runtime ports.

Trading algorithms are unchanged. This service owns scheduling/logic, while
shared session state remains on Scanner during the incremental extraction.
"""

import asyncio
import json
import logging
from copy import deepcopy
from typing import Any, Protocol

import httpx

from .exchanges import VenueAPI, market_probe
from .features import candle_features, validate_bars
from .horizons import DURATIONS
from .native_streams import NativeStreams
from .recovery import failure_health, success_health
from .storage import now_ms

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class RuntimePorts(Protocol):
    api: Any
    bounded_rest: Any
    candle_cache: Any
    candle_cache_limits: Any
    context: Any
    cross_venue: Any
    derivative_cache: Any
    derivative_cache_hits: Any
    derivative_cache_misses: Any
    exchange: Any
    feature_cache: Any
    feature_cache_hits: Any
    feature_cache_misses: Any
    flow_ready: Any
    instrument_cache: Any
    liquidation_baseline: Any
    oi_series: Any
    pending_symbols: Any
    record_quotes: Any
    recorder: Any
    rest_inflight: Any
    rest_queue_depth: Any
    rest_reduced_semaphore: Any
    rest_reduced_until: Any
    rest_semaphore: Any
    scan_lock: Any
    select_source: Any
    settings: Any
    source_ready: Any
    spread_history: Any
    spread_key: Any
    status: Any
    store: Any
    streams: Any
    v8_cache: Any


class MarketDataCoordinator:
    def __init__(self, runtime: RuntimePorts):
        from collections import OrderedDict

        self.runtime = runtime
        self.candle_cache, self.candle_cache_limits = {}, {}
        self.feature_cache = OrderedDict()
        self.feature_cache_hits = self.feature_cache_misses = 0
        self.derivative_cache = {}
        self.derivative_cache_hits = self.derivative_cache_misses = 0
        settings = getattr(runtime, "settings", None)
        self.rest_semaphore = asyncio.Semaphore(getattr(settings, "rest_concurrency", 3))
        self.rest_reduced_semaphore = asyncio.Semaphore(1)
        self.rest_reduced_until = self.rest_inflight = self.rest_queue_depth = 0

    async def select_source(self):
        runtime = self.runtime
        if runtime.source_ready or not isinstance(runtime.api, VenueAPI):
            return
        # Stable primary until failure, deterministic preference; no round-robin tape mixing.
        for name in ("binance", "bybit", "okx"):
            report = await market_probe(name, runtime.settings)
            runtime.store.put("probe:" + name, report)
            if report["status"] != "HEALTHY":
                continue
            previous = runtime.exchange
            if name == previous:
                runtime.source_ready = True
                return
            if runtime.pending_symbols():
                runtime.cross_venue.peers[previous] = (runtime.api, runtime.streams)
            else:
                await runtime.streams.stop()
                await runtime.api.close()
            runtime.api, runtime.streams = runtime.cross_venue.ensure_peer(name)
            runtime.cross_venue.peers.pop(name)
            # A promoted collector is the primary now. Publish its health under
            # the primary keys rather than the former secondary namespace.
            runtime.streams.store = runtime.store
            job = runtime.cross_venue.jobs.pop(name, None)
            if job:
                job.cancel()
            runtime.context.clear()
            runtime.candle_cache.clear()
            runtime.candle_cache_limits.clear()
            runtime.feature_cache.clear()
            runtime.derivative_cache.clear()
            runtime.v8_cache.clear()
            runtime.liquidation_baseline.clear()
            runtime.instrument_cache = None
            runtime.source_ready = True
            change = dict(
                previous=previous,
                current=name,
                at_ms=now_ms(),
                reason="verified REST and trade WS probe; fresh book/tape warmup required",
            )
            runtime.store.put("active_exchange", change)
            runtime.recorder.offer("control/source_change", "ALL", now_ms(), change, complete=False)
            return
        raise ConnectionError("No compatible public exchange passed REST and trade WS checks")

    async def cached_candles(self, symbol, interval, asof, limit):
        runtime = self.runtime
        duration = DURATIONS[interval]
        key, boundary = (symbol, interval), asof // duration
        cached = runtime.candle_cache.get(key)
        fetched_boundary, fetched_limit = runtime.candle_cache_limits.get(key, (None, 0))
        if (
            cached
            and cached[0] == boundary
            and (len(cached[1]) >= limit or (fetched_boundary == boundary and fetched_limit >= limit))
        ):
            return cached[1]
        bars = await runtime.bounded_rest(runtime.api.candles(symbol, interval, asof, limit=limit))
        validate_bars(bars, asof)
        runtime.candle_cache[key] = (boundary, bars)
        # A fresh short history cannot gain closed bars before the next boundary.
        # Keep the requested limit so new listings do not trigger duplicate REST work.
        if bars and bars[-1].end == boundary * duration:
            runtime.candle_cache_limits[key] = (boundary, limit)
        else:
            runtime.candle_cache_limits.pop(key, None)
        return bars

    async def bounded_rest(self, awaitable):
        runtime = self.runtime
        runtime.rest_queue_depth += 1
        waiting = True
        try:
            async with runtime.rest_semaphore:
                runtime.rest_queue_depth -= 1
                waiting = False
                runtime.rest_inflight += 1
                try:
                    if now_ms() < runtime.rest_reduced_until:
                        async with runtime.rest_reduced_semaphore:
                            return await awaitable
                    return await awaitable
                except (PermissionError, httpx.HTTPError, TimeoutError) as exc:
                    if isinstance(exc, (PermissionError, httpx.TimeoutException)) or (
                        isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500
                    ):
                        runtime.rest_reduced_until = now_ms() + 60_000
                    raise
                finally:
                    runtime.rest_inflight -= 1
        except BaseException:
            if waiting:
                runtime.rest_queue_depth -= 1
            raise

    async def optional_rest(self, awaitable):
        runtime = self.runtime
        """Optional spot work shares the current venue's pacing lock and concurrency."""
        import time

        owner = runtime.api.bybit if runtime.api.bybit else runtime.api
        async with owner.lock:
            await asyncio.sleep(max(0, owner.next_request - time.monotonic()))
            owner.next_request = time.monotonic() + 1 / runtime.settings.rest_requests_per_second
        return await runtime.bounded_rest(awaitable)

    def cached_features(self, symbol, timeframe, bars, asof):
        runtime = self.runtime
        if not bars or not hasattr(runtime, "feature_cache"):
            return candle_features(bars, asof)
        closed_end = max((bar.end for bar in bars if bar.end <= asof), default=0)
        key = (runtime.exchange, symbol, timeframe, closed_end, "candle-features-v1")
        if key in runtime.feature_cache:
            runtime.feature_cache_hits += 1
            runtime.feature_cache.move_to_end(key)
            return deepcopy(runtime.feature_cache[key])
        runtime.feature_cache_misses += 1
        result = candle_features(bars, asof)
        runtime.feature_cache[key] = deepcopy(result)
        if len(runtime.feature_cache) > 512:
            runtime.feature_cache.popitem(last=False)
        return result

    async def cached_derivative_history(self, endpoint, symbol, start, end, ttl_ms):
        runtime = self.runtime
        key = (runtime.exchange, symbol, endpoint)
        cached = runtime.derivative_cache.get(key)
        if cached and 0 <= end - cached[0] < ttl_ms:
            runtime.derivative_cache_hits += 1
            return deepcopy(cached[1]), cached[0]
        runtime.derivative_cache_misses += 1
        rows = await runtime.bounded_rest(runtime.api.history(endpoint, symbol, start, end))
        observed_ms = now_ms()
        runtime.derivative_cache[key] = (observed_ms, deepcopy(rows))
        if len(runtime.derivative_cache) > 300:
            runtime.derivative_cache.pop(next(iter(runtime.derivative_cache)))
        return rows, observed_ms

    def record_quotes(self, body, receipt):
        runtime = self.runtime
        oi_scope = set(
            list(
                dict.fromkeys(
                    ["BTCUSDT", "ETHUSDT"] + runtime.pending_symbols() + list(runtime.streams.selected)
                )
            )[:10]
        )
        runtime.oi_series = {
            key: value
            for key, value in runtime.oi_series.items()
            if key[0] == runtime.exchange and key[1] in oi_scope
        }
        updates = []
        for ticker in body["result"]["list"]:
            symbol = ticker["symbol"]
            if symbol in runtime.context:
                d = runtime.context[symbol]["derivatives"]
                if ticker.get("fundingRate") is not None:
                    d.update(
                        funding_rate=float(ticker["fundingRate"]),
                        ticker_observed_ms=int(
                            ticker.get("funding_observed_ms", ticker.get("observed_ms", body["time"]))
                        ),
                    )
            if symbol in oi_scope:
                from collections import deque

                from .liquidation import sample_oi

                key = (runtime.exchange, symbol)
                series = runtime.oi_series.setdefault(
                    key, deque(runtime.store.get(f"v8_oi:{runtime.exchange}:{symbol}", []), maxlen=30)
                )
                sample_oi(
                    series,
                    int(ticker.get("observed_ms", body["time"])),
                    receipt,
                    ticker.get("openInterest"),
                    ticker.get("openInterestValue"),
                )
                runtime.store.put(f"v8_oi:{runtime.exchange}:{symbol}", list(series))
            history = runtime.spread_history(symbol)
            history.add(
                int(ticker.get("observed_ms", body["time"])),
                receipt,
                float(ticker.get("bid1Price") or 0),
                float(ticker.get("ask1Price") or 0),
            )
            updates.append((runtime.spread_key(symbol), json.dumps(history.export())))
        with runtime.store.db:
            runtime.store.db.executemany("INSERT OR REPLACE INTO kv VALUES(?,?)", updates)
        runtime.store.put(
            "quote_health",
            success_health(receipt)
            | {"event_ms": int(body["time"]), "symbols": len(updates), "status": "observed"},
        )

    async def quote_loop(self):
        runtime = self.runtime
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            delay = runtime.settings.quote_sample_seconds
            try:
                if not runtime.source_ready:
                    await asyncio.sleep(5)
                    continue
                body = await runtime.api.get("tickers", category="linear")
                runtime.record_quotes(body, now_ms())
            except Exception as exc:
                delay = min(15, delay)
                runtime.store.put(
                    "quote_health",
                    failure_health(runtime.store.get("quote_health", {}), exc, now_ms(), delay),
                )
            # Request/processing time belongs inside the sampling interval.
            # Sleeping a full interval afterwards steadily skips time buckets.
            # Overruns do not fabricate observations or replay missed samples.
            await asyncio.sleep(
                delay
                if delay < runtime.settings.quote_sample_seconds
                else max(0, delay - (loop.time() - started))
            )

    async def source_watchdog(self):
        runtime = self.runtime
        failed_since = None
        degraded_since = None
        while True:
            await asyncio.sleep(30)
            from .feed_health import persistent_health

            persistent = persistent_health(runtime, now_ms())
            counts = persistent.get("primary_persistent_stream_health", {})
            if counts.get("total", 0) > counts.get("fresh", 0):
                degraded_since = degraded_since or now_ms()
            else:
                degraded_since = None
            # Native symbols reconnect independently. One quiet or stale symbol
            # must not destroy continuous history for the rest of the universe.
            source_feed_available = (
                any(runtime.streams.connected_for(s) for s in runtime.streams.selected)
                if isinstance(runtime.streams, NativeStreams)
                else runtime.streams.connected
            )
            runtime.store.put(
                "runtime_health",
                dict(
                    at_ms=now_ms(),
                    scanner_enabled=runtime.settings.scan_enabled,
                    scanner_state=runtime.status["state"],
                    recorder_healthy=runtime.recorder.healthy,
                    recorder_reason=runtime.recorder.reason,
                    source=runtime.exchange,
                    source_ready=runtime.source_ready,
                    selected=list(runtime.streams.selected),
                    feeds_degraded_since_ms=degraded_since,
                    streams_fresh=runtime.streams.connected
                    and all(runtime.flow_ready(s, now_ms()) for s in runtime.streams.selected)
                    and persistent["active_lifecycle_stream_health"]["fresh"]
                    == persistent["active_lifecycle_stream_health"]["total"],
                    source_feed_available=source_feed_available,
                    last_market_event_ms=max(
                        (t.last_event for t in runtime.streams.tapes.values()), default=0
                    ),
                    last_recorder_write_ms=getattr(runtime.recorder, "last_success_ms", None),
                    **persistent,
                ),
            )
            if not runtime.source_ready or not runtime.streams.selected or source_feed_available:
                failed_since = None
                continue
            failed_since = failed_since or now_ms()
            if now_ms() - failed_since >= 120_000:
                # Recovery must not stop the independent health heartbeat
                # while a broad scan owns the source/selection lock.
                if runtime.scan_lock.locked():
                    continue
                async with runtime.scan_lock:
                    # Recover current source first. Active plans never change venue.
                    previous = runtime.exchange
                    runtime.streams.required_symbols = set(runtime.pending_symbols())
                    await runtime.streams.select(runtime.pending_symbols(), retain_selected=True)
                    if runtime.settings.market_source in {"auto", "multi"}:
                        runtime.source_ready = False
                        try:
                            # New scans may use a healthy venue while original
                            # plans keep their own peer and historical recovery.
                            await runtime.select_source()
                        except ConnectionError:
                            runtime.source_ready = True
                    runtime.status.update(
                        state="reconnecting" if runtime.exchange == previous else "warming",
                        exchange=runtime.exchange,
                        reason="source recovery; original setup venues and plans retained",
                        at_ms=now_ms(),
                    )
                    runtime.store.put("scanner", runtime.status)
                failed_since = None

    async def clock_loop(self):
        runtime = self.runtime
        """Refresh operator diagnostics independently of long scans and REST queues."""
        while True:
            source = runtime.exchange
            api = VenueAPI(source, runtime.settings)
            delay = 60
            try:
                started = now_ms()
                async with asyncio.timeout(5):
                    body = await api.get("time")
                received = now_ms()
                if source == runtime.exchange:
                    exchange_time = int(body["time"])
                    runtime.store.put(
                        "clock_health",
                        dict(
                            source=source,
                            at_ms=received,
                            skew_ms=(started + received) // 2 - exchange_time,
                            round_trip_ms=received - started,
                            receipt_offset_ms=received - exchange_time,
                            method="request midpoint minus exchange time; warning only",
                        ),
                    )
                    runtime.store.put("clock_check_health", success_health(received) | dict(source=source))
            except Exception as exc:
                delay = 15
                runtime.store.put(
                    "clock_check_health",
                    failure_health(runtime.store.get("clock_check_health", {}), exc, now_ms(), delay)
                    | dict(source=source),
                )
            finally:
                await api.close()
            await asyncio.sleep(delay)
