import asyncio
import contextlib
import logging

from .exchanges import VenueAPI
from .liquidity import SpreadHistory
from .native_streams import NativeStreams
from .notifications import Notifier
from .storage import now_ms
from .streams import Streams

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class Scanner:
    def __init__(self, settings, store, recorder):
        self.settings, self.store, self.recorder = settings, store, recorder
        from .v2 import initialize

        self.v2_cutover = initialize(store, now_ms())
        name = settings.market_source if settings.market_source not in {"auto", "multi"} else "bybit"
        self.api = VenueAPI(name, settings, recorder)
        self.streams = (
            Streams(settings, store, recorder)
            if name == "bybit"
            else NativeStreams(settings, store, recorder, self.api)
        )
        self.source_ready = settings.market_source not in {"auto", "multi"}
        self.notifier = Notifier(settings, store)
        from .setup_runtime import Runtime as SetupRuntime

        self.setup_runtime = SetupRuntime(self)
        self.notifier.prepare_initial = self.setup_runtime.prepare_delivery
        self.notifier.finalize_initial = self.setup_runtime.finalize_delivery
        self.notifier.guard_initial = self.setup_runtime.dispatch_guard
        from .scanner_market_data import MarketDataCoordinator

        self.market_data = MarketDataCoordinator(self)
        from .scanner_discovery import MarketDiscoveryCoordinator

        self.discovery = MarketDiscoveryCoordinator(self)
        from .scanner_confirmation import ConfirmationEngine

        self.confirmation = ConfirmationEngine(self)
        from .scanner_lifecycle import LifecycleCoordinator

        self.lifecycle = LifecycleCoordinator(self)
        from .scanner_research import ResearchScheduler

        self.research = ResearchScheduler(self)
        from .scanner_maintenance import MaintenanceCoordinator

        self.maintenance = MaintenanceCoordinator(self)
        self.context = {}
        self.reconcile_pending = {s["id"] for s in store.active_signals() if s["source"] != "tradingview"}
        self.v8_cache = {}
        self.v8_disabled = {}
        self.oi_series = {}
        from .current_oi import Collector as OICollector

        self.current_oi_collector = OICollector(self)
        from .spot_perp import Collector

        self.spot_collector = Collector(self.optional_rest)
        self.liquidation_baseline = {}
        self.tasks = []
        self.scan_lock = asyncio.Lock()
        self.lifecycle_bootstrapped = asyncio.Event()
        from .cross_venue import CrossVenue

        self.cross_venue = CrossVenue(self)
        self.status = {"state": "disabled", "at_ms": now_ms(), "eligible": 0, "errors": 0}

    def data_owner(self):
        """Explicit compatibility boundary for component-owned market-data state."""
        from .scanner_market_data import MarketDataCoordinator

        if getattr(self, "market_data", None) is None:
            self.market_data = MarketDataCoordinator(self)
        return self.market_data

    @property
    def candle_cache(self):
        return self.data_owner().candle_cache

    @candle_cache.setter
    def candle_cache(self, value):
        self.data_owner().candle_cache = value

    @property
    def candle_cache_limits(self):
        return self.data_owner().candle_cache_limits

    @candle_cache_limits.setter
    def candle_cache_limits(self, value):
        self.data_owner().candle_cache_limits = value

    @property
    def feature_cache(self):
        return self.data_owner().feature_cache

    @feature_cache.setter
    def feature_cache(self, value):
        self.data_owner().feature_cache = value

    @property
    def feature_cache_hits(self):
        return self.data_owner().feature_cache_hits

    @feature_cache_hits.setter
    def feature_cache_hits(self, value):
        self.data_owner().feature_cache_hits = value

    @property
    def feature_cache_misses(self):
        return self.data_owner().feature_cache_misses

    @feature_cache_misses.setter
    def feature_cache_misses(self, value):
        self.data_owner().feature_cache_misses = value

    @property
    def derivative_cache(self):
        return self.data_owner().derivative_cache

    @derivative_cache.setter
    def derivative_cache(self, value):
        self.data_owner().derivative_cache = value

    @property
    def derivative_cache_hits(self):
        return self.data_owner().derivative_cache_hits

    @derivative_cache_hits.setter
    def derivative_cache_hits(self, value):
        self.data_owner().derivative_cache_hits = value

    @property
    def derivative_cache_misses(self):
        return self.data_owner().derivative_cache_misses

    @derivative_cache_misses.setter
    def derivative_cache_misses(self, value):
        self.data_owner().derivative_cache_misses = value

    @property
    def rest_semaphore(self):
        return self.data_owner().rest_semaphore

    @rest_semaphore.setter
    def rest_semaphore(self, value):
        self.data_owner().rest_semaphore = value

    @property
    def rest_reduced_semaphore(self):
        return self.data_owner().rest_reduced_semaphore

    @rest_reduced_semaphore.setter
    def rest_reduced_semaphore(self, value):
        self.data_owner().rest_reduced_semaphore = value

    @property
    def rest_reduced_until(self):
        return self.data_owner().rest_reduced_until

    @rest_reduced_until.setter
    def rest_reduced_until(self, value):
        self.data_owner().rest_reduced_until = value

    @property
    def rest_inflight(self):
        return self.data_owner().rest_inflight

    @rest_inflight.setter
    def rest_inflight(self, value):
        self.data_owner().rest_inflight = value

    @property
    def rest_queue_depth(self):
        return self.data_owner().rest_queue_depth

    @rest_queue_depth.setter
    def rest_queue_depth(self, value):
        self.data_owner().rest_queue_depth = value

    @property
    def exchange(self):
        return getattr(self.api, "name", "bybit")

    def spread_key(self, symbol):
        if self.settings.spread_bucket_seconds != 300 or self.settings.spread_window_minutes != 360:
            return f"spreads:{self.settings.spread_bucket_seconds}s:{self.settings.spread_window_minutes}m:{self.exchange}:{symbol}"
        return "spreads:" + ("" if self.exchange == "bybit" else self.exchange + ":") + symbol

    def spread_history(self, symbol):
        return SpreadHistory(
            self.store.get(self.spread_key(symbol), []),
            bucket_ms=self.settings.spread_bucket_seconds * 1000,
            window_ms=self.settings.spread_window_minutes * 60_000,
        )

    def market_context(self, now):
        """Never score cached BTC/ETH context from a prior venue or future receipt."""
        result = {}
        for symbol in ("BTCUSDT", "ETHUSDT"):
            row = self.store.get("market:" + symbol, {})
            valid = row.get("exchange") == self.exchange and 0 <= now - row.get("asof", 0) < 960_000
            result[symbol] = row.get("h4", {}).get("regime", "unavailable") if valid else "unavailable"
        return result

    async def select_source(self):
        from .scanner_market_data import MarketDataCoordinator

        return await (getattr(self, "market_data", None) or MarketDataCoordinator(self)).select_source()

    async def cached_candles(self, symbol, interval, asof, limit):
        from .scanner_market_data import MarketDataCoordinator

        return await (getattr(self, "market_data", None) or MarketDataCoordinator(self)).cached_candles(
            symbol, interval, asof, limit
        )

    async def bounded_rest(self, awaitable):
        from .db_runtime import measure_await
        from .scanner_market_data import MarketDataCoordinator

        return await measure_await(
            getattr(self, "store", None),
            (getattr(self, "market_data", None) or MarketDataCoordinator(self)).bounded_rest(awaitable),
            "rest_latency_ms",
        )

    async def optional_rest(self, awaitable):
        from .scanner_market_data import MarketDataCoordinator

        return await (getattr(self, "market_data", None) or MarketDataCoordinator(self)).optional_rest(
            awaitable
        )

    def cached_features(self, symbol, timeframe, bars, asof):
        from .scanner_market_data import MarketDataCoordinator

        return (getattr(self, "market_data", None) or MarketDataCoordinator(self)).cached_features(
            symbol, timeframe, bars, asof
        )

    async def cached_derivative_history(self, endpoint, symbol, start, end, ttl_ms):
        from .scanner_market_data import MarketDataCoordinator

        return await (
            getattr(self, "market_data", None) or MarketDataCoordinator(self)
        ).cached_derivative_history(endpoint, symbol, start, end, ttl_ms)

    def pending_symbols(self):
        return list(
            dict.fromkeys(s["symbol"] for s in self.store.active_signals() if s["source"] == self.exchange)
        )

    async def refresh_once(self):
        if not self.recorder.healthy:
            return
        # Selected feeds must keep generating minutes even after the prior one expires.
        symbols = list(dict.fromkeys(self.pending_symbols() + list(self.streams.selected)))
        refreshed, errors = 0, 0
        for symbol in symbols:
            c = self.context.get(symbol)
            if not c:
                continue
            try:
                now = now_ms()
                h4 = await self.cached_candles(symbol, "240", now, 160)
                h1 = await self.cached_candles(symbol, "60", now, 200)
                m15 = await self.cached_candles(symbol, "15", now, 120)
                # History I/O can yield to live receipts; readiness uses availability.
                now = now_ms()
                self.context[symbol] = c | {"h4": h4, "h1": h1, "m15": m15, "asof": now}
                if self.settings.execution_window_seconds < 900:
                    self.save_candidates(c["instrument"], h4, h1, m15, now)
                await self.discover_horizons(c["instrument"], now)
                refreshed += 1
            except Exception as exc:
                # Keep the last observed context; evaluate still enforces its age.
                errors += 1
                log.warning("refresh_failed", extra={"symbol": symbol, "error_type": type(exc).__name__})
        self.store.put("refresh_health", dict(at_ms=now_ms(), refreshed=refreshed, errors=errors))

    async def refresh_pending(self):
        while True:
            try:
                await self.refresh_once()
            except Exception as exc:
                self.store.put("refresh_health", dict(at_ms=now_ms(), error_type=type(exc).__name__))
            await asyncio.sleep(30)

    def generation_profile(self, instrument, bars, now):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).generation_profile(
            instrument, bars, now
        )

    async def profile_loop(self):
        from .scanner_research import ResearchScheduler

        return await (getattr(self, "research", None) or ResearchScheduler(self)).profile_loop()

    async def volume_research(self, instrument, timeframe, bars, asof):
        from .scanner_research import ResearchScheduler

        return await (getattr(self, "research", None) or ResearchScheduler(self)).volume_research(
            instrument, timeframe, bars, asof
        )

    def observe_generation(self, signal, instrument, bars, now, initial_book=None):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).observe_generation(
            signal, instrument, bars, now, initial_book
        )

    def observe_duplicate(self, signal, reason):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).observe_duplicate(
            signal, reason
        )

    def save_candidates(
        self, instrument, context_bars, setup_bars, execution_bars, evaluated, initial_book=None
    ):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).save_candidates(
            instrument, context_bars, setup_bars, execution_bars, evaluated, initial_book
        )

    async def discover_horizons(self, instrument, now):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return await (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).discover_horizons(
            instrument, now
        )

    def remember_structures(self, instrument, horizon, observations, now):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).remember_structures(
            instrument, horizon, observations, now
        )

    def flow_ready(self, symbol, now):
        from .scanner_confirmation import ConfirmationEngine

        return (getattr(self, "confirmation", None) or ConfirmationEngine(self)).flow_ready(symbol, now)

    def entry_ready(self, signal, book, now):
        from .scanner_confirmation import ConfirmationEngine

        return (getattr(self, "confirmation", None) or ConfirmationEngine(self)).entry_ready(
            signal, book, now
        )

    def apply_trade_quality(self, signal, book, original_now):
        from .scanner_confirmation import ConfirmationEngine

        return (getattr(self, "confirmation", None) or ConfirmationEngine(self)).apply_trade_quality(
            signal, book, original_now
        )

    def record_membership(self, evaluated_ms, symbol, eligible, payload):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).record_membership(
            evaluated_ms, symbol, eligible, payload
        )

    def record_confirmation_book_gap(self, signal, now, start, end):
        from .scanner_confirmation import ConfirmationEngine

        return (getattr(self, "confirmation", None) or ConfirmationEngine(self)).record_confirmation_book_gap(
            signal, now, start, end
        )

    def record_quotes(self, body, receipt):
        from .scanner_market_data import MarketDataCoordinator

        return (getattr(self, "market_data", None) or MarketDataCoordinator(self)).record_quotes(
            body, receipt
        )

    async def quote_loop(self):
        from .scanner_market_data import MarketDataCoordinator

        return await (getattr(self, "market_data", None) or MarketDataCoordinator(self)).quote_loop()

    async def source_watchdog(self):
        from .scanner_market_data import MarketDataCoordinator

        return await (getattr(self, "market_data", None) or MarketDataCoordinator(self)).source_watchdog()

    async def depth_context(self, inst, ticker, ticker_time, evaluated):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return await (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).depth_context(
            inst, ticker, ticker_time, evaluated
        )

    async def clock_loop(self):
        from .scanner_market_data import MarketDataCoordinator

        return await (getattr(self, "market_data", None) or MarketDataCoordinator(self)).clock_loop()

    async def scan_once(self):
        from .db_runtime import measure_await
        from .scanner_discovery import MarketDiscoveryCoordinator

        return await measure_await(
            self.store,
            (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).scan_once(),
            "scan_duration_ms",
        )

    async def monitor_alerted(self, s, now):
        from .scanner_lifecycle import LifecycleCoordinator

        return await (getattr(self, "lifecycle", None) or LifecycleCoordinator(self)).monitor_alerted(s, now)

    async def health_loop(self):
        from .scanner_lifecycle import LifecycleCoordinator

        return await (getattr(self, "lifecycle", None) or LifecycleCoordinator(self)).health_loop()

    async def evaluate(self):
        from .db_runtime import measure_await
        from .scanner_confirmation import ConfirmationEngine

        return await measure_await(
            self.store,
            (getattr(self, "confirmation", None) or ConfirmationEngine(self)).evaluate(),
            "confirmation_duration_ms",
        )

    async def scan_loop(self):
        from .scanner_discovery import MarketDiscoveryCoordinator

        return await (getattr(self, "discovery", None) or MarketDiscoveryCoordinator(self)).scan_loop()

    async def evaluation_loop(self):
        from .scanner_confirmation import ConfirmationEngine

        return await (getattr(self, "confirmation", None) or ConfirmationEngine(self)).evaluation_loop()

    async def research_loop(self):
        from .scanner_research import ResearchScheduler

        return await (getattr(self, "research", None) or ResearchScheduler(self)).research_loop()

    async def v8_loop(self):
        from .scanner_research import ResearchScheduler

        return await (getattr(self, "research", None) or ResearchScheduler(self)).v8_loop()

    async def context_ev_loop(self):
        from .scanner_research import ResearchScheduler

        return await (getattr(self, "research", None) or ResearchScheduler(self)).context_ev_loop()

    async def storage_loop(self):
        from .scanner_maintenance import MaintenanceCoordinator

        return await (getattr(self, "maintenance", None) or MaintenanceCoordinator(self)).storage_loop()

    def start(self):
        from .supervision import supervise

        self.store.put("scanner_process_start", dict(at_ms=now_ms()))
        self.store.put(
            "funnel_process_baseline",
            dict(self.store.db.execute("SELECT metric,n FROM funnel_totals WHERE dimension='all'")),
        )

        def start_task(name, factory):
            self.tasks.append(asyncio.create_task(supervise(self.store, name, factory)))

        self.tasks = []
        from .lifecycle import reconcile_loop, run

        start_task("lifecycle", lambda: run(self))
        start_task("reconciliation", lambda: reconcile_loop(self))
        start_task("confirmation", self.evaluation_loop)
        start_task("thesis_health", self.health_loop)
        start_task("terminal_delivery", self.notifier.terminal_loop)
        start_task("post_terminal", self.research_loop)
        start_task("v8_context_ev", self.context_ev_loop)
        start_task("v8_research", self.v8_loop)
        start_task("storage", self.storage_loop)
        from .operational import monitor as monitor_operations

        start_task("operations", lambda: monitor_operations(self))
        from .discord_health import monitor as monitor_discord

        start_task("discord_health", lambda: monitor_discord(self.settings, self.store))
        if self.settings.scan_enabled:
            start_task("clock", self.clock_loop)
            if self.settings.macro_news_enabled:
                from .macro import run as macro_run

                start_task("macro", lambda: macro_run(self))
            start_task("scanner", self.scan_loop)
            start_task("quotes", self.quote_loop)
            start_task("volume_profiles", self.profile_loop)
            start_task("pending", self.refresh_pending)
            start_task("source", self.source_watchdog)
            start_task("cross_venue", self.cross_venue.run)

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.streams.stop()
        await self.api.close()
        await self.spot_collector.close()
