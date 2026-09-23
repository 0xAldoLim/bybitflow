import asyncio
import contextlib
import json
import logging
from statistics import median

from .exchanges import VenueAPI, market_probe
from .features import candle_features, validate_bars
from .fundamentals import facts_asof
from .horizons import DURATIONS, PROFILES, assign
from .ingestion import candle_records, parse_eligible_metadata
from .liquidity import SpreadHistory
from .models import DAY, Signal
from .native_streams import NativeStreams
from .notifications import Notifier
from .orderflow import Book, footprint
from .risk import evaluate_risk
from .scoring import score
from .storage import now_ms
from .strategy import candidates, confirm
from .streams import Streams

log = logging.getLogger(__name__)
TERMINAL = {"INVALIDATED", "EXPIRED", "RESOLVED"}


class Scanner:
    def __init__(self, settings, store, recorder):
        self.settings, self.store, self.recorder = settings, store, recorder
        name = settings.market_source if settings.market_source not in {"auto", "multi"} else "bybit"
        self.api = VenueAPI(name, settings, recorder)
        self.streams = (
            Streams(settings, store, recorder)
            if name == "bybit"
            else NativeStreams(settings, store, recorder, self.api)
        )
        self.source_ready = settings.market_source not in {"auto", "multi"}
        self.notifier = Notifier(settings, store)
        self.context = {}
        self.reconcile_pending = {s["id"] for s in store.active_signals() if s["source"] != "tradingview"}
        self.candle_cache = {}
        self.tasks = []
        self.scan_lock = asyncio.Lock()
        self.lifecycle_bootstrapped = asyncio.Event()
        from .cross_venue import CrossVenue

        self.cross_venue = CrossVenue(self)
        self.status = {"state": "disabled", "at_ms": now_ms(), "eligible": 0, "errors": 0}

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
        if self.source_ready or not isinstance(self.api, VenueAPI):
            return
        # Stable primary until failure, deterministic preference; no round-robin tape mixing.
        for name in ("binance", "bybit", "okx"):
            report = await market_probe(name, self.settings)
            self.store.put("probe:" + name, report)
            if report["status"] != "HEALTHY":
                continue
            previous = self.exchange
            if name == previous:
                self.source_ready = True
                return
            if self.pending_symbols():
                self.cross_venue.peers[previous] = (self.api, self.streams)
            else:
                await self.streams.stop()
                await self.api.close()
            self.api, self.streams = self.cross_venue.ensure_peer(name)
            self.cross_venue.peers.pop(name)
            job = self.cross_venue.jobs.pop(name, None)
            if job:
                job.cancel()
            self.context.clear()
            self.candle_cache.clear()
            self.source_ready = True
            change = dict(
                previous=previous,
                current=name,
                at_ms=now_ms(),
                reason="verified REST and trade WS probe; fresh book/tape warmup required",
            )
            self.store.put("active_exchange", change)
            self.recorder.offer("control/source_change", "ALL", now_ms(), change, complete=False)
            return
        raise ConnectionError("No compatible public exchange passed REST and trade WS checks")

    async def cached_candles(self, symbol, interval, asof, limit):
        duration = DURATIONS[interval]
        key, boundary = (symbol, interval), asof // duration
        cached = self.candle_cache.get(key)
        if cached and cached[0] == boundary and len(cached[1]) >= limit:
            return cached[1]
        bars = await self.api.candles(symbol, interval, asof, limit=limit)
        validate_bars(bars, asof)
        self.candle_cache[key] = (boundary, bars)
        return bars

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
        cache = "swing_profiles" if bars and bars[-1].interval >= 14400000 else "volume_profiles"
        profile = getattr(self, cache, {}).get(instrument.symbol, {})
        if profile.get("source") == self.exchange and 0 <= now - profile.get("available_ms", 0) <= 120000:
            return profile
        return {"available": False, "reason": "No fresh complete cached executed-volume profile"}

    async def profile_loop(self):
        from .orderflow import Tape
        from .profiles import build

        self.volume_profiles = {}
        self.swing_profiles = {}
        while True:
            if self.source_ready and self.recorder.healthy:
                for symbol, context in list(self.context.items()):
                    tape = self.streams.tapes.get(symbol)
                    if tape is None:
                        continue
                    now = now_ms()
                    end = now // 60000 * 60000
                    snapshot = Tape()
                    snapshot.trades = list(tape.trades)
                    snapshot.coverage_start = tape.coverage_start
                    snapshot.last_event = tape.last_event
                    inst = context["instrument"]
                    book = self.streams.books.get(symbol)
                    book_features = (
                        book.features(now) if book and book.fresh(now, self.settings.book_stale_ms) else {}
                    )
                    atr = candle_features(context["h1"], now)["atr"]
                    result = await asyncio.to_thread(
                        build,
                        snapshot,
                        inst.tick,
                        atr,
                        end - 1800000,
                        end,
                        now,
                        self.exchange,
                        symbol,
                        self.volume_profiles.get(symbol),
                        book_features,
                    )
                    result["available_ms"] = now_ms()
                    self.volume_profiles[symbol] = result
                    swing_atr = candle_features(context["h4"], now)["atr"]
                    swing = await asyncio.to_thread(
                        build,
                        snapshot,
                        inst.tick,
                        swing_atr,
                        end - 14400000,
                        end,
                        now,
                        self.exchange,
                        symbol,
                        self.swing_profiles.get(symbol),
                        book_features,
                    )
                    swing["available_ms"] = now_ms()
                    self.swing_profiles[symbol] = swing
                    # Baselines mature from every complete selected-market window,
                    # independent of whether the strategy generated a candidate.
                    window = self.settings.execution_window_seconds * 1000
                    start = end - window
                    if (
                        snapshot.coverage_start <= start
                        and snapshot.last_event >= end - 10000
                        and book
                        and book.fresh(now, self.settings.book_stale_ms)
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
                            flow.update(flow_response(trades, bf))
                            for horizon in self.settings.horizon_profiles:
                                identity = SimpleNamespace(
                                    source=self.exchange,
                                    symbol=symbol,
                                    horizon_profile=horizon,
                                    entry_session=session_context(now)["primary"],
                                )
                                key, history = flow_baseline(
                                    self.store, identity.source, symbol, identity.entry_session, horizon, end
                                )
                                quality = assess_flow_quality(
                                    trades,
                                    inst.tick,
                                    bf,
                                    flow,
                                    now,
                                    start,
                                    end,
                                    history,
                                    self.store.get("cross:" + symbol, {}),
                                )
                                flow["quality"] = quality
                                remember(self.store, key, history, quality)
                                session_baseline(self.store, identity, flow, bf, now, window_end_ms=end)
                    await asyncio.sleep(0.05)
                self.store.put(
                    "profile_health",
                    dict(
                        state="HEALTHY",
                        last_success_ms=now_ms(),
                        symbols=len(self.volume_profiles),
                        complete=sum(bool(p.get("available")) for p in self.volume_profiles.values()),
                    ),
                )
            await asyncio.sleep(30)

    def save_candidates(self, instrument, context_bars, setup_bars, execution_bars, evaluated):
        if not self.recorder.healthy:
            return []
        if not self.flow_ready(instrument.symbol, evaluated):
            return []
        plans = candidates(
            instrument,
            context_bars,
            setup_bars,
            execution_bars,
            evaluated,
            execution_window_ms=self.settings.execution_window_seconds * 1000,
            structural_targets=True,
            volume_profile=self.generation_profile(instrument, setup_bars, evaluated),
            horizon="CORE_INTRADAY",
        )
        for signal in plans:
            assign(signal, "CORE_INTRADAY")
            old = self.store.db.execute("SELECT 1 FROM signals WHERE id=?", (signal.id,)).fetchone()
            if not old and signal.expires_ms > evaluated:
                self.store.signal(signal)
                for reason in signal.gates:
                    from .funnel import reject

                    reject(self.store, signal, reason, signal.created_ms)
        return plans

    async def discover_horizons(self, instrument, now):
        """All profiles reuse one venue stream and a shared closed-candle cache."""
        if not self.flow_ready(instrument.symbol, now):
            return
        for name in self.settings.horizon_profiles:
            if name == "CORE_INTRADAY":
                continue
            profile = PROFILES[name]
            bars = [
                await self.cached_candles(instrument.symbol, tf, now, 120)
                for tf in (profile.context, profile.setup, profile.execution)
            ]
            plans = candidates(
                instrument,
                *bars,
                now,
                execution_window_ms=self.settings.execution_window_seconds * 1000,
                structural_targets=True,
                volume_profile=self.generation_profile(instrument, bars[1], now),
                horizon=name,
            )
            for signal in plans:
                assign(signal, name)
                signal.expires_ms = bars[1][-1].end + DURATIONS[profile.setup]
                signal.trigger_expires_ms = now + 120_000
                if not self.store.db.execute("SELECT 1 FROM signals WHERE id=?", (signal.id,)).fetchone():
                    self.store.signal(signal)
                    for reason in signal.gates:
                        from .funnel import reject

                        reject(self.store, signal, reason, signal.created_ms)

    def flow_ready(self, symbol, now):
        tape = self.streams.tapes.get(symbol)
        book = self.streams.books.get(symbol)
        start = now // 60000 * 60000 - self.settings.execution_window_seconds * 1000
        ready = bool(
            tape
            and book
            and book.fresh(now, self.settings.book_stale_ms)
            and 0 < tape.coverage_start <= start
            and 0 <= now - tape.last_receipt <= self.settings.trade_stale_ms
        )
        self.store.put(
            f"flow_warmup:{self.exchange}:{symbol}",
            dict(
                ready=ready,
                coverage_start_ms=tape.coverage_start if tape else None,
                tape_last_event_ms=tape.last_event if tape else None,
                book_last_event_ms=book.event_ms if book else None,
                required_window_ms=self.settings.execution_window_seconds * 1000,
            ),
        )
        return ready

    def entry_ready(self, signal, book, now):
        from .production import entry_position

        if not book.fresh(now, self.settings.book_stale_ms):
            return False
        price = float(min(book.asks) if signal.direction == "LONG" else max(book.bids))
        signal.coverage["entry_position"] = entry_position(
            signal, price, book.features(now).get("spread_bps", 0)
        )
        return signal.coverage["entry_position"]["state"] in {"IDEAL_ENTRY", "ACCEPTABLE_ENTRY"}

    def record_membership(self, evaluated_ms, symbol, eligible, payload):
        available = now_ms()
        payload = payload | {
            "evaluated_ms": evaluated_ms,
            "available_ms": available,
            "exchange": self.exchange,
        }
        self.store.membership(available, symbol, eligible, payload)
        self.recorder.offer(
            "universe/membership",
            symbol,
            available,
            {"exchange": self.exchange, "eligible": eligible, "details": payload},
            available,
        )

    def record_quotes(self, body, receipt):
        updates = []
        for ticker in body["result"]["list"]:
            symbol = ticker["symbol"]
            if symbol in self.context:
                d = self.context[symbol]["derivatives"]
                if ticker.get("fundingRate") is not None:
                    d.update(
                        funding_rate=float(ticker["fundingRate"]),
                        ticker_observed_ms=int(
                            ticker.get("funding_observed_ms", ticker.get("observed_ms", body["time"]))
                        ),
                    )
            history = self.spread_history(symbol)
            history.add(
                int(ticker.get("observed_ms", body["time"])),
                receipt,
                float(ticker.get("bid1Price") or 0),
                float(ticker.get("ask1Price") or 0),
            )
            updates.append((self.spread_key(symbol), json.dumps(history.export())))
        with self.store.db:
            self.store.db.executemany("INSERT OR REPLACE INTO kv VALUES(?,?)", updates)
        self.store.put(
            "quote_health",
            {"at_ms": receipt, "event_ms": int(body["time"]), "symbols": len(updates), "status": "observed"},
        )

    async def quote_loop(self):
        while True:
            try:
                if not self.source_ready:
                    await asyncio.sleep(5)
                    continue
                body = await self.api.get("tickers", category="linear")
                self.record_quotes(body, now_ms())
            except Exception as exc:
                self.store.put("quote_health", {"at_ms": now_ms(), "error_type": type(exc).__name__})
            await asyncio.sleep(self.settings.quote_sample_seconds)

    async def source_watchdog(self):
        failed_since = None
        while True:
            await asyncio.sleep(30)
            from .feed_health import persistent_health

            persistent = persistent_health(self, now_ms())
            # Native symbols reconnect independently. One quiet or stale symbol
            # must not destroy continuous history for the rest of the universe.
            source_feed_available = (
                any(self.streams.connected_for(s) for s in self.streams.selected)
                if isinstance(self.streams, NativeStreams)
                else self.streams.connected
            )
            self.store.put(
                "runtime_health",
                dict(
                    at_ms=now_ms(),
                    scanner_enabled=self.settings.scan_enabled,
                    scanner_state=self.status["state"],
                    recorder_healthy=self.recorder.healthy,
                    recorder_reason=self.recorder.reason,
                    source=self.exchange,
                    source_ready=self.source_ready,
                    selected=list(self.streams.selected),
                    streams_fresh=self.streams.connected
                    and all(self.flow_ready(s, now_ms()) for s in self.streams.selected),
                    source_feed_available=source_feed_available,
                    last_market_event_ms=max((t.last_event for t in self.streams.tapes.values()), default=0),
                    last_recorder_write_ms=getattr(self.recorder, "last_success_ms", None),
                    **persistent,
                ),
            )
            if not self.source_ready or not self.streams.selected or source_feed_available:
                failed_since = None
                continue
            failed_since = failed_since or now_ms()
            if now_ms() - failed_since >= 120_000:
                async with self.scan_lock:
                    # Recover current source first. Active plans never change venue.
                    self.streams.required_symbols = set(self.pending_symbols())
                    await self.streams.select(list(self.streams.selected) + self.pending_symbols())
                    self.status.update(
                        state="reconnecting",
                        reason="original-source stream recovery in progress",
                        at_ms=now_ms(),
                    )
                    self.store.put("scanner", self.status)
                failed_since = None

    async def depth_context(self, inst, ticker, ticker_time, evaluated):
        t = ticker
        oi = await self.api.history("open-interest", inst.symbol, evaluated - 4 * 3_600_000, evaluated)
        funding = await self.api.history("funding/history", inst.symbol, evaluated - 2 * DAY, evaluated)
        if self.exchange == "okx":
            current_funding = await self.api.funding_now(inst.symbol)
            t = t | {
                "fundingRate": current_funding.get("fundingRate"),
                "nextFundingTime": current_funding.get("nextFundingTime"),
                "observed_ms": int(current_funding["ts"]),
            }
        oi = sorted(oi, key=lambda x: int(x["timestamp"]))
        f = sorted(funding, key=lambda x: int(x["fundingRateTimestamp"]))
        # Current predicted funding is context only, never injected into historical decisions.
        derivatives = {
            "exchange": self.exchange,
            "funding_rate": float(t["fundingRate"]) if t.get("fundingRate") else None,
            "next_funding_ms": int(t.get("nextFundingTime") or 0),
            "funding_interval_minutes": inst.funding_interval_minutes,
            "funding_history": f,
            "oi_history": oi,
            "oi_change_pct": (float(oi[-1]["openInterest"]) / float(oi[0]["openInterest"]) - 1) * 100
            if len(oi) >= 2 and float(oi[0]["openInterest"])
            else None,
            "oi_unit": "base coin, API openInterest field; see audited definition",
            "oi_notional": float(t["openInterestValue"])
            if t.get("openInterestValue")
            else (float(oi[-1]["notional"]) if oi and oi[-1].get("notional") else None),
            "mark": float(t["markPrice"]) if t.get("markPrice") else None,
            "index": float(t["indexPrice"]) if t.get("indexPrice") else None,
            "ticker_observed_ms": int(t.get("funding_observed_ms", t.get("observed_ms", ticker_time))),
            "collected_ms": now_ms(),
        }
        return derivatives

    async def scan_once(self):
        from .discovery import depth_shortlist, horizon_pre_ranks

        async with self.scan_lock:
            await self.select_source()
            self.status.update(state="scanning", started_ms=now_ms())
            self.store.put("scanner", self.status)
            body = await self.api.get("time")
            asof = int(body["time"])
            if abs(asof - now_ms()) > 2000:
                raise ValueError("Local/exchange clock skew exceeds two seconds")
            rows = await self.api.instruments()
            pinned = self.pending_symbols()
            priority = list(dict.fromkeys(pinned + self.settings.core_watchlist))
            rows.sort(
                key=lambda r: (
                    priority.index(r["symbol"]) if r["symbol"] in priority else len(priority),
                    r["symbol"],
                )
            )
            ticker_body = await self.api.get("tickers", category="linear")
            self.record_quotes(ticker_body, now_ms())
            tickers = {t["symbol"]: t for t in ticker_body["result"]["list"]}
            ranked, broad, errors = [], {}, 0
            verified = []
            requests = 0

            async def admit(symbol):
                nonlocal requests, errors
                if symbol in verified:
                    return
                try:
                    inst, context_bars, setup_bars, execution_bars = broad[symbol]
                    requests += 1
                    data = await self.api.get("orderbook", category="linear", symbol=symbol, limit=50)
                    book = Book()
                    book.apply(
                        dict(
                            type="snapshot",
                            ts=data["time"],
                            cts=data["result"].get("cts", data["time"]),
                            data=data["result"],
                        ),
                        now_ms(),
                    )
                    if not book.fresh(now_ms(), self.settings.book_stale_ms) or not all(
                        book.impact(side, self.settings.hypothetical_notional) for side in ("LONG", "SHORT")
                    ):
                        return
                    evaluated = now_ms()
                    derivatives = await self.depth_context(
                        inst, tickers[symbol], ticker_body["time"], evaluated
                    )
                    self.context[symbol] = dict(
                        instrument=inst,
                        h4=context_bars,
                        h1=setup_bars,
                        m15=execution_bars,
                        derivatives=derivatives,
                        asof=evaluated,
                    )
                    normal = self.spread_history(symbol).assess(
                        evaluated, self.settings.max_spread_bps, self.settings.spread_min_samples
                    )
                    self.record_membership(
                        evaluated,
                        symbol,
                        normal["eligible"],
                        dict(depth_verified=True, normal_spread=normal, metadata=inst.metadata),
                    )
                    self.save_candidates(inst, context_bars, setup_bars, execution_bars, evaluated)
                    await self.discover_horizons(inst, evaluated)
                    self.store.put(
                        "market:" + symbol,
                        dict(
                            exchange=self.exchange,
                            instrument=inst.model_dump(mode="json"),
                            candles=candle_records(setup_bars),
                            h4=candle_features(context_bars, evaluated),
                            h1=candle_features(setup_bars, evaluated),
                            derivatives=derivatives,
                            normal_spread=normal,
                            asof=evaluated,
                        ),
                    )
                    verified.append(symbol)
                    if symbol in priority:
                        await self.streams.select(list(dict.fromkeys(list(self.streams.selected) + [symbol])))
                except PermissionError:
                    raise
                except Exception as exc:
                    if not self.recorder.healthy:
                        raise RuntimeError("Recording unavailable; depth verification aborted") from exc
                    errors += 1

            # Restore existing opportunities before broad eligibility ranking. Admission
            # verifies depth; it never changes their original plan or lifecycle state.
            for raw in rows:
                if raw["symbol"] not in pinned:
                    continue
                try:
                    inst = (
                        self.api.parse(raw, asof)
                        if isinstance(self.api, VenueAPI)
                        else parse_eligible_metadata(raw, self.settings, asof)
                    )
                    if inst is None:
                        continue
                    now = now_ms()
                    bars = [
                        await self.cached_candles(inst.symbol, tf, now, count)
                        for tf, count in (("240", 160), ("60", 200), ("15", 120))
                    ]
                    broad[inst.symbol] = (inst, *bars)
                    await admit(inst.symbol)
                except Exception:
                    errors += 1
            for raw in rows:
                inst = (
                    self.api.parse(raw, asof)
                    if isinstance(self.api, VenueAPI)
                    else parse_eligible_metadata(raw, self.settings, asof)
                )
                if not inst:
                    continue
                try:
                    t = tickers.get(inst.symbol, {})
                    bid, ask = float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
                    spread = (ask - bid) / ((ask + bid) / 2) * 10000 if ask > bid > 0 else float("inf")
                    if spread > self.settings.max_spread_bps:
                        continue
                    now = now_ms()
                    daily = await self.cached_candles(inst.symbol, "D", now, 120)
                    if (
                        len(daily) < 30
                        or now - daily[-1].end >= DAY
                        or any(c.volume <= 0 for c in daily[-30:])
                    ):
                        continue
                    turnover = median(c.turnover for c in daily[-7:])
                    if turnover < self.settings.min_daily_turnover:
                        continue
                    context_bars = await self.cached_candles(inst.symbol, "240", now, 160)
                    setup_bars = await self.cached_candles(inst.symbol, "60", now, 200)
                    execution_bars = await self.cached_candles(inst.symbol, "15", now, 120)
                    if now - execution_bars[-1].end > 960_000 or now - setup_bars[-1].end > 3_660_000:
                        continue
                    pre = horizon_pre_ranks(
                        daily,
                        context_bars,
                        setup_bars,
                        execution_bars,
                        now,
                        spread,
                        self.settings.max_spread_bps,
                        t.get("openInterestValue") is not None,
                    )
                    normal = self.spread_history(inst.symbol).assess(
                        now, self.settings.max_spread_bps, self.settings.spread_min_samples
                    )
                    ranked.append(
                        dict(
                            symbol=inst.symbol,
                            exchange=self.exchange,
                            eligible=True,
                            normal_spread=normal,
                            turnover_7d=turnover,
                            spread_bps=spread,
                            asof=now,
                            **pre,
                        )
                    )
                    broad[inst.symbol] = (inst, context_bars, setup_bars, execution_bars)
                    if inst.symbol in priority:
                        await admit(inst.symbol)
                except PermissionError:
                    raise
                except Exception as exc:
                    if not self.recorder.healthy:
                        raise RuntimeError("Recording unavailable; broad scan aborted") from exc
                    errors += 1
            ranked.sort(key=lambda r: r["best_pre_rank"], reverse=True)
            available = {r["symbol"] for r in ranked}
            core = [x for x in self.settings.core_watchlist if x in available]
            capacity = self.settings.deep_symbols
            if (
                getattr(self.recorder, "state", "NORMAL") != "NORMAL"
                or self.store.get("storage_maintenance", {}).get("state") == "STORAGE_BACKPRESSURE"
            ):
                capacity = max(len(pinned), len(core), capacity // 2)
            shortlist, selection = depth_shortlist(
                ranked,
                core,
                [x for x in pinned if x in available],
                capacity,
                self.settings.stage_a_depth_candidates,
                now_ms() // 900_000,
                self.settings.exploration_fraction,
            )
            for symbol in shortlist:
                if len(verified) >= max(capacity, len([x for x in priority if x in available])):
                    break
                await admit(symbol)
            retained = [x for x in pinned if x in self.streams.selected]
            selected = list(dict.fromkeys(retained + [x for x in shortlist if x in verified]))[
                : max(capacity, len(retained))
            ]
            for symbol in selected:
                selection.setdefault(
                    symbol,
                    dict(
                        selection_reason="ACTIVE"
                        if symbol in pinned
                        else "CORE"
                        if symbol in core
                        else "EXPLOIT",
                        selection_probability=1.0,
                        preliminary_rank=None,
                    ),
                )
            selection = {k: v for k, v in selection.items() if k in selected}
            self.store.put("deep_selection", selection)
            self.store.put("watchlist", ranked)
            await self.streams.select(selected)
            distribution = {
                h: sum(r["best_pre_rank_horizon"] == h for r in ranked)
                for h in ("SHORT_INTRADAY", "CORE_INTRADAY", "SWING")
            }
            self.status = dict(
                state="collecting",
                at_ms=now_ms(),
                discovered=len(rows),
                preeligible=len(ranked),
                eligible=sum(r["normal_spread"]["eligible"] for r in ranked),
                provisional=sum(not r["normal_spread"]["eligible"] for r in ranked),
                depth_verified=len(verified),
                deep_selected=len(selected),
                retained_active=len(retained),
                depth_requests=requests,
                errors=errors,
                deep_symbols=selected,
                exchange=self.exchange,
                best_horizon_distribution=distribution,
                selection_counts={
                    kind: sum(v["selection_reason"] == kind for v in selection.values())
                    for kind in ("ACTIVE", "CORE", "EXPLOIT", "EXPLORE")
                },
            )
            self.store.put("scanner", self.status)
            from .funnel import emit

            for field, metric in [
                ("discovered", "broad_markets_discovered"),
                ("preeligible", "preeligible_markets"),
                ("depth_verified", "depth_verified_markets"),
                ("deep_selected", "deep_selected_markets"),
            ]:
                emit(
                    self.store,
                    metric,
                    self.status["at_ms"],
                    amount=self.status[field],
                    key=f"scan:{self.status['at_ms']}:{metric}",
                )
            return self.status

    async def monitor_alerted(self, s, now):
        from .lifecycle import source_feed

        previous = dict(s.coverage)
        same_source = s.source == self.exchange
        _, streams = source_feed(self, s.source)
        book = streams.books.get(s.symbol) if streams else None
        tape = streams.tapes.get(s.symbol) if streams else None
        context = (
            self.context.get(s.symbol) if same_source else self.cross_venue.contexts.get((s.source, s.symbol))
        )
        candles = (
            (lambda interval: self.candle_cache.get((s.symbol, interval), (None, []))[1])
            if same_source
            else (lambda interval: self.cross_venue.candles.get((s.source, s.symbol, interval), []))
        )
        fresh_book = bool(book and book.fresh(now, self.settings.book_stale_ms))
        fresh_tape = bool(
            tape
            and 0 <= now - tape.last_receipt <= self.settings.trade_stale_ms
            and -2000 <= now - tape.last_event <= self.settings.trade_stale_ms
        )
        context_fresh = bool(
            context and 0 <= now - context["asof"] <= self.settings.scan_seconds * 1000 + 60_000
        )
        reasons = []
        if not streams:
            reasons.append("Original exchange feed unavailable")
        if not fresh_book or not fresh_tape:
            reasons.append("Live prices unavailable or stale")
        if not context_fresh:
            reasons.append("Market context unavailable or stale")
        if not self.recorder.healthy:
            reasons.append("Recording unavailable")
        if reasons and s.horizon_profile != "LEGACY":
            s.evidence["primary_coverage_complete"] = False
        s.coverage = previous | dict(
            checked_ms=now,
            monitoring="paused" if reasons else "active",
            monitoring_reasons=reasons,
            monitoring_event=None,
        )
        if not reasons and s.horizon_profile != "LEGACY":
            from .evidence import flow_response
            from .thesis_health import evaluate as health_evaluate

            setup = candles(s.setup_timeframe)
            feature = candle_features(setup, now) if len(setup) >= 60 else {}
            health_window = 60_000 if s.horizon_profile == "SHORT_INTRADAY" else 900_000
            recent = tape.window(now - health_window, now)
            observed = footprint(
                recent,
                context["instrument"].tick,
                feature.get("atr", float(context["instrument"].tick) * 10),
                book,
            )
            observed.update(flow_response(recent, book.features(now)))
            bf = book.features(now)
            if ":flow-quality-v1" in s.version:
                from .flow_quality import assess as assess_flow_quality
                from .flow_quality import baseline as flow_baseline

                _, history = flow_baseline(
                    self.store, s.source, s.symbol, s.entry_session, s.horizon_profile, now
                )
                observed["quality"] = assess_flow_quality(
                    recent,
                    context["instrument"].tick,
                    bf,
                    observed,
                    now,
                    now - health_window,
                    now,
                    history,
                    self.store.get("cross:" + s.symbol, {}),
                )
            structure = (
                feature.get("bos") == ("down" if s.direction == "LONG" else "up")
                and bool(setup)
                and (setup[-1].close < s.zone[0] if s.direction == "LONG" else setup[-1].close > s.zone[1])
            )
            observation = dict(
                coverage_complete=bool(
                    tape.coverage_start is not None
                    and tape.coverage_start <= now - health_window
                    and observed.get("available")
                ),
                flow=observed,
                obi=bf.get("obi_persistence", 0),
                structural_failure=structure,
                structure_timeframe=s.setup_timeframe,
                window_end_ms=now,
            )
            sign = 1 if s.direction == "LONG" else -1
            profile = (
                getattr(
                    self,
                    "swing_profiles"
                    if s.horizon_profile in {"SWING", "EXTENDED_SWING"}
                    else "volume_profiles",
                    {},
                ).get(s.symbol, {})
                if same_source
                else {}
            )
            if profile.get("available") and 0 <= now - profile.get("available_ms", 0) <= 120000:
                adverse_value = profile.get("value_migration_direction") == ("DOWN" if sign > 0 else "UP")
                adverse_acceptance = profile.get("acceptance") == (
                    "BELOW_VALUE" if sign > 0 else "ABOVE_VALUE"
                )
                observation["auction_health"] = (
                    "DEGRADED" if adverse_value and adverse_acceptance else "HEALTHY"
                )
            factor_bars = (
                self.candle_cache.get(("BTCUSDT", s.context_timeframe), (None, []))[1] if same_source else []
            )
            original_alignment = s.evidence.get("market_alignment", {})
            if original_alignment.get("alignment") == "IDIOSYNCRATIC_DIVERGENCE":
                from .evidence import factor_context

                original_sign = 1 if s.direction == "LONG" else -1
                current_factor = (
                    factor_context(candles(s.context_timeframe), factor_bars, [], asof=now)
                    if len(factor_bars) >= 60
                    else {}
                )
                threshold = original_alignment.get("residual_threshold", 0.0015)
                current_residual = original_sign * current_factor.get("residual_btc", 0)
                if current_factor.get("available"):
                    observation["factor_health"] = (
                        "DEGRADED" if current_residual < 0.5 * threshold else "HEALTHY"
                    )
                    observation["original_divergence_residual"] = current_residual
            elif (
                len(factor_bars) >= 60 and now - factor_bars[-1].end <= DURATIONS[s.context_timeframe] + 60000
            ):
                factor = candle_features(factor_bars, now)
                observation["factor_health"] = (
                    "DEGRADED"
                    if factor.get("regime") == ("trending down" if sign > 0 else "trending up")
                    else "HEALTHY"
                )
            derivatives = context.get("derivatives", {})
            if (
                0 <= now - derivatives.get("collected_ms", 0) <= 360000
                and derivatives.get("oi_change_pct") is not None
            ):
                observation["derivatives_health"] = (
                    "DEGRADED"
                    if sign * observed.get("delta_pct", 0) <= -20 and abs(derivatives["oi_change_pct"]) >= 2
                    else "HEALTHY"
                )
            cross = self.store.get("cross:" + s.symbol, {})
            if (
                cross.get("available")
                and s.source in cross.get("exchanges", [])
                and cross.get("aligned_flow_venues", 0) >= 2
                and 0 <= now - cross.get("receipt_ms", 0) <= 45000
            ):
                observation["cross_venue_health"] = (
                    "DEGRADED"
                    if cross.get("trusted_consensus_delta_sign") == ("NEGATIVE" if sign > 0 else "POSITIVE")
                    else "MIXED"
                    if cross.get("consensus_delta_sign") == "MIXED"
                    else "HEALTHY"
                )
            observation["volatility_liquidity_health"] = (
                "DEGRADED" if bf.get("spread_bps", 0) > self.settings.max_spread_bps else "HEALTHY"
            )
            key = "thesis_health:" + s.id
            health = health_evaluate(s, observation, self.store.get(key, {}), now)
            self.store.put(key, health)
            if health["withdraw"]:
                s.state = "INVALIDATED"
                s.coverage["terminal_reason"] = health["reason"]
                s.evidence["withdrawal"] = health
                self.store.signal(s, health["reason"])
                await self.notifier.send_research(s, update=True)
                return
        if now >= (s.holding_deadline_ms or s.expires_ms):
            s.state = "EXPIRED"
            s.coverage["monitoring_event"] = "ended"
            s.invalidation = "Tracking period ended; no account outcome is inferred"
        elif reasons:
            since = previous.get("pause_since_ms") or now
            s.coverage["pause_since_ms"] = since
            if now - since >= 60_000 and not previous.get("pause_notice_sent"):
                s.coverage["monitoring_event"] = "paused"
                result = await self.notifier.send_research(s, update=True)
                s.coverage["pause_notice_sent"] = result in {"sent", "already-attempted"}
            self.store.signal(s, "Monitoring paused; setup outcome unknown")
            return
        else:
            facts = facts_asof(self.store, context["instrument"].base, now)
            context_bars = context["h4"]
            if s.horizon_profile != "LEGACY":
                cached = candles(s.context_timeframe)
                if cached:
                    context_bars = cached
            regime = candle_features(context_bars, now)["regime"]
            supportive = regime in {"range", "trending up" if s.direction == "LONG" else "trending down"}
            if s.horizon_profile == "LEGACY" and (not supportive or any(f["major_event"] for f in facts)):
                s.state = "INVALIDATED"
                s.invalidation = (
                    "Higher-timeframe market regime no longer supports the setup"
                    if not supportive
                    else "A known major asset event invalidated the setup"
                )
            elif previous.get("pause_since_ms"):
                s.coverage["monitoring_event"] = "resumed" if previous.get("pause_notice_sent") else None
                if s.coverage["monitoring_event"]:
                    await self.notifier.send_research(s, update=True)
                s.coverage.pop("pause_since_ms", None)
                s.coverage.pop("pause_notice_sent", None)
        if s.state not in TERMINAL and not reasons:
            s.coverage["last_trustworthy_ms"] = now
        self.store.signal(s, s.invalidation if s.state in TERMINAL else "Monitoring active")
        if s.state in TERMINAL:
            await self.notifier.send_research(s, update=True)

    async def health_loop(self):
        await self.lifecycle_bootstrapped.wait()
        while True:
            for payload in self.store.active_signals():
                s = Signal.model_validate(payload)
                if s.source == "tradingview":
                    continue
                try:
                    if (
                        s.id not in self.reconcile_pending
                        and s.coverage.get("monitor_status") != "RECONCILED_PENDING_PATH"
                    ):
                        await self.monitor_alerted(s, now_ms())
                except Exception as exc:
                    self.store.put(
                        "thesis_health:" + s.id,
                        dict(
                            state="DEGRADED",
                            checked_ms=now_ms(),
                            coverage_status="UNAVAILABLE",
                            error_type=type(exc).__name__,
                        ),
                    )
            await asyncio.sleep(15)

    async def evaluate(self):
        now = now_ms()
        self.recorder.critical_symbols = set(self.pending_symbols()) | {"BTCUSDT", "ETHUSDT"}
        self.recorder.critical_symbols.update(
            r[0]
            for r in self.store.db.execute(
                "SELECT DISTINCT json_extract(s.payload,'$.signal.symbol') FROM ml_snapshots s WHERE s.stage='decision' "
                "AND NOT EXISTS (SELECT 1 FROM ml_labels l WHERE l.snapshot_id=s.id AND l.policy='prints-v1') "
                "AND s.decision_ms>?",
                (now - 7 * DAY,),
            )
        )
        for payload in self.store.active_signals():
            s = Signal.model_validate(payload)
            if s.source == "tradingview":
                continue  # TradingView has its own source freshness and lifecycle worker.
            if s.state in TERMINAL:
                continue
            if s.id in getattr(self, "reconcile_pending", set()):
                continue
            if s.state == "ALERTED":
                continue
            c = self.context.get(s.symbol)
            book, tape = self.streams.books.get(s.symbol), self.streams.tapes.get(s.symbol)
            connected = (
                self.streams.connected_for(s.symbol)
                if isinstance(self.streams, NativeStreams)
                else self.streams.connected
            )
            mid = float((max(book.bids) + min(book.asks)) / 2) if book and book.valid else None
            production_v2 = ":production-v2" in s.version
            if now >= s.expires_ms or (
                production_v2 and s.trigger_expires_ms is not None and now >= s.trigger_expires_ms
            ):
                s.state = "EXPIRED"
            elif (
                not production_v2
                and s.evidence.get("execution_window_ms", 900_000) < 900_000
                and now > s.evidence.get("execution_window_end_ms", now) + 120_000
            ):
                s.state = "EXPIRED"
            elif s.source != self.exchange:
                continue
            if s.state in TERMINAL:
                self.store.signal(s, s.invalidation if s.state == "INVALIDATED" else "entry window expired")
                continue
            from .macro import state as macro_state

            macro = macro_state(self.store, self.settings, now)
            if not s.evidence.get("score_components"):
                s.evidence["macro"] = macro
            if macro["macro_pause_active"]:
                s.evidence["macro"] = macro
                from .funnel import emit

                emit(self.store, "macro_deferred", now, signal=s, reason="MACRO_EVENT_PAUSE")
                s.coverage["macro_deferred_ms"] = now
                self.store.signal(s, "High-impact USD entry pause; lifecycle continues")
                continue
            if not c or not tape or not book:
                from .coverage import record as coverage_record

                reason = (
                    "COVERAGE_SYMBOL_NOT_SELECTED"
                    if s.symbol not in self.streams.selected
                    else "COVERAGE_TAPE_NOT_CONNECTED"
                    if not tape
                    else "COVERAGE_BOOK_NOT_CONNECTED"
                    if not book
                    else "COVERAGE_CONTEXT_STALE"
                )
                coverage_record(self, s, now, [reason], now // 60000 * 60000 - 60000, now // 60000 * 60000)
                self.store.signal(s, reason)
                continue
            if s.horizon_profile not in {"LEGACY", "CORE_INTRADAY"}:
                cached = self.candle_cache.get((s.symbol, s.execution_timeframe))
                if not cached:
                    continue
                c = c | {"m15": cached[1]}
            execution_bars = c["m15"]
            candle_end = execution_bars[-1].end
            window_ms = s.evidence.get("execution_window_ms", 900_000)
            end = s.evidence.get("execution_window_end_ms", candle_end) if window_ms < 900_000 else candle_end
            if production_v2 and not s.evidence.get("score_components") and window_ms < 900_000:
                end = now // 60000 * 60000
            start = end - window_ms
            checks = {
                "recorder unavailable": self.recorder.healthy,
                "symbol feed unavailable or stale": connected,
                "order book stale": book.fresh(now, self.settings.book_stale_ms),
                f"full closed {window_ms // 1000}-second trade window not yet retained": 0
                < tape.coverage_start
                <= start,
                "latest trade stale": 0 <= now - tape.last_receipt <= self.settings.trade_stale_ms
                and -2000 <= now - tape.last_event <= self.settings.trade_stale_ms,
                "closed candle stale": 0 <= now - candle_end <= DURATIONS[s.execution_timeframe] + 60_000,
                "execution window stale": 0 <= now - end <= (120_000 if window_ms < 900_000 else 960_000),
                "market context stale": 0 <= now - c["asof"] <= self.settings.scan_seconds * 1000 + 60_000,
            }
            coverage_reasons = [reason for reason, passed in checks.items() if not passed]
            healthy = not coverage_reasons
            macro_deferred_ms = s.coverage.get("macro_deferred_ms")
            s.coverage = s.coverage | dict(
                book_fresh=book.fresh(now, self.settings.book_stale_ms),
                trade_window_complete=healthy,
                recording=self.recorder.healthy,
                historical_depth="not backfilled",
                fundamentals="manual facts or missing",
                checked_ms=now,
                window_start_ms=start,
                window_end_ms=end,
                retained_from_ms=tape.coverage_start,
                reasons=coverage_reasons,
            )
            if macro_deferred_ms is not None:
                s.coverage["macro_deferred_ms"] = macro_deferred_ms
            # One immutable first-covered execution decision per setup ID. Re-evaluating
            # changed flow against yesterday's frozen ML vector would corrupt meta-labels.
            if s.state == "CONFIRMED" and not s.gates and healthy:
                if s.horizon_profile == "EXTENDED_SWING":
                    continue
                if s.coverage.get("macro_deferred_ms"):
                    current_flow = footprint(
                        tape.window(start, end),
                        c["instrument"].tick,
                        candle_features(execution_bars, now)["atr"],
                    )
                    resumed_flow_ok = confirm(s, current_flow, execution_bars)
                    if production_v2:
                        from .evidence import flow_response
                        from .production import flow_support, structure_response

                        current_flow.update(
                            flow_response([t for t in tape.window(start, end) if t.receipt_ms <= now], {})
                        )
                        if ":flow-quality-v1" in s.version:
                            from .flow_quality import assess as assess_flow_quality
                            from .flow_quality import baseline as flow_baseline

                            _, history = flow_baseline(
                                self.store, s.source, s.symbol, s.entry_session, s.horizon_profile, end
                            )
                            current_flow["quality"] = assess_flow_quality(
                                [t for t in tape.window(start, end) if t.receipt_ms <= now],
                                c["instrument"].tick,
                                book.features(now),
                                current_flow,
                                now,
                                start,
                                end,
                                history,
                                self.store.get("cross:" + s.symbol, {}),
                            )
                        support = flow_support(
                            current_flow,
                            1 if s.direction == "LONG" else -1,
                            structure_response(s, execution_bars, now),
                        )
                        resumed_flow_ok = support["raw_fallback"] and support["state"] == "FLOW_SUPPORTIVE"
                    if end <= s.coverage["macro_deferred_ms"] or not resumed_flow_ok:
                        continue
                # A deferred last-mile send must not silently strand an immutable decision.
                if self.entry_ready(s, book, now_ms()) and await self.notifier.send_research(s) == "sent":
                    s.state = "ALERTED"
                    self.store.signal(s, "research card delivered; no trade executed")
                continue
            if s.evidence.get("score_components"):
                continue
            s.state = "PENDING CONFIRMATION"
            self.store.signal(s)
            from .funnel import emit, reject

            if not healthy:
                from .coverage import record as coverage_record

                codes = []
                for reason in coverage_reasons:
                    codes.append(
                        "COVERAGE_RECORDER_UNHEALTHY"
                        if reason == "recorder unavailable"
                        else "COVERAGE_TAPE_NOT_CONNECTED"
                        if reason == "symbol feed unavailable or stale"
                        else "COVERAGE_BOOK_STALE"
                        if reason == "order book stale"
                        else "COVERAGE_TRADE_STALE"
                        if reason == "latest trade stale"
                        else "COVERAGE_TAPE_WARMING"
                        if "not yet retained" in reason and now < tape.coverage_start + window_ms
                        else "COVERAGE_WINDOW_PREDATES_TAPE"
                        if "not yet retained" in reason
                        else "COVERAGE_CONTEXT_STALE"
                    )
                coverage_record(self, s, now, codes, start, end)
                self.store.signal(s, s.coverage["readiness"])
                emit(
                    self.store,
                    "coverage_incomplete",
                    now,
                    signal=s,
                    reason=codes[0],
                )
                continue
            emit(self.store, "confirmation_attempts", now, signal=s, key=f"confirmation:{s.id}:{end}")
            trades = [t for t in tape.window(start, end) if t.receipt_ms <= now]
            execution_features = candle_features(execution_bars, now)
            s.evidence["m15" if s.horizon_profile == "LEGACY" else "execution_features"] = execution_features
            # Generation already fixed these deadlines. Confirmation must not
            # extend or replace an existing setup's original lifecycle.
            if s.trigger_expires_ms is None:
                s.trigger_expires_ms = end + (120_000 if window_ms < 900_000 else 900_000)
            if s.holding_deadline_ms is None:
                s.holding_deadline_ms = now + s.expected_hold_max * 60_000
            if s.primary_tracking_deadline is None and s.horizon_profile != "LEGACY":
                s.primary_tracking_deadline = s.holding_deadline_ms
            # Use only additions at/before window close to avoid confirmation leakage.
            window_book = Book()
            window_book.valid = book.valid
            window_book.changes.extend(x for x in book.changes if x[0] <= end)
            flow = footprint(trades, c["instrument"].tick, execution_features["atr"], window_book)
            bf = book.features(now)
            from .evidence import flow_response

            prior_trades = tape.window(start - window_ms, start)
            flow.update(flow_response(trades, bf, flow_response(prior_trades, bf)))
            from .flow_quality import assess as assess_flow_quality
            from .flow_quality import baseline as flow_baseline

            _, quality_history = flow_baseline(
                self.store, s.source, s.symbol, s.entry_session, s.horizon_profile, end
            )
            quality = assess_flow_quality(
                trades,
                c["instrument"].tick,
                bf,
                flow,
                now,
                start,
                end,
                quality_history,
                self.store.get("cross:" + s.symbol, {}),
            )
            flow["quality"] = quality
            s.evidence["flow_quality"] = quality
            if ":flow-quality-v1" not in s.version:
                flow.pop("quality")
            s.evidence["effective_volume_profile"] = {
                "policy": quality["flow_quality_policy"],
                "available": quality["flow_quality_state"] != "UNAVAILABLE",
                "coverage_complete": healthy,
                "profile_confidence": quality.get("profile_confidence", "LOW"),
                "poc": quality.get("effective_poc"),
                "vah": quality.get("effective_vah"),
                "val": quality.get("effective_val"),
                "hvn": quality.get("effective_hvn"),
                "lvn": quality.get("effective_lvn"),
                "raw_vs_effective_profile_shift": quality.get("raw_vs_effective_profile_shift"),
                "available_ms": now,
                "end_ms": end,
            }
            flow_ok = confirm(s, flow, execution_bars)
            if flow_ok and not production_v2:
                emit(self.store, "flow_confirmed", now, signal=s, key=f"flow:{s.id}:{end}")
            s.gates = []
            normal_spread = self.spread_history(s.symbol).assess(
                now, self.settings.max_spread_bps, self.settings.spread_min_samples
            )
            s.evidence["normal_spread"] = normal_spread
            s.gates.extend(normal_spread["reasons"])
            if not flow_ok and not production_v2:
                s.gates.append("executed order flow did not confirm family trigger")
            if (
                ":flow-quality-v1" in s.version
                and quality.get("flow_trust_score") is not None
                and quality["flow_trust_score"] < 0.6
            ):
                s.gates.append("LOW_INFORMATION_EXECUTED_FLOW")
            if now - end > 900_000:
                s.gates.append("execution trigger expired")
            if end < s.evidence["trigger_bar_end"]:
                s.gates.append("execution confirmation predates setup")
            if mid is None or not s.zone[0] <= mid <= s.zone[1]:
                s.gates.append("current price outside planned entry zone")
            if bf["spread_bps"] > self.settings.max_spread_bps:
                s.gates.append("current spread exceeds gate")
            d = dict(c["derivatives"])
            rate = d["funding_rate"]
            if rate is None or not 0 <= now - d["ticker_observed_ms"] <= 1_200_000:
                s.gates.append("derivatives ticker unavailable or stale")
            if rate is not None and (rate if s.direction == "LONG" else -rate) > 0.001:
                s.gates.append("extreme same-direction funding crowding")
            d["liquidation_feed"] = {
                "bybit": "actual allLiquidation events; reported bankruptcy prices",
                "binance": "sampled forceOrder events; approximate last-fill notional, not a census",
                "okx": "unavailable; stream not implemented",
            }[self.exchange]
            d["liquidations"] = (
                list(self.streams.liquidations[s.symbol])[-50:] if self.exchange != "okx" else None
            )
            facts = facts_asof(self.store, c["instrument"].base, now)
            if any(f["major_event"] for f in facts):
                s.gates.append("known major asset event")
            portfolio = self.store.get("portfolio")
            if portfolio and now - portfolio["at_ms"] > DAY:
                portfolio = None
            s.risk = evaluate_risk(s, c["instrument"], self.settings, book, portfolio, rate)
            s.gates.extend(s.risk["reasons"])
            from .cross_venue import fresh_comparison

            s.evidence.update(
                cross_exchange=fresh_comparison(self.store.get("cross:" + s.symbol, {}), now, self.exchange),
                flow=flow,
                book=bf,
                derivatives=d,
                fundamentals=facts,
                cross_market=self.market_context(now),
            )
            if s.horizon_profile != "LEGACY":
                from .evidence import (
                    auction_context,
                    derivatives_context,
                    execution_context,
                    factor_context,
                    range_context,
                    session_baseline,
                )
                from .observations import recommend

                setup_bars = self.candle_cache.get((s.symbol, s.setup_timeframe), (None, c["h1"]))[1]
                previous_flow = footprint(
                    tape.window(start - window_ms, start), c["instrument"].tick, execution_features["atr"]
                )

                def factor_bars(symbol):
                    if symbol == s.symbol:
                        return []
                    return self.candle_cache.get((symbol, s.setup_timeframe), (None, []))[1]

                s.evidence.update(
                    range=range_context(setup_bars, now),
                    auction=auction_context(flow, previous_flow, mid),
                    market_factor=factor_context(
                        setup_bars, factor_bars("BTCUSDT"), factor_bars("ETHUSDT"), asof=now
                    ),
                    execution=execution_context(s, execution_bars, book, self.settings),
                    selection=self.store.get("deep_selection", {}).get(s.symbol, {}),
                    horizon_suitability=recommend(self.store, s, now),
                    derivatives=derivatives_context(d, setup_bars),
                    session_metrics=session_baseline(self.store, s, flow, bf, now, window_end_ms=end),
                )
                s.evidence["factor_timeframes"] = {
                    tf: factor_context(
                        self.candle_cache.get((s.symbol, tf), (None, []))[1],
                        self.candle_cache.get(("BTCUSDT", tf), (None, []))[1]
                        if s.symbol != "BTCUSDT"
                        else [],
                        self.candle_cache.get(("ETHUSDT", tf), (None, []))[1]
                        if s.symbol != "ETHUSDT"
                        else [],
                        asof=now,
                    )
                    for tf in dict.fromkeys((s.context_timeframe, s.setup_timeframe, s.execution_timeframe))
                }
                if production_v2:
                    from .production import (
                        INTRADAY,
                        REVERSALS,
                        evaluate_intraday_confirmation,
                        market_alignment,
                        structure_response,
                    )

                    s.evidence["factor_regimes"] = {}
                    for tf in dict.fromkeys((s.context_timeframe, s.setup_timeframe, s.execution_timeframe)):
                        regimes = {}
                        for label, symbol in (("btc", "BTCUSDT"), ("eth", "ETHUSDT")):
                            closed = [
                                b for b in self.candle_cache.get((symbol, tf), (None, []))[1] if b.end <= now
                            ]
                            regimes[label] = candle_features(closed, now) if len(closed) >= 60 else {}
                        s.evidence["factor_regimes"][tf] = regimes
                    regimes = s.evidence["factor_regimes"].get(s.setup_timeframe, {})
                    alignment = market_alignment(
                        s,
                        regimes.get("btc", {}),
                        regimes.get("eth", {}),
                        now,
                        structure_response(s, execution_bars, now),
                    )
                    s.evidence["market_alignment"] = alignment
                    if alignment.get("policy") == "market-alignment-v3":
                        history = self.store.get("market-alignment-history-v3", [])
                        identity = f"{s.id}:{end}"
                        if not any(row["identity"] == identity for row in history):
                            history.append(
                                dict(
                                    identity=identity,
                                    at_ms=now,
                                    source=s.source,
                                    state=alignment["market_alignment_state"],
                                    timeframe_context=alignment.get("timeframe_context"),
                                    blocked=alignment.get("blocked", False),
                                )
                            )
                            self.store.put("market-alignment-history-v3", history[-1000:])
                    if s.horizon_profile in INTRADAY and s.family in REVERSALS:
                        confirmation = evaluate_intraday_confirmation(s, flow, execution_bars, now, alignment)
                        s.evidence["confirmation"] = confirmation
                        flow_ok = confirmation["passed"]
                        s.gates.extend(confirmation["reason_codes"])
                    else:
                        if not flow_ok:
                            s.gates.append("executed order flow did not confirm family trigger")
                        if alignment["blocked"]:
                            s.gates.append(alignment["reason"])
                    if flow_ok:
                        emit(self.store, "flow_confirmed", now, signal=s, key=f"flow:{s.id}:{end}")
                elif ":autonomy-v1" in s.version:
                    from .thesis_health import market_gate

                    alignment = market_gate(
                        s,
                        candle_features(factor_bars("BTCUSDT"), now)
                        if len(factor_bars("BTCUSDT")) >= 60
                        else {},
                        candle_features(factor_bars("ETHUSDT"), now)
                        if len(factor_bars("ETHUSDT")) >= 60
                        else {},
                    )
                    s.evidence["market_alignment"] = alignment
                    if alignment["blocked"]:
                        s.gates.append(alignment["reason"])
                    if s.family in {"liquidity_sweep", "range_rejection"}:
                        sign = 1 if s.direction == "LONG" else -1
                        participation = (
                            sign * flow.get("delta_pct", 0) >= 10 and flow.get("delta_persistence", 0) >= 0.5
                        )
                        if not flow_ok or not participation:
                            s.gates.append("UNCONFIRMED_LIQUIDITY_SWEEP")
                if s.evidence["execution"]["stop_noise_ratio"] < 0.5:
                    s.gates.append("stop is inside half the observed execution-bar noise")
                if s.evidence["execution"]["volatility_regime"] == "VOLATILITY_SHOCK":
                    s.gates.append("volatility shock; execution assumptions require requalification")
                if s.horizon_profile == "EXTENDED_SWING":
                    s.evidence["publication_policy"] = "shadow research until horizon-specific validation"
            if production_v2 and s.gates:
                for gate in s.gates:
                    reject(self.store, s, gate, now)
                self.store.signal(s, "Awaiting valid evidence within the original entry window")
                continue
            score(s, flow_ok, rate is not None, preserve_original=":hardening-v1" not in s.version)
            if self.settings.sss_research and s.quality >= 95 and not s.gates and s.risk.get("accepted"):
                s.final_tier = "SSS RESEARCH · UNCALIBRATED"
            from .ml.inference import apply as apply_ml

            if production_v2:
                # ML is advisory for this explicit deterministic policy, including abstention.
                try:
                    apply_ml(
                        s,
                        self.settings.model_copy(update={"ml_filter_research": False}),
                        self.store,
                        now_ms(),
                    )
                except Exception as exc:
                    s.qualification["ml_reason"] = "Optional ML unavailable: " + type(exc).__name__
            else:
                apply_ml(s, self.settings, self.store, now_ms())
            self.recorder.offer(
                "features/signal",
                s.symbol,
                end,
                {
                    "exchange": self.exchange,
                    "signal_id": s.id,
                    "quality": s.quality,
                    "flow": flow,
                    "risk": s.risk,
                    "gates": s.gates,
                    "strategy_version": s.version,
                },
            )
            for gate in s.gates:
                reject(self.store, s, gate, now)
            if not s.gates:
                s.state = "CONFIRMED"
            self.store.signal(s)
            if s.state == "CONFIRMED":
                if s.horizon_profile == "EXTENDED_SWING":
                    continue
                # Last-mile freshness immediately before webhook I/O.
                if not book.fresh(now_ms(), self.settings.book_stale_ms) or not self.recorder.healthy:
                    continue
                if self.entry_ready(s, book, now_ms()) and await self.notifier.send_research(s) == "sent":
                    s.state = "ALERTED"
                    self.store.signal(s, "research card delivered; no trade executed")

    async def scan_loop(self):
        await self.lifecycle_bootstrapped.wait()
        while True:
            delay = self.settings.scan_seconds
            try:
                await self.scan_once()
            except Exception as exc:
                self.status = dict(state="error", error_type=type(exc).__name__, at_ms=now_ms())
                if isinstance(exc, ValueError):
                    self.status["reason"] = (
                        "Local/exchange clock skew exceeds two seconds"
                        if str(exc) == "Local/exchange clock skew exceeds two seconds"
                        else "Public market data validation failed"
                    )
                self.store.put("scanner", self.status)
                # REST outages must not erase independent, healthy live history.
                feed_available = (
                    any(self.streams.connected_for(s) for s in self.streams.selected)
                    if isinstance(self.streams, NativeStreams)
                    else getattr(self.streams, "connected", False)
                )
                preserve_feed = feed_available and self.source_ready and self.recorder.healthy
                self.streams.required_symbols = set(self.pending_symbols())
                await self.streams.select(list(self.streams.selected) + self.pending_symbols())
                self.status["live_feed_preserved"] = preserve_feed
                self.store.put("scanner", self.status)
                log.warning("scan_failed", extra={"error_type": type(exc).__name__})
                # A transient outage should not suspend collection for a full scan interval.
                delay = min(delay, 60)
            await asyncio.sleep(delay)

    async def evaluation_loop(self):
        await self.lifecycle_bootstrapped.wait()
        while True:
            try:
                await self.evaluate()
                self.store.put(
                    "confirmation_health",
                    self.store.get("confirmation_health", {})
                    | dict(state="HEALTHY", last_success_ms=now_ms()),
                )
            except Exception as exc:
                import traceback

                frames = traceback.extract_tb(exc.__traceback__)
                self.store.put(
                    "confirmation_health",
                    self.store.get("confirmation_health", {})
                    | dict(
                        state="DEGRADED",
                        last_error_ms=now_ms(),
                        last_error_type=type(exc).__name__,
                        sqlite_error=getattr(exc, "sqlite_errorname", None),
                        location=f"{frames[-1].name}:{frames[-1].lineno}" if frames else None,
                    ),
                )
                log.warning("evaluation_failed", extra={"error_type": type(exc).__name__})
            await asyncio.sleep(10)

    async def research_loop(self):
        """Compact late observations use shared REST candles, never additional DOM streams."""
        from .observations import advance_batch, refresh_recommendations

        while True:
            try:
                if self.source_ready and self.settings.post_terminal_enabled:
                    rows = self.store.db.execute(
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
                        api = self.api if source == self.exchange else VenueAPI(source, self.settings)
                        try:
                            bars = await api.candles(symbol, "1", now_ms(), limit=300, start=start)
                            await asyncio.to_thread(
                                advance_batch,
                                self.settings.data_dir,
                                [ident for ident, _ in group],
                                bars,
                                now_ms(),
                            )
                        finally:
                            if api is not self.api:
                                await api.close()
                    from .path_research import advance_batch as advance_paths

                    path_rows = self.store.db.execute(
                        "SELECT signal_id,source,symbol,cursor_ms FROM swing_path_jobs WHERE next_ms<=? ORDER BY next_ms LIMIT 50",
                        (now_ms(),),
                    ).fetchall()
                    path_groups = defaultdict(list)
                    for ident, source, symbol, cursor in path_rows:
                        path_groups[(source, symbol)].append((ident, cursor))
                    for (source, symbol), group in list(path_groups.items())[:5]:
                        api = self.api if source == self.exchange else VenueAPI(source, self.settings)
                        try:
                            bars = await api.candles(
                                symbol, "1", now_ms(), limit=300, start=min(cursor for _, cursor in group)
                            )
                            await asyncio.to_thread(
                                advance_paths,
                                self.settings.data_dir,
                                [ident for ident, _ in group],
                                bars,
                                now_ms(),
                            )
                        finally:
                            if api is not self.api:
                                await api.close()
                    await asyncio.to_thread(refresh_recommendations, self.settings.data_dir, now_ms())
                    self.store.put("observation_health", dict(at_ms=now_ms(), processed=len(rows)))
            except Exception as exc:
                self.store.put("observation_health", dict(at_ms=now_ms(), error_type=type(exc).__name__))
            await asyncio.sleep(60)

    async def storage_loop(self):
        from .retention import maintain as storage_maintain
        from .retention import storage_status
        from .storage import Store

        def maintain():
            store = Store(self.settings.data_dir)
            try:
                from .funnel import maintain as maintain_funnel

                maintain_funnel(store, now_ms())
                result = storage_maintain(store, self.settings)
                if now_ms() - store.get("storage_status", {}).get("at_ms", 0) >= 300_000:
                    storage_status(store, self.settings)
                store.put("storage_health", result)
                return result
            finally:
                store.close()

        while True:
            try:
                result = await asyncio.to_thread(maintain)
                self.store.put("storage_health", result)
            except Exception as exc:
                self.store.put("storage_health", dict(error_type=type(exc).__name__, at_ms=now_ms()))
            await asyncio.sleep(60)

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
        start_task("storage", self.storage_loop)
        if self.settings.scan_enabled:
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
