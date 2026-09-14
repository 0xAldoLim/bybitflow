import asyncio
import contextlib
import json
import logging
from statistics import median

import httpx

from .exchanges import VenueAPI, market_probe
from .features import candle_features, validate_bars
from .fundamentals import facts_asof
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
        self.candle_cache = {}
        self.tasks = []
        self.scan_lock = asyncio.Lock()
        self.status = {"state": "disabled", "at_ms": now_ms(), "eligible": 0, "errors": 0}

    @property
    def exchange(self):
        return getattr(self.api, "name", "bybit")

    def spread_key(self, symbol):
        return "spreads:" + ("" if self.exchange == "bybit" else self.exchange + ":") + symbol

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
            await self.streams.stop()
            await self.api.close()
            self.api = VenueAPI(name, self.settings, self.recorder)
            self.streams = (
                Streams(self.settings, self.store, self.recorder)
                if name == "bybit"
                else NativeStreams(self.settings, self.store, self.recorder, self.api)
            )
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
        duration = {"D": DAY, "240": 14_400_000, "60": 3_600_000, "15": 900_000}[interval]
        key, boundary = (symbol, interval), asof // duration
        cached = self.candle_cache.get(key)
        if cached and cached[0] == boundary:
            return cached[1]
        bars = await self.api.candles(symbol, interval, asof, limit=limit)
        validate_bars(bars, asof)
        self.candle_cache[key] = (boundary, bars)
        return bars

    def pending_symbols(self):
        return list(
            dict.fromkeys(
                r[0]
                for r in self.store.db.execute(
                    "SELECT symbol FROM signals WHERE coalesce(json_extract(payload,'$.source'),'bybit')=? AND ("
                    "(state NOT IN ('INVALIDATED','EXPIRED','RESOLVED') AND json_extract(payload,'$.expires_ms')>?) OR "
                    "(json_extract(payload,'$.holding_deadline_ms')>? AND EXISTS "
                    "(SELECT 1 FROM transitions t WHERE t.signal_id=signals.id AND t.state='ALERTED'))) ORDER BY created_ms",
                    (self.exchange, now_ms(), now_ms()),
                )
            )
        )

    async def refresh_pending(self):
        # Fast lane: a broad-universe sweep must not age pending setups silently.
        while True:
            for symbol in self.pending_symbols():
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
                except Exception:
                    self.context.pop(symbol, None)
            await asyncio.sleep(30)

    def save_candidates(self, instrument, h4, h1, m15, evaluated):
        plans = candidates(
            instrument,
            h4,
            h1,
            m15,
            evaluated,
            execution_window_ms=self.settings.execution_window_seconds * 1000,
        )
        for signal in plans:
            old = self.store.db.execute("SELECT 1 FROM signals WHERE id=?", (signal.id,)).fetchone()
            if not old and signal.expires_ms > evaluated:
                self.store.signal(signal)
        return plans

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
            history = SpreadHistory(self.store.get(self.spread_key(symbol), []))
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
                    streams_fresh=self.streams.connected,
                    source_feed_available=source_feed_available,
                ),
            )
            if not self.source_ready or not self.streams.selected or source_feed_available:
                failed_since = None
                continue
            failed_since = failed_since or now_ms()
            if now_ms() - failed_since >= 120_000 and self.settings.market_source in {"auto", "multi"}:
                async with self.scan_lock:
                    self.source_ready = False
                    self.context.clear()
                    await self.streams.select([])
                    self.status.update(
                        state="error",
                        reason="deep feed stale; source requalification required",
                        at_ms=now_ms(),
                    )
                    self.store.put("scanner", self.status)
                failed_since = None

    async def scan_once(self):
        async with self.scan_lock:
            await self.select_source()
            self.status.update(state="scanning", started_ms=now_ms())
            self.status.pop("error_type", None)
            body = await self.api.get("time")
            asof = int(body["time"])
            if abs(asof - now_ms()) > 2000:
                raise ValueError("Local/exchange clock skew exceeds two seconds")
            rows = await self.api.instruments()
            priority_symbols = self.pending_symbols() + self.settings.core_watchlist
            rows.sort(
                key=lambda r: (
                    priority_symbols.index(r["symbol"])
                    if r["symbol"] in priority_symbols
                    else len(priority_symbols),
                    r["symbol"],
                )
            )
            ticker_body = await self.api.get("tickers", category="linear")
            self.record_quotes(ticker_body, now_ms())
            tickers = {r["symbol"]: r for r in ticker_body["result"]["list"]}
            ranked, new_context, errors = [], {}, 0
            for raw in rows:
                inst = (
                    self.api.parse(raw, asof)
                    if isinstance(self.api, VenueAPI)
                    else parse_eligible_metadata(raw, self.settings, asof)
                )
                if not inst:
                    self.record_membership(
                        asof, raw["symbol"], False, {"reason": "instrument metadata gate", "metadata": raw}
                    )
                    continue
                t = tickers.get(inst.symbol, {})
                reasons = []
                try:
                    bid, ask = float(t.get("bid1Price") or 0), float(t.get("ask1Price") or 0)
                    spread = (ask - bid) / ((ask + bid) / 2) * 10000 if ask > bid > 0 else float("inf")
                    if spread > self.settings.max_spread_bps:
                        reasons.append("current spread")
                    if reasons:
                        self.record_membership(
                            asof, inst.symbol, False, {"reasons": reasons, "metadata": raw}
                        )
                        continue
                    daily = await self.cached_candles(inst.symbol, "D", now_ms(), 31)
                    validate_bars(daily, now_ms())
                    if len(daily) < 30 or asof - daily[-1].end >= DAY:
                        reasons.append("30 contiguous completed trading days not verified")
                    turnover = median(c.turnover for c in daily[-7:]) if len(daily) >= 7 else 0
                    if turnover < self.settings.min_daily_turnover or any(c.volume <= 0 for c in daily[-30:]):
                        reasons.append("historical daily liquidity gate")
                    if reasons:
                        self.record_membership(
                            asof,
                            inst.symbol,
                            False,
                            {"reasons": reasons, "turnover": turnover, "metadata": raw},
                        )
                        continue
                    # Actual depth, even for the broad stage; never synthesized from candles.
                    book_data = await self.api.get(
                        "orderbook", category="linear", symbol=inst.symbol, limit=50
                    )
                    book = Book()
                    book.apply(
                        {
                            "type": "snapshot",
                            "ts": book_data["time"],
                            "cts": book_data["result"].get("cts", book_data["time"]),
                            "data": book_data["result"],
                        },
                        now_ms(),
                    )
                    if not all(
                        book.impact(side, self.settings.hypothetical_notional) for side in ("LONG", "SHORT")
                    ):
                        reasons.append("hypothetical order exceeds visible depth")
                    if reasons:
                        self.record_membership(
                            asof, inst.symbol, False, {"reasons": reasons, "metadata": raw}
                        )
                        continue
                    # Refresh asof per instrument: a full-universe sweep can take several minutes.
                    evaluated = now_ms()
                    h4 = await self.cached_candles(inst.symbol, "240", evaluated, 160)
                    h1 = await self.cached_candles(inst.symbol, "60", evaluated, 200)
                    m15 = await self.cached_candles(inst.symbol, "15", evaluated, 120)
                    f4, f1 = candle_features(h4, evaluated), candle_features(h1, evaluated)
                    if evaluated - m15[-1].end > 960_000 or evaluated - h1[-1].end > 3_660_000:
                        raise ValueError("Stale closed candles")
                    oi = await self.api.history(
                        "open-interest", inst.symbol, evaluated - 4 * 3_600_000, evaluated
                    )
                    funding = await self.api.history(
                        "funding/history", inst.symbol, evaluated - 2 * DAY, evaluated
                    )
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
                        "oi_change_pct": (float(oi[-1]["openInterest"]) / float(oi[0]["openInterest"]) - 1)
                        * 100
                        if len(oi) >= 2 and float(oi[0]["openInterest"])
                        else None,
                        "oi_unit": "base coin, API openInterest field; see audited definition",
                        "oi_notional": float(t["openInterestValue"])
                        if t.get("openInterestValue")
                        else (float(oi[-1]["notional"]) if oi and oi[-1].get("notional") else None),
                        "mark": float(t["markPrice"]) if t.get("markPrice") else None,
                        "index": float(t["indexPrice"]) if t.get("indexPrice") else None,
                        "ticker_observed_ms": int(
                            t.get("funding_observed_ms", t.get("observed_ms", ticker_body["time"]))
                        ),
                        "collected_ms": now_ms(),
                    }
                    point = {
                        "metadata": raw,
                        "turnover_median_7d": turnover,
                        "spread_bps": spread,
                        "continuous_daily_bars": len(daily),
                        "available_ms": evaluated,
                    }
                    normal_spread = SpreadHistory(self.store.get(self.spread_key(inst.symbol), [])).assess(
                        now_ms(), self.settings.max_spread_bps, self.settings.spread_min_samples
                    )
                    point["normal_spread"] = normal_spread
                    self.record_membership(evaluated, inst.symbol, normal_spread["eligible"], point)
                    plans = self.save_candidates(inst, h4, h1, m15, evaluated)
                    priority = (30 if plans else 0) + 20 * f1["efficiency"] + 10 * f1["volume_expansion"]
                    ranked.append(
                        {
                            "exchange": self.exchange,
                            "symbol": inst.symbol,
                            "regime_4h": f4["regime"],
                            "regime_1h": f1["regime"],
                            "rank_score": round(priority, 2),
                            "turnover_7d": turnover,
                            "spread_bps": spread,
                            "normal_spread": normal_spread,
                            "eligible": normal_spread["eligible"],
                            "candidates": len(plans),
                            "asof": evaluated,
                        }
                    )
                    new_context[inst.symbol] = dict(
                        instrument=inst, h4=h4, h1=h1, m15=m15, derivatives=derivatives, asof=evaluated
                    )
                    self.context[inst.symbol] = new_context[inst.symbol]
                    # Start core/pending tape collection before a long universe sweep finishes.
                    if inst.symbol in priority_symbols or plans:
                        wanted = list(
                            dict.fromkeys(
                                [s for s in self.pending_symbols() if s in self.context]
                                + [s for s in self.settings.core_watchlist if s in self.context]
                                + list(self.streams.selected)
                                + [inst.symbol]
                            )
                        )[: self.settings.deep_symbols]
                        if tuple(wanted) != tuple(self.streams.selected):
                            await self.streams.select(wanted)
                    self.store.put(
                        "market:" + inst.symbol,
                        {
                            "exchange": self.exchange,
                            "instrument": inst.model_dump(mode="json"),
                            "candles": candle_records(h1),
                            "h4": f4,
                            "h1": f1,
                            "derivatives": derivatives,
                            "normal_spread": normal_spread,
                            "fundamentals": facts_asof(self.store, inst.base, evaluated),
                            "asof": evaluated,
                        },
                    )
                except PermissionError:
                    raise
                except Exception as exc:
                    if not self.recorder.healthy:
                        raise RuntimeError("Recording unavailable; broad scan aborted") from exc
                    errors += 1
                    self.record_membership(
                        now_ms(), inst.symbol, False, {"reason": type(exc).__name__, "metadata": raw}
                    )
                    log.warning(
                        "symbol_scan_failed", extra={"symbol": inst.symbol, "error_type": type(exc).__name__}
                    )
            ranked.sort(key=lambda x: x["rank_score"], reverse=True)
            self.context = {s: c for s, c in self.context.items() if now_ms() - c["asof"] <= 960_000}
            self.candle_cache = {k: v for k, v in self.candle_cache.items() if k[0] in tickers}
            self.store.put("watchlist", ranked)
            core = [s for s in self.settings.core_watchlist if s in new_context]
            pinned = self.pending_symbols()
            selected = list(dict.fromkeys(pinned + core + [r["symbol"] for r in ranked]))[
                : self.settings.deep_symbols
            ]
            await self.streams.select(selected)
            self.status = dict(
                state="collecting",
                at_ms=now_ms(),
                discovered=len(rows),
                eligible=sum(r["eligible"] for r in ranked),
                provisional=sum(not r["eligible"] for r in ranked),
                errors=errors,
                deep_symbols=selected,
                exchange=self.exchange,
            )
            self.store.put("scanner", self.status)
            return self.status

    async def evaluate(self):
        now = now_ms()
        for payload in self.store.signals(2000):
            s = Signal.model_validate(payload)
            if s.source == "tradingview":
                continue  # TradingView has its own source freshness and lifecycle worker.
            if s.state in TERMINAL:
                continue
            was_alerted = s.state == "ALERTED"
            c = self.context.get(s.symbol)
            book, tape = self.streams.books.get(s.symbol), self.streams.tapes.get(s.symbol)
            connected = (
                self.streams.connected_for(s.symbol)
                if isinstance(self.streams, NativeStreams)
                else self.streams.connected
            )
            mid = float((max(book.bids) + min(book.asks)) / 2) if book and book.valid else None
            if now >= s.expires_ms:
                s.state = "EXPIRED"
            elif s.source != self.exchange:
                s.state = "INVALIDATED"
                s.invalidation = "Primary exchange changed; source continuity cannot be transferred"
            elif mid and (mid <= s.stop if s.direction == "LONG" else mid >= s.stop):
                s.state = "INVALIDATED"
                s.invalidation = "Observed price crossed the planned stop level; no account fill is verified"
            elif was_alerted and (
                not c
                or not book
                or not book.fresh(now, self.settings.book_stale_ms)
                or not self.recorder.healthy
                or not connected
            ):
                s.state = "INVALIDATED"
                s.invalidation = "Required live feed lost; the published setup is no longer supported"
            if s.state in TERMINAL:
                self.store.signal(s, s.invalidation if s.state == "INVALIDATED" else "entry window expired")
                if was_alerted:
                    await self.notifier.send_research(s, update=True)
                continue
            if not c or not tape or not book:
                continue
            candle_end = c["m15"][-1].end
            window_ms = s.evidence.get("execution_window_ms", 900_000)
            end = s.evidence.get("execution_window_end_ms", candle_end) if window_ms < 900_000 else candle_end
            if was_alerted and window_ms < 900_000:
                # Monitor current coverage without ageing the published decision's frozen window.
                end = now // 60_000 * 60_000
            start = end - window_ms
            checks = {
                "recorder unavailable": self.recorder.healthy,
                "symbol feed unavailable or stale": connected,
                "order book stale": book.fresh(now, self.settings.book_stale_ms),
                f"full closed {window_ms // 1000}-second trade window not yet retained": 0
                < tape.coverage_start
                <= start,
                "latest trade stale": 0 <= now - tape.last_receipt <= self.settings.trade_stale_ms
                and -1000 <= now - tape.last_event <= self.settings.trade_stale_ms,
                "closed candle stale": 0 <= now - candle_end <= 960_000,
                "execution window stale": 0 <= now - end <= (120_000 if window_ms < 900_000 else 960_000),
                "market context stale": 0 <= now - c["asof"] <= self.settings.scan_seconds * 1000 + 60_000,
            }
            coverage_reasons = [reason for reason, passed in checks.items() if not passed]
            healthy = not coverage_reasons
            s.coverage = dict(
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
            if was_alerted:
                facts = facts_asof(self.store, c["instrument"].base, now)
                regime = candle_features(c["h4"], now)["regime"]
                supportive = regime in {"range", "trending up" if s.direction == "LONG" else "trending down"}
                spread_ok = SpreadHistory(self.store.get(self.spread_key(s.symbol), [])).assess(
                    now, self.settings.max_spread_bps, self.settings.spread_min_samples
                )["eligible"]
                if not healthy or not supportive or not spread_ok or any(f["major_event"] for f in facts):
                    s.state = "INVALIDATED"
                    if not healthy:
                        s.invalidation = "Live evidence unavailable; setup withdrawn. This does not establish a stop-loss hit"
                    elif not supportive:
                        s.invalidation = "Higher-timeframe market regime no longer supports the setup"
                    elif not spread_ok:
                        s.invalidation = "Observed spread no longer meets the liquidity requirement"
                    else:
                        s.invalidation = "A known major asset event invalidated the setup assumptions"
                    self.store.signal(s)
                    await self.notifier.send_research(s, update=True)
                continue
            # One immutable first-covered execution decision per setup ID. Re-evaluating
            # changed flow against yesterday's frozen ML vector would corrupt meta-labels.
            if s.evidence.get("score_components"):
                continue
            s.state = "PENDING CONFIRMATION"
            self.store.signal(s)
            if not healthy:
                continue
            trades = tape.window(start, end)
            s.evidence["m15"] = candle_features(c["m15"], now)
            s.trigger_expires_ms = end + (120_000 if window_ms < 900_000 else 900_000)
            s.holding_deadline_ms = now + 14_400_000
            # Use only additions at/before window close to avoid confirmation leakage.
            window_book = Book()
            window_book.valid = book.valid
            window_book.changes.extend(x for x in book.changes if x[0] <= end)
            flow = footprint(trades, c["instrument"].tick, s.evidence["m15"]["atr"], window_book)
            flow_ok = confirm(s, flow, c["m15"])
            s.gates = []
            normal_spread = SpreadHistory(self.store.get(self.spread_key(s.symbol), [])).assess(
                now, self.settings.max_spread_bps, self.settings.spread_min_samples
            )
            s.evidence["normal_spread"] = normal_spread
            s.gates.extend(normal_spread["reasons"])
            if not flow_ok:
                s.gates.append("executed order flow did not confirm family trigger")
            if now - end > 900_000:
                s.gates.append("execution trigger expired")
            if end < s.evidence["trigger_bar_end"]:
                s.gates.append("execution confirmation predates setup")
            if mid is None or not s.zone[0] <= mid <= s.zone[1]:
                s.gates.append("current price outside planned entry zone")
            bf = book.features(now)
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
            score(s, flow_ok, rate is not None)
            if self.settings.sss_research and s.quality >= 95 and not s.gates and s.risk.get("accepted"):
                s.final_tier = "SSS RESEARCH · UNCALIBRATED"
            from .ml.inference import apply as apply_ml

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
            if not s.gates:
                s.state = "CONFIRMED"
            self.store.signal(s)
            if s.state == "CONFIRMED":
                # Last-mile freshness immediately before webhook I/O.
                if not book.fresh(now_ms(), self.settings.book_stale_ms) or not self.recorder.healthy:
                    continue
                if await self.notifier.send_research(s) == "sent":
                    s.state = "ALERTED"
                    self.store.signal(s, "research card delivered; no trade executed")

    async def scan_loop(self):
        while True:
            delay = self.settings.scan_seconds
            try:
                await self.scan_once()
            except Exception as exc:
                self.status = dict(state="error", error_type=type(exc).__name__, at_ms=now_ms())
                self.store.put("scanner", self.status)
                # REST outages must not erase independent, healthy live history.
                feed_available = (
                    any(self.streams.connected_for(s) for s in self.streams.selected)
                    if isinstance(self.streams, NativeStreams)
                    else getattr(self.streams, "connected", False)
                )
                preserve_feed = (
                    isinstance(exc, (httpx.TransportError, ConnectionError, TimeoutError))
                    and feed_available
                    and self.source_ready
                    and self.recorder.healthy
                )
                if not preserve_feed:
                    self.context.clear()
                    await self.streams.select([])
                    if self.settings.market_source in {"auto", "multi"}:
                        self.source_ready = False
                self.status["live_feed_preserved"] = preserve_feed
                self.store.put("scanner", self.status)
                log.warning("scan_failed", extra={"error_type": type(exc).__name__})
                # A transient outage should not suspend collection for a full scan interval.
                delay = min(delay, 60)
            await asyncio.sleep(delay)

    async def evaluation_loop(self):
        while True:
            try:
                await self.evaluate()
            except Exception as exc:
                log.warning("evaluation_failed", extra={"error_type": type(exc).__name__})
            await asyncio.sleep(10)

    def start(self):
        self.tasks = [asyncio.create_task(self.evaluation_loop())]
        if self.settings.scan_enabled:
            self.tasks.append(asyncio.create_task(self.scan_loop()))
            self.tasks.append(asyncio.create_task(self.quote_loop()))
            self.tasks.append(asyncio.create_task(self.refresh_pending()))
            self.tasks.append(asyncio.create_task(self.source_watchdog()))
            if self.settings.market_source == "multi":
                from .cross_venue import CrossVenue

                self.tasks.append(asyncio.create_task(CrossVenue(self).run()))

    async def stop(self):
        for task in self.tasks:
            task.cancel()
        for task in self.tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.streams.stop()
        await self.api.close()
