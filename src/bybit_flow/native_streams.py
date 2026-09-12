"""Bounded exchange-native streams feeding the existing Book/Tape/footprint engine.

Each retained symbol keeps its own connection tasks during subscription rotation.
Binance public-depth and market-trade sockets are separate (2026 routing rules).
Raw wire records and normalized scanner-v1 records have distinct source prefixes.
"""

import asyncio
import contextlib
import json
import random
import uuid
from collections import deque
from decimal import Decimal as D

from websockets.asyncio.client import connect

from .exchanges import trade_from_wire
from .orderflow import Book, BookGap, Tape
from .storage import now_ms


class SequencedBook:
    def __init__(self, venue, book, multiplier=D(1)):
        self.venue, self.book, self.multiplier = venue, book, multiplier
        self.last, self.bridged, self.local = None, False, 1

    def apply(self, data, receipt, snapshot=False):
        if self.venue == "binance":
            current = int(data["lastUpdateId"] if snapshot else data["u"])
            if not snapshot:
                if self.last is None:
                    raise BookGap("Binance delta before REST snapshot")
                if not self.bridged:
                    if current < self.last:
                        return False
                    if not int(data["U"]) <= self.last <= current:
                        self.book.reset()
                        raise BookGap("Binance snapshot bridge missing")
                    self.bridged = True
                elif int(data["pu"]) != self.last or current <= self.last:
                    self.book.reset()
                    raise BookGap("Binance pu continuity failure")
            bids, asks = (data["bids"], data["asks"]) if snapshot else (data["b"], data["a"])
            event = int(data["T"])
        else:
            current = int(data["seqId"])
            if not snapshot:
                if self.last is None or int(data["prevSeqId"]) != self.last or current < self.last:
                    self.book.reset()
                    raise BookGap("OKX sequence gap/reset; new snapshot required")
                if current == self.last and (data["bids"] or data["asks"]):
                    self.book.reset()
                    raise BookGap("OKX changed levels without advancing sequence")
            bids, asks, event = data["bids"], data["asks"], int(data["ts"])
        self.local += 1
        self.book.apply(
            {
                "type": "snapshot" if snapshot else "delta",
                "ts": event,
                "data": {
                    "u": self.local,
                    "seq": self.local,
                    "b": [[r[0], str(D(r[1]) * self.multiplier)] for r in bids],
                    "a": [[r[0], str(D(r[1]) * self.multiplier)] for r in asks],
                },
            },
            receipt,
        )
        self.last = current
        if len(self.book.bids) + len(self.book.asks) > 6000:
            self.book.reset()
            raise BookGap("Retained depth budget exceeded; resnapshot required")
        if snapshot and self.venue == "binance":
            # REST is useful for bootstrap only; cannot qualify until WS bridge exists.
            self.bridged = False
        return True


class NativeStreams:
    def __init__(self, settings, store, recorder, api):
        self.settings, self.store, self.recorder, self.api = settings, store, recorder, api
        self.selected, self.books, self.tapes, self.liquidations = (), {}, {}, {}
        self.tasks, self.sessions, self.last_sample, self.reconnects = {}, {}, {}, {}
        self.last_message = 0
        self.frames = {}

    @property
    def connected(self):
        return bool(self.selected) and all(self.connected_for(s) for s in self.selected)

    def connected_for(self, symbol):
        now = now_ms()
        return (
            symbol in self.selected
            and self.books[symbol].fresh(now, self.settings.book_stale_ms)
            and 0 <= now - self.tapes[symbol].last_receipt <= self.settings.trade_stale_ms
        )

    async def select(self, symbols):
        selected = tuple(dict.fromkeys(symbols))[: self.settings.deep_symbols]
        removed, added = set(self.selected) - set(selected), set(selected) - set(self.selected)
        for s in removed:
            task = self.tasks.pop(s)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            for mapping in (
                self.books,
                self.tapes,
                self.liquidations,
                self.sessions,
                self.last_sample,
                self.frames,
            ):
                mapping.pop(s, None)
        for s in added:
            self.frames[s] = deque(maxlen=720)
            self.books[s], self.tapes[s], self.liquidations[s] = (
                Book(),
                Tape(self.settings.tape_max_trades),
                deque(maxlen=1000),
            )
            self.tasks[s] = asyncio.create_task(self.run_symbol(s))
        self.selected = selected
        if added or removed:
            self.record(
                "control/rotation", "ALL", now_ms(), {"added": sorted(added), "removed": sorted(removed)}
            )

    def record(self, source, symbol, event, payload, complete=True):
        self.recorder.offer(
            f"native/{self.api.name}/{source}",
            symbol,
            event,
            payload | {"exchange": self.api.name, "connection_id": self.sessions.get(symbol)},
            now_ms(),
            complete=complete,
        )

    def trade(self, symbol, raw):
        receipt = now_ms()
        t = trade_from_wire(self.api.name, raw, receipt, self.api.metadata.get(symbol))
        if not self.tapes[symbol].add(t):
            return
        self.record(
            "ws/publicTrade." + symbol,
            symbol,
            t.event_ms,
            {
                "normalization": "scanner-v1; base units; exchange-reported taker side",
                "data": [
                    {"T": t.event_ms, "i": t.trade_id, "S": t.side, "p": str(t.price), "v": str(t.size)}
                ],
            },
        )
        self.last_message = receipt
        self.sample(symbol)

    def sample(self, symbol):
        now = now_ms()
        if now - self.last_sample.get(symbol, 0) < 5000:
            return
        self.last_sample[symbol] = now
        book, tape = self.books[symbol], self.tapes[symbol]
        health = dict(
            exchange=self.api.name,
            symbol=symbol,
            at_ms=now,
            status="HEALTHY"
            if book.fresh(now, self.settings.book_stale_ms)
            and now - tape.last_receipt <= self.settings.trade_stale_ms
            else "DEGRADED",
            book_event_ms=book.event_ms,
            trade_event_ms=tape.last_event,
            coverage_start=tape.coverage_start,
            reconnects=self.reconnects.get(symbol, 0),
            connection_id=self.sessions[symbol],
            liquidations="sampled forceOrder"
            if self.api.name == "binance"
            else "unavailable; not subscribed",
        )
        self.store.put("stream_health", health)
        self.store.put(f"feed:{self.api.name}:{symbol}", health)
        if book.fresh(now, self.settings.book_stale_ms):
            frame = book.features(now)
            frame.update(exchange=self.api.name, connection_id=self.sessions[symbol])
            self.frames[symbol].append(frame)
            self.record("liquidity/frame", symbol, book.event_ms, frame)
            self.store.put(f"liquidity:{self.api.name}:{symbol}", frame)

    async def run_symbol(self, symbol):
        backoff = 1
        while True:
            self.sessions[symbol] = uuid.uuid4().hex
            self.books[symbol].reset()
            self.tapes[symbol].reset(now_ms())
            self.liquidations[symbol].clear()
            try:
                if self.api.name == "binance":
                    async with asyncio.TaskGroup() as group:
                        group.create_task(self.binance_market(symbol))
                        group.create_task(self.binance_depth(symbol))
                else:
                    await self.okx(symbol)
            except asyncio.CancelledError:
                if self.recorder.healthy:
                    self.record("control/gap", symbol, now_ms(), {"reason": "subscription stopped"}, False)
                raise
            except Exception as exc:
                self.books[symbol].reset()
                self.tapes[symbol].reset(now_ms())
                self.reconnects[symbol] = self.reconnects.get(symbol, 0) + 1
                if self.recorder.healthy:
                    self.record("control/gap", symbol, now_ms(), {"reason": type(exc).__name__}, False)
                self.store.put(
                    f"feed:{self.api.name}:{symbol}",
                    {"status": "DISCONNECTED", "at_ms": now_ms(), "error_type": type(exc).__name__},
                )
                if not self.recorder.healthy:
                    return  # Persistence loss is a circuit breaker, not a reason to collect unrecorded data.
                await asyncio.sleep(backoff + random.random())
                backoff = min(backoff * 2, 60)

    async def binance_market(self, symbol):
        streams = f"{symbol.lower()}@aggTrade/{symbol.lower()}@forceOrder"
        async with connect(
            "wss://fstream.binance.com/market/stream?streams=" + streams,
            max_queue=64,
            max_size=1_000_000,
            open_timeout=15,
        ) as ws:
            self.tapes[symbol].reset(now_ms())
            self.record("control/subscribed", symbol, now_ms(), {"symbols": [symbol]})
            while True:
                msg = json.loads(await asyncio.wait_for(ws.recv(), 35))["data"]
                self.record("raw/market", symbol, int(msg["E"]), msg)
                if msg["e"] == "aggTrade":
                    self.trade(symbol, msg)
                elif msg["e"] == "forceOrder":
                    o = msg["o"]
                    # Last filled quantity/average fill price, not total order size at limit price.
                    price, quantity = D(o["ap"]), D(o["l"])
                    if price > 0 and quantity > 0:
                        liquidation = dict(
                            exchange="binance",
                            event_ms=int(o["T"]),
                            receipt_ms=now_ms(),
                            liquidated_side="LONG" if o["S"] == "SELL" else "SHORT",
                            price=str(price),
                            quantity=str(quantity),
                            notional=str(price * quantity),
                            price_semantics="reported average fill price; sampled stream, not complete liquidations",
                        )
                        self.liquidations[symbol].append(liquidation)
                        self.record("liquidation", symbol, int(o["T"]), liquidation)

    async def binance_depth(self, symbol):
        url = f"wss://fstream.binance.com/public/ws/{symbol.lower()}@depth@100ms"
        async with connect(url, max_queue=64, max_size=2_000_000, open_timeout=15) as ws:
            # The websocket library buffers bounded messages while the REST snapshot is requested.
            snapshot = await self.api.request("/fapi/v1/depth", symbol=symbol, limit=1000)
            book = self.books[symbol]
            state = SequencedBook("binance", book)
            state.apply(snapshot, now_ms(), snapshot=True)
            book.valid = False
            while True:
                msg = json.loads(await asyncio.wait_for(ws.recv(), 10))
                self.record("raw/depth", symbol, int(msg["E"]), msg)
                if not state.bridged and int(msg["u"]) < state.last:
                    continue
                # Bridge is checked by SequencedBook before the book can qualify.
                book.valid = True
                state.apply(msg, now_ms())
                self.sample(symbol)

    async def okx(self, symbol):
        inst = self.api.metadata[symbol]
        state = SequencedBook("okx", self.books[symbol], inst.contract_multiplier)
        async with connect(
            "wss://ws.okx.com:8443/ws/v5/public",
            max_queue=64,
            max_size=2_000_000,
            open_timeout=15,
            ping_interval=None,
        ) as ws:
            await ws.send(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [
                            {"channel": channel, "instId": inst.exchange_symbol}
                            for channel in ("trades", "books")
                        ],
                    }
                )
            )
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), 20)
                except TimeoutError:
                    await ws.send("ping")
                    raw = await asyncio.wait_for(ws.recv(), 10)
                if raw == "pong":
                    continue
                msg = json.loads(raw)
                if msg.get("event") == "error":
                    raise ValueError("OKX subscription rejected")
                channel = msg.get("arg", {}).get("channel")
                if msg.get("event") == "subscribe" and channel == "trades":
                    self.tapes[symbol].reset(now_ms())
                    self.record("control/subscribed", symbol, now_ms(), {"symbols": [symbol]})
                for row in msg.get("data", []):
                    self.record("raw/" + str(channel), symbol, int(row["ts"]), row)
                    if channel == "trades":
                        self.trade(symbol, row)
                    elif channel == "books":
                        state.apply(row, now_ms(), snapshot=msg["action"] == "snapshot")
                        self.sample(symbol)

    async def stop(self):
        await self.select([])
