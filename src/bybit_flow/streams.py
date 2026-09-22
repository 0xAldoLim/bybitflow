import asyncio
import contextlib
import json
import logging
import random
from collections import deque
from decimal import Decimal

import websockets

from .models import Trade
from .orderflow import Book, Tape
from .storage import now_ms

log = logging.getLogger(__name__)


class Streams:
    def __init__(self, settings, store, recorder):
        self.settings, self.store, self.recorder = settings, store, recorder
        self.selected = ()
        self.books, self.tapes, self.liquidations = {}, {}, {}
        self.connected = False
        self.task = None
        self.last_message = 0
        self.ws = None
        self.subscription_changes = {}
        self.frames = {}
        self.last_frame = {}
        self.selection_lock = asyncio.Lock()

    def topics(self, symbols):
        return [
            topic
            for s in symbols
            for topic in (
                f"publicTrade.{s}",
                f"orderbook.{self.settings.book_depth}.{s}",
                f"allLiquidation.{s}",
            )
        ]

    async def select(self, symbols):
        async with self.selection_lock:
            await self._select(symbols)

    async def _select(self, symbols):
        symbols = tuple(sorted(set(symbols) | getattr(self, "required_symbols", set())))
        if symbols == self.selected and self.task and not self.task.done():
            return
        if self.ws and self.connected:
            added, removed = set(symbols) - set(self.selected), set(self.selected) - set(symbols)
            for s in added:
                self.books[s] = Book()
                self.tapes[s] = Tape(self.settings.tape_max_trades)
                self.liquidations[s] = deque(maxlen=10000)
            self.selected = symbols
            for s in removed:
                self.books.pop(s, None)
                self.tapes.pop(s, None)
                self.liquidations.pop(s, None)
                self.frames.pop(s, None)
                self.last_frame.pop(s, None)
            try:
                if removed:
                    await self.ws.send(json.dumps({"op": "unsubscribe", "args": self.topics(removed)}))
                if added:
                    request_id = str(now_ms())
                    self.subscription_changes[request_id] = list(added)
                    await self.ws.send(
                        json.dumps({"op": "subscribe", "req_id": request_id, "args": self.topics(added)})
                    )
                self.recorder.offer(
                    "control/rotation", "ALL", now_ms(), {"added": sorted(added), "removed": sorted(removed)}
                )
                return
            except Exception:
                self.invalidate("subscription rotation failed")
        await self.stop()
        self.selected = symbols
        self.books = {s: Book() for s in symbols}
        self.tapes = {s: Tape(self.settings.tape_max_trades) for s in symbols}
        self.liquidations = {s: deque(maxlen=10000) for s in symbols}
        if symbols:
            self.task = asyncio.create_task(self.run())

    def invalidate(self, reason):
        self.connected = False
        if self.recorder.healthy:
            try:
                self.recorder.offer("control/gap", "ALL", now_ms(), {"reason": reason}, complete=False)
            except RuntimeError:
                pass
        for s in self.selected:
            self.books[s].reset()
            self.tapes[s].reset(now_ms())
            self.liquidations[s].clear()
        self.store.put("stream_health", {"connected": False, "reason": reason, "at_ms": now_ms()})

    def process(self, msg, receipt):
        topic = msg["topic"]
        symbol = topic.split(".")[-1]
        if symbol not in self.books:
            return
        if self.recorder.offer("ws/" + topic, symbol, msg["ts"], msg, receipt) is False:
            self.books[symbol].reset()
            self.tapes[symbol].reset(receipt)
            return
        if topic.startswith("orderbook."):
            self.books[symbol].apply(msg, receipt)
            if receipt - self.last_frame.get(symbol, 0) >= 5000:
                frame = self.books[symbol].features(receipt) | {"exchange": "bybit"}
                self.frames.setdefault(symbol, deque(maxlen=720)).append(frame)
                self.last_frame[symbol] = receipt
                self.recorder.offer("liquidity/frame", symbol, self.books[symbol].event_ms, frame, receipt)
        elif topic.startswith("publicTrade."):
            for t in msg["data"]:
                # Block trades remain raw, excluded from continuous-book execution evidence.
                if t.get("BT", False):
                    continue
                self.tapes[symbol].add(
                    Trade(symbol, int(t["T"]), receipt, t["i"], t["S"], Decimal(t["p"]), Decimal(t["v"]))
                )
        elif topic.startswith("allLiquidation."):
            rows = msg["data"] if isinstance(msg["data"], list) else [msg["data"]]
            for t in rows:
                self.liquidations[symbol].append(
                    dict(
                        event_ms=int(t["T"]),
                        receipt_ms=receipt,
                        liquidated_position="LONG" if t["S"] == "Buy" else "SHORT",
                        bankruptcy_price=t["p"],
                        quantity=t["v"],
                        bankruptcy_notional=float(Decimal(t["p"]) * Decimal(t["v"])),
                    )
                )

    async def heartbeat(self, ws):
        while True:
            await asyncio.sleep(20)
            await ws.send(json.dumps({"op": "ping"}))

    async def run(self):
        delay = 1
        while True:
            self.invalidate("awaiting fresh subscription and snapshots")
            heartbeat = None
            try:
                if not self.recorder.healthy:
                    return
                async with websockets.connect(
                    "wss://stream.bybit.com/v5/public/linear",
                    max_queue=64,
                    max_size=4 * 1024 * 1024,
                    open_timeout=20,
                    ping_interval=None,
                ) as ws:
                    self.ws = ws
                    self.subscription_changes.clear()
                    args = [
                        topic
                        for s in self.selected
                        for topic in (
                            f"publicTrade.{s}",
                            f"orderbook.{self.settings.book_depth}.{s}",
                            f"allLiquidation.{s}",
                        )
                    ]
                    if len(json.dumps(args)) > 21000:
                        raise ValueError("Subscription request exceeds documented limit")
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    heartbeat = asyncio.create_task(self.heartbeat(ws))
                    while True:
                        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=35))
                        received = now_ms()
                        self.last_message = received
                        if msg.get("op") == "subscribe":
                            if not msg.get("success"):
                                raise ValueError("Subscription rejected")
                            self.connected = True
                            added = self.subscription_changes.pop(msg.get("req_id"), None)
                            confirmed_symbols = added if added is not None else list(self.selected)
                            self.recorder.offer(
                                "control/subscribed",
                                "ALL",
                                received,
                                {"symbols": confirmed_symbols},
                                received,
                            )
                            for symbol in confirmed_symbols:
                                if symbol in self.tapes:
                                    self.tapes[symbol].reset(received)
                            self.store.put(
                                "stream_health",
                                {"connected": True, "at_ms": received, "symbols": list(self.selected)},
                            )
                            delay = 1
                        if "topic" in msg:
                            self.process(msg, received)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Never log transport URLs, credentials or raw payloads.
                self.invalidate(type(exc).__name__)
                log.warning("stream_reconnect", extra={"error_type": type(exc).__name__})
                await asyncio.sleep(delay + random.random())
                delay = min(60, delay * 2)
            finally:
                self.ws = None
                if heartbeat:
                    heartbeat.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await heartbeat

    async def stop(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.task
            self.task = None
        if self.selected:
            self.invalidate("collector stopped or subscription set changed")
