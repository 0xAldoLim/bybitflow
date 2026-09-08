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

    async def select(self, symbols):
        symbols = tuple(sorted(set(symbols)))
        if symbols == self.selected:
            return
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
        self.recorder.offer("ws/" + topic, symbol, msg["ts"], msg, receipt)
        if topic.startswith("orderbook."):
            self.books[symbol].apply(msg, receipt)
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
                            self.recorder.offer(
                                "control/subscribed",
                                "ALL",
                                received,
                                {"symbols": list(self.selected)},
                                received,
                            )
                            for tape in self.tapes.values():
                                tape.reset(received)
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
                if heartbeat:
                    heartbeat.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await heartbeat

    async def stop(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
            self.task = None
        if self.selected:
            self.invalidate("collector stopped or subscription set changed")
