"""Bounded minute OI observations using the existing public venue REST budget."""

import asyncio
from collections import deque

from .liquidation import oi_changes, sample_oi
from .storage import now_ms

TTL_MS = 55_000


def scope(scanner):
    return list(
        dict.fromkeys(["BTCUSDT", "ETHUSDT"] + scanner.pending_symbols() + list(scanner.streams.selected))
    )[:10]


class Collector:
    def __init__(self, scanner):
        self.scanner = scanner
        self.cache = {}

    def series(self, source, symbol):
        row = self.cache.get((source, symbol), {})
        if row.get("observation") and 0 <= now_ms() - row["observation"]["available_ms"] <= 90_000:
            return self.scanner.oi_series.get((source, symbol), ())
        return ()

    async def refresh(self):
        scanner, source = self.scanner, self.scanner.exchange
        api = scanner.api
        symbols = scope(scanner)
        keys = {(source, symbol) for symbol in symbols}
        self.cache = {key: value for key, value in self.cache.items() if key in keys}
        scanner.oi_series = {key: value for key, value in scanner.oi_series.items() if key in keys}

        async def one(symbol):
            key = (source, symbol)
            cached = self.cache.get(key, {})
            at = now_ms()
            if 0 <= at - cached.get("attempt_ms", -TTL_MS) < TTL_MS:
                return
            self.cache[key] = dict(attempt_ms=at, observation=None)
            try:
                row = await scanner.bounded_rest(api.current_oi(symbol))
                if scanner.exchange != source or row["source"] != source or row["symbol"] != symbol:
                    raise ValueError("Current OI source or symbol changed")
                series = scanner.oi_series.setdefault(
                    key, deque(scanner.store.get(f"v8_oi:{source}:{symbol}", []), maxlen=30)
                )
                sample_oi(
                    series,
                    row["source_ms"],
                    row["available_ms"],
                    row["open_interest"],
                    row["open_interest_notional"],
                )
                scanner.store.put(f"v8_oi:{source}:{symbol}", list(series))
                self.cache[key]["observation"] = row
            except Exception as exc:
                self.cache[key]["error"] = type(exc).__name__

        await asyncio.gather(*(one(symbol) for symbol in symbols))
        if scanner.exchange != source:
            return
        at = now_ms()
        scanner.store.put(
            "v8_current_oi",
            dict(
                source=source,
                at_ms=at,
                symbols=len(symbols),
                available=sum(bool(self.series(source, symbol)) for symbol in symbols),
                errors={
                    symbol: row["error"]
                    for (venue, symbol), row in self.cache.items()
                    if venue == source and row.get("error")
                },
                samples={symbol: len(scanner.oi_series.get((source, symbol), ())) for symbol in symbols},
                one_minute_ready=sum(
                    oi_changes(self.series(source, symbol), at)["oi_change_1m_pct"] is not None
                    for symbol in symbols
                ),
                five_minute_ready=sum(
                    oi_changes(self.series(source, symbol), at)["oi_change_5m_pct"] is not None
                    for symbol in symbols
                ),
                ttl_ms=TTL_MS,
            ),
        )
