"""Auditable public-only adapter: fixed host, endpoint allowlist, GET only."""

import asyncio
import random
import time
from dataclasses import asdict

import httpx

from .models import Candle, Instrument
from .storage import now_ms

PUBLIC_PATHS = {
    "time",
    "instruments-info",
    "tickers",
    "kline",
    "orderbook",
    "open-interest",
    "funding/history",
    "mark-price-kline",
    "index-price-kline",
    "risk-limit",
}


class Bybit:
    def __init__(self, settings, recorder=None, transport=None):
        self.settings, self.recorder = settings, recorder
        self.client = httpx.AsyncClient(
            base_url="https://api.bybit.com", timeout=20, transport=transport, follow_redirects=False
        )
        self.lock = asyncio.Lock()
        self.next_request = 0.0
        self.blocked_until = 0.0

    async def get(self, endpoint, **params):
        if endpoint not in PUBLIC_PATHS:
            raise ValueError("Only allowlisted public market-data endpoints are available")
        if time.monotonic() < self.blocked_until:
            raise PermissionError("Bybit public API cooldown is active")
        if self.recorder and not self.recorder.healthy:
            raise RuntimeError("Recording circuit open; no further market requests")
        for attempt in range(5):
            async with self.lock:
                await asyncio.sleep(max(0, self.next_request - time.monotonic()))
                self.next_request = time.monotonic() + 1 / self.settings.rest_requests_per_second
            try:
                r = await self.client.get("/v5/market/" + endpoint, params=params)
                if r.status_code == 403:
                    # Do not bypass regional restrictions or a blocked IP.
                    self.blocked_until = time.monotonic() + 600
                    raise PermissionError("Bybit returned 403; collection stopped for at least ten minutes")
                if r.status_code == 429 or r.status_code >= 500:
                    await asyncio.sleep(max(0, float(r.headers.get("Retry-After", 2**attempt))))
                    continue
                r.raise_for_status()
                body = r.json()
                if body.get("retCode") == 10006:
                    reset = float(r.headers.get("X-Bapi-Limit-Reset-Timestamp", now_ms() + 1000))
                    await asyncio.sleep(max(1, (reset - now_ms()) / 1000))
                    continue
                if body.get("retCode") != 0:
                    raise ValueError(f"Bybit public API code {body.get('retCode')}")
                received = now_ms()
                if self.recorder:
                    self.recorder.offer(
                        "rest/" + endpoint,
                        params.get("symbol", "UNIVERSE"),
                        body["time"],
                        {"params": params, "response": body},
                        received,
                    )
                return body
            except (httpx.TransportError, httpx.HTTPStatusError):
                if attempt == 4:
                    raise
                await asyncio.sleep(min(30, 2**attempt + random.random()))
        raise RuntimeError("Public endpoint retry budget exhausted")

    async def instruments(self):
        rows, cursor, seen = [], "", set()
        while True:
            params = dict(category="linear", limit=1000)
            if cursor:
                params["cursor"] = cursor
            body = await self.get("instruments-info", **params)
            rows.extend(body["result"]["list"])
            cursor = body["result"].get("nextPageCursor", "")
            if not cursor:
                return rows
            if cursor in seen:
                raise ValueError("Pagination cursor repeated")
            seen.add(cursor)

    async def candles(self, symbol, interval, asof, limit=200, start=None):
        width = 86_400_000 if interval == "D" else int(interval) * 60_000
        end, rows = asof - 1, {}
        if start is not None:
            end = min(end, start + limit * width - 1)
        while len(rows) < limit:
            params = dict(
                category="linear",
                symbol=symbol,
                interval=interval,
                end=end,
                limit=min(1000, limit - len(rows) + 1),
            )
            if start is not None:
                params["start"] = start
            data = (await self.get("kline", **params))["result"]["list"]
            if not data:
                break
            earliest = min(int(x[0]) for x in data)
            if earliest > end:
                raise ValueError("Candle pagination made no progress")
            for x in data:
                c = Candle(int(x[0]), width, *map(float, x[1:7]))
                if c.end <= asof and (start is None or c.start >= start):
                    rows[c.start] = c
            end = earliest - 1
            if start is not None and end < start:
                break
        return sorted(rows.values(), key=lambda c: c.start)[-limit:]

    async def history(self, endpoint, symbol, start, end):
        if endpoint not in {"open-interest", "funding/history"}:
            raise ValueError("Unsupported history")
        rows, cursor, seen = [], "", set()
        while end >= start:
            params = dict(category="linear", symbol=symbol, startTime=start, endTime=end, limit=200)
            if endpoint == "open-interest":
                params["intervalTime"] = "1h"
                if cursor:
                    params["cursor"] = cursor
            result = (await self.get(endpoint, **params))["result"]
            page = result["list"]
            rows.extend(page)
            if not page:
                break
            if endpoint == "open-interest":
                cursor = result.get("nextPageCursor", "")
                if not cursor:
                    break
                if cursor in seen:
                    raise ValueError("History cursor repeated")
                seen.add(cursor)
            else:
                new_end = min(int(x["fundingRateTimestamp"]) for x in page) - 1
                if new_end >= end:
                    raise ValueError("Funding pagination made no progress")
                end = new_end
        return rows

    async def close(self):
        await self.client.aclose()


def parse_eligible_metadata(row, settings, asof):
    if (
        row.get("contractType") != "LinearPerpetual"
        or row.get("status") != "Trading"
        or row.get("settleCoin") not in settings.settle_coins
        or row.get("isPreListing", False)
        or row.get("marketRegion")
        or row.get("underlyingTicker")
    ):
        return None
    try:
        i = Instrument.parse(row)
        if (
            min(i.tick, i.qty_step, i.max_qty) <= 0
            or i.launch_ms <= 0
            or asof - i.launch_ms < settings.min_age_days * 86_400_000
        ):
            return None
        return i
    except (ValueError, KeyError, TypeError):
        return None


def candle_records(candles):
    return [asdict(c) for c in candles]
