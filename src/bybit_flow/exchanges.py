"""Public-only venue adapters. No credentials, signing, private paths or order methods.

The scanner-v1 view deliberately retains the existing scanner's field names. These
are INTERNAL normalized records, not purported Binance/OKX wire payloads. Original
responses are recorded separately under raw/<venue> with their actual schema.
All quantities handed to strategy/risk/Book are base units, including OKX contracts.
"""

import asyncio
import json
import re
from decimal import Decimal as D
from typing import Protocol

import httpx
from websockets.asyncio.client import connect

from .ingestion import Bybit, parse_eligible_metadata
from .models import DAY, Candle, Instrument, Trade
from .storage import now_ms

VENUES = ("binance", "bybit", "okx")
BASES = {
    "binance": "https://fapi.binance.com",
    "bybit": "https://api.bybit.com",
    "okx": "https://www.okx.com",
}
PATHS = {
    "binance": {
        "/fapi/v1/time",
        "/fapi/v1/exchangeInfo",
        "/fapi/v1/ticker/bookTicker",
        "/fapi/v1/premiumIndex",
        "/fapi/v1/fundingInfo",
        "/fapi/v1/klines",
        "/fapi/v1/depth",
        "/fapi/v1/fundingRate",
        "/futures/data/openInterestHist",
    },
    "okx": {
        "/api/v5/public/time",
        "/api/v5/public/instruments",
        "/api/v5/market/tickers",
        "/api/v5/market/history-candles",
        "/api/v5/market/books",
        "/api/v5/public/funding-rate",
        "/api/v5/public/funding-rate-history",
        "/api/v5/public/open-interest",
        "/api/v5/public/mark-price",
    },
}


class PublicMarket(Protocol):
    name: str

    async def instruments(self): ...
    async def candles(self, symbol, interval, asof, limit=200, start=None): ...
    async def close(self): ...


def symbol_id(symbol, venue):
    if not re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol):
        raise ValueError("Expected canonical USDT perpetual symbol")
    return symbol[:-4] + "-USDT-SWAP" if venue == "okx" else symbol


def instrument_from_wire(venue, row, funding_minutes=None):
    if venue == "binance":
        if (
            row.get("contractType") != "PERPETUAL"
            or row.get("status") != "TRADING"
            or row.get("marginAsset") != "USDT"
            or row.get("quoteAsset") != "USDT"
        ):
            return None
        filters = {f["filterType"]: f for f in row["filters"]}
        lot = filters.get("MARKET_LOT_SIZE", filters["LOT_SIZE"])
        return Instrument(
            exchange=venue,
            exchange_symbol=row["symbol"],
            symbol=row["symbol"],
            base=row["baseAsset"],
            settle="USDT",
            launch_ms=int(row["onboardDate"]),
            tick=filters["PRICE_FILTER"]["tickSize"],
            qty_step=lot["stepSize"],
            min_qty=lot["minQty"],
            max_qty=lot["maxQty"],
            min_notional=filters.get("MIN_NOTIONAL", {}).get("notional", "0"),
            # Public exchangeInfo does not expose account leverage brackets.
            max_leverage=1,
            funding_interval_minutes=funding_minutes or 60,
            metadata={
                "wire": row,
                "leverage_limit": "unavailable; illustrative 1x only",
                "funding_interval": "observed" if funding_minutes else "unknown; reserve hourly",
            },
        )
    if venue == "okx":
        if (
            row.get("instType") != "SWAP"
            or row.get("ctType") != "linear"
            or row.get("settleCcy") != "USDT"
            or row.get("state") != "live"
        ):
            return None
        parts = row["instId"].split("-")
        if len(parts) != 3 or parts[1:] != ["USDT", "SWAP"] or row.get("ctValCcy") != parts[0]:
            return None  # Do not silently convert inverse or quote-valued contracts.
        multiplier = D(row["ctVal"]) * D(row.get("ctMult") or "1")
        return Instrument(
            exchange=venue,
            exchange_symbol=row["instId"],
            symbol=parts[0] + "USDT",
            base=parts[0],
            settle="USDT",
            launch_ms=int(row["listTime"]),
            tick=row["tickSz"],
            qty_step=D(row["lotSz"]) * multiplier,
            min_qty=D(row["minSz"]) * multiplier,
            max_qty=D(row.get("maxMktSz") or "0") * multiplier,
            min_notional="0",
            contract_multiplier=multiplier,
            max_leverage=float(row.get("lever") or 1),
            funding_interval_minutes=funding_minutes or 60,
            metadata={"wire": row, "funding_interval": "unknown; reserve hourly"},
        )
    raise ValueError("Unsupported instrument schema")


def trade_from_wire(venue, row, receipt, instrument=None):
    if venue == "binance":
        return Trade(
            row["s"],
            int(row["T"]),
            receipt,
            str(row["a"]),
            "Sell" if row["m"] else "Buy",
            D(row["p"]),
            D(row["q"]),
            venue,
            row["s"],
        )
    if venue == "bybit":
        return Trade(
            row["s"], int(row["T"]), receipt, row["i"], row["S"], D(row["p"]), D(row["v"]), venue, row["s"]
        )
    if venue == "okx" and instrument and row["instId"] == instrument.exchange_symbol:
        return Trade(
            instrument.symbol,
            int(row["ts"]),
            receipt,
            row["tradeId"],
            {"buy": "Buy", "sell": "Sell"}[row["side"]],
            D(row["px"]),
            D(row["sz"]) * instrument.contract_multiplier,
            venue,
            row["instId"],
        )
    raise ValueError("Trade requires verified contract specification")


class VenueAPI:
    def __init__(self, name, settings, recorder=None, transport=None):
        if name not in VENUES:
            raise ValueError("Unsupported exchange")
        self.name, self.settings, self.recorder = name, settings, recorder
        self.bybit = Bybit(settings, recorder, transport) if name == "bybit" else None
        self.client = httpx.AsyncClient(
            base_url=BASES[name], timeout=15, transport=transport, follow_redirects=False
        )
        self.lock, self.next_request, self.blocked_until = asyncio.Lock(), 0.0, 0.0
        self.metadata, self.oi_samples = {}, {}
        self.health = {"exchange": name, "rest": "UNAVAILABLE", "ws": "DISCONNECTED"}

    async def request(self, path, **params):
        if path not in PATHS.get(self.name, set()):
            raise ValueError("Only audited public GET endpoints are allowed")
        loop = asyncio.get_running_loop()
        if loop.time() < self.blocked_until:
            raise PermissionError("Exchange request circuit is open")
        for attempt in range(4):
            async with self.lock:
                await asyncio.sleep(max(0, self.next_request - loop.time()))
                self.next_request = loop.time() + 1 / self.settings.rest_requests_per_second
            if self.recorder and not self.recorder.healthy:
                raise RuntimeError("Recording circuit open")
            response = await self.client.get(path, params=params)
            if response.status_code in {403, 418, 451}:
                self.blocked_until = loop.time() + 600
                raise PermissionError("Official endpoint denied access; no bypass or immediate retry")
            if response.status_code == 429:
                retry = response.headers.get("Retry-After", "60")
                self.blocked_until = loop.time() + min(600, max(1, float(retry)))
                raise PermissionError("Exchange rate limit; circuit open")
            if response.status_code >= 500 and attempt < 3:
                await asyncio.sleep(2**attempt)
                continue
            response.raise_for_status()
            body = response.json()
            if self.name == "okx" and body.get("code") != "0":
                raise ValueError("OKX public response error")
            receipt = now_ms()
            self.health.update(rest="HEALTHY", rest_receipt_ms=receipt)
            if self.recorder:
                self.recorder.offer(
                    f"raw/{self.name}/rest{path}",
                    params.get("symbol", params.get("instId", "UNIVERSE")),
                    receipt,
                    {
                        "exchange": self.name,
                        "params": params,
                        "response": body,
                        "event_time_quality": "endpoint-dependent; envelope is receipt time",
                    },
                    receipt,
                )
            return body
        raise RuntimeError("Public request failed")

    async def instruments(self):
        if self.bybit:
            return await self.bybit.instruments()
        rows = (
            (await self.request("/fapi/v1/exchangeInfo"))["symbols"]
            if self.name == "binance"
            else (await self.request("/api/v5/public/instruments", instType="SWAP"))["data"]
        )
        result = []
        for row in rows:
            inst = instrument_from_wire(self.name, row)
            if inst:
                self.metadata[inst.symbol] = inst
                result.append({"symbol": inst.symbol, "normalized_instrument": inst.model_dump(mode="json")})
        self.health["metadata_ms"] = now_ms()
        return result

    def parse(self, row, asof):
        if self.bybit:
            return parse_eligible_metadata(row, self.settings, asof)
        inst = Instrument.model_validate(row["normalized_instrument"])
        if (
            min(inst.tick, inst.qty_step, inst.min_qty, inst.max_qty, inst.contract_multiplier) <= 0
            or inst.launch_ms <= 0
            or asof - inst.launch_ms < self.settings.min_age_days * DAY
        ):
            return None
        return inst

    async def get(self, endpoint, **params):
        """Scanner-v1 normalized view. Raw wire observations are NEVER recorded as this view."""
        if self.bybit:
            return await self.bybit.get(endpoint, **params)
        if endpoint == "time":
            b = await self.request("/fapi/v1/time" if self.name == "binance" else "/api/v5/public/time")
            return {"time": int(b["serverTime"] if self.name == "binance" else b["data"][0]["ts"])}
        if endpoint == "tickers":
            if self.name == "binance":
                quotes = await self.request("/fapi/v1/ticker/bookTicker")
                marks = {r["symbol"]: r for r in await self.request("/fapi/v1/premiumIndex")}
                rows = [
                    dict(
                        symbol=r["symbol"],
                        bid1Price=r["bidPrice"],
                        ask1Price=r["askPrice"],
                        observed_ms=int(r["time"]),
                        funding_observed_ms=int(marks.get(r["symbol"], {}).get("time") or 0),
                        fundingRate=marks.get(r["symbol"], {}).get("lastFundingRate"),
                        markPrice=marks.get(r["symbol"], {}).get("markPrice"),
                        indexPrice=marks.get(r["symbol"], {}).get("indexPrice"),
                        nextFundingTime=marks.get(r["symbol"], {}).get("nextFundingTime"),
                    )
                    for r in quotes
                ]
            else:
                data = (await self.request("/api/v5/market/tickers", instType="SWAP"))["data"]
                rows = [
                    dict(
                        symbol=r["instId"].replace("-USDT-SWAP", "USDT"),
                        bid1Price=r["bidPx"],
                        ask1Price=r["askPx"],
                        observed_ms=int(r["ts"]),
                    )
                    for r in data
                    if r["instId"].endswith("-USDT-SWAP")
                ]
            return {"time": now_ms(), "result": {"list": rows}, "exchange": self.name}
        if endpoint == "orderbook":
            symbol = params["symbol"]
            if self.name == "binance":
                b = await self.request("/fapi/v1/depth", symbol=symbol, limit=1000)
                event = int(b["T"])
                data = dict(b=b["bids"], a=b["asks"], u=int(b["lastUpdateId"]), seq=int(b["lastUpdateId"]))
            else:
                b = (await self.request("/api/v5/market/books", instId=symbol_id(symbol, self.name), sz=400))[
                    "data"
                ][0]
                multiplier = self.metadata[symbol].contract_multiplier
                event = int(b["ts"])
                data = dict(
                    b=[[r[0], str(D(r[1]) * multiplier)] for r in b["bids"]],
                    a=[[r[0], str(D(r[1]) * multiplier)] for r in b["asks"]],
                    u=1,
                    seq=1,
                )
            return {"time": event, "result": data, "exchange": self.name}
        raise ValueError("Unknown scanner market-data operation")

    async def candles(self, symbol, interval, asof, limit=200, start=None):
        if self.bybit:
            return await self.bybit.candles(symbol, interval, asof, limit=limit, start=start)
        minutes = {"D": 1440, "240": 240, "60": 60, "15": 15}[interval]
        width, end, result = minutes * 60000, asof - 1, {}
        while len(result) < limit:
            count = min(100 if self.name == "okx" else 499, limit - len(result) + 1)
            if self.name == "binance":
                rows = await self.request(
                    "/fapi/v1/klines",
                    symbol=symbol,
                    interval={"D": "1d", "240": "4h", "60": "1h", "15": "15m"}[interval],
                    limit=count,
                    endTime=end,
                )
            else:
                rows = (
                    await self.request(
                        "/api/v5/market/history-candles",
                        instId=symbol_id(symbol, self.name),
                        bar={"D": "1Dutc", "240": "4H", "60": "1H", "15": "15m"}[interval],
                        limit=count,
                        after=end,
                    )
                )["data"]
            if not rows:
                break
            oldest = min(int(r[0]) for r in rows)
            if oldest > end:
                raise ValueError("Candle pagination made no progress")
            for r in rows:
                if self.name == "okx" and r[8] != "1":
                    continue
                c = Candle(
                    int(r[0]),
                    width,
                    *map(float, r[1:5]),
                    float(r[5 if self.name == "binance" else 6]),
                    float(r[7]),
                )
                if c.end <= asof and (start is None or c.start >= start):
                    result[c.start] = c
            if start is not None and oldest <= start:
                break
            end = oldest - 1
        return sorted(result.values(), key=lambda c: c.start)[-limit:]

    async def history(self, endpoint, symbol, start, end):
        if self.bybit:
            return await self.bybit.history(endpoint, symbol, start, end)
        if self.name == "binance":
            if endpoint == "open-interest":
                rows = await self.request(
                    "/futures/data/openInterestHist",
                    symbol=symbol,
                    period="1h",
                    startTime=start,
                    endTime=end,
                    limit=500,
                )
                return [
                    dict(
                        timestamp=r["timestamp"],
                        openInterest=r["sumOpenInterest"],
                        notional=r["sumOpenInterestValue"],
                    )
                    for r in rows
                ]
            rows = await self.request(
                "/fapi/v1/fundingRate", symbol=symbol, startTime=start, endTime=end, limit=1000
            )
            return [
                dict(
                    fundingRateTimestamp=r["fundingTime"],
                    fundingRate=r["fundingRate"],
                    mark=r.get("markPrice"),
                )
                for r in rows
            ]
        if endpoint == "open-interest":
            row = (
                await self.request(
                    "/api/v5/public/open-interest", instType="SWAP", instId=symbol_id(symbol, self.name)
                )
            )["data"][0]
            # OKX current OI is not historical OI. Build history only from actual observations.
            sample = dict(timestamp=int(row["ts"]), openInterest=row["oiCcy"], notional=row.get("oiUsd"))
            history = self.oi_samples.setdefault(symbol, [])
            if not history or sample["timestamp"] > history[-1]["timestamp"]:
                history.append(sample)
            history[:] = [r for r in history[-500:] if int(r["timestamp"]) >= start]
            return history.copy()
        rows = (
            await self.request(
                "/api/v5/public/funding-rate-history", instId=symbol_id(symbol, self.name), limit=100
            )
        )["data"]
        return [
            dict(fundingRateTimestamp=r["fundingTime"], fundingRate=r["realizedRate"] or r["fundingRate"])
            for r in rows
            if start <= int(r["fundingTime"]) <= end
        ]

    async def funding_now(self, symbol):
        if self.name != "okx":
            return None
        return (await self.request("/api/v5/public/funding-rate", instId=symbol_id(symbol, self.name)))[
            "data"
        ][0]

    async def close(self):
        await self.client.aclose()
        if self.bybit:
            await self.bybit.close()


async def market_probe(name, settings, timeout=12):
    """Independent REST + public trade WS tests. Never calls private APIs or writes candidates."""
    api = VenueAPI(name, settings)
    report = {"exchange": name, "rest": "UNAVAILABLE", "ws": "UNAVAILABLE", "at_ms": now_ms()}
    try:
        try:
            t = now_ms()
            async with asyncio.timeout(timeout):
                event = (await api.get("time"))["time"]
            report.update(
                rest="HEALTHY" if abs(now_ms() - event) <= 2000 else "STALE",
                rest_latency_ms=now_ms() - t,
                clock_skew_ms=now_ms() - event,
            )
        except Exception as exc:
            report["rest_error"] = type(exc).__name__  # no URLs/proxy credentials/error bodies
        try:
            async with asyncio.timeout(timeout):
                inst = None
                if name == "okx":
                    await api.instruments()
                    inst = api.metadata["BTCUSDT"]
                url = {
                    "binance": "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
                    "bybit": "wss://stream.bybit.com/v5/public/linear",
                    "okx": "wss://ws.okx.com:8443/ws/v5/public",
                }[name]
                async with connect(url, open_timeout=timeout, max_queue=16, max_size=1_000_000) as ws:
                    if name == "bybit":
                        await ws.send(json.dumps({"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}))
                    elif name == "okx":
                        await ws.send(
                            json.dumps(
                                {
                                    "op": "subscribe",
                                    "args": [{"channel": "trades", "instId": "BTC-USDT-SWAP"}],
                                }
                            )
                        )
                    while True:
                        body = json.loads(await ws.recv())
                        if name == "binance" and body.get("e") == "aggTrade":
                            trade = trade_from_wire(name, body, now_ms())
                        elif name == "bybit" and body.get("topic") == "publicTrade.BTCUSDT":
                            trade = trade_from_wire(name, body["data"][-1], now_ms())
                        elif (
                            name == "okx"
                            and body.get("arg", {}).get("channel") == "trades"
                            and body.get("data")
                        ):
                            trade = trade_from_wire(name, body["data"][-1], now_ms(), inst)
                        else:
                            continue
                        age = now_ms() - trade.event_ms
                        report.update(
                            ws="HEALTHY" if -1000 <= age <= settings.trade_stale_ms else "STALE",
                            symbol=trade.symbol,
                            public_trade_price=str(trade.price),
                            freshness_ms=age,
                            trade_id=trade.trade_id,
                            event_ms=trade.event_ms,
                            receipt_ms=trade.receipt_ms,
                        )
                        break
        except Exception as exc:
            report["ws_error"] = type(exc).__name__
    finally:
        await api.close()
    report["status"] = "HEALTHY" if report["rest"] == report["ws"] == "HEALTHY" else "UNAVAILABLE"
    return report
