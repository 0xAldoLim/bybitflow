"""Full public REST adapter -> scanner tests using synthetic transport payloads only."""

import asyncio
import math

import httpx
import pytest

from bybit_flow.exchanges import VenueAPI
from bybit_flow.liquidity import SpreadHistory
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Recorder, Store, now_ms


@pytest.mark.parametrize("venue", ["binance", "okx"])
async def test_native_rest_to_persistent_watchlist(settings, venue):
    settings.market_source = venue
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    writer = asyncio.create_task(recorder.run())
    scanner = Scanner(settings, store, recorder)
    asof = now_ms()
    history = SpreadHistory()
    for i in range(12, 0, -1):
        known = asof - i * 300_000
        history.add(known, known, 99.99, 100.01)
    store.put(scanner.spread_key("TESTUSDT"), history.export())
    requests = []

    def handle(request):
        path, p = request.url.path, request.url.params
        requests.append(path)
        if path.endswith("/time"):
            data = {"serverTime": asof} if venue == "binance" else [{"ts": str(asof)}]
        elif path.endswith("/exchangeInfo"):
            data = {
                "symbols": [
                    dict(
                        symbol="TESTUSDT",
                        baseAsset="TEST",
                        quoteAsset="USDT",
                        marginAsset="USDT",
                        contractType="PERPETUAL",
                        status="TRADING",
                        onboardDate=asof - 100 * 86400000,
                        filters=[
                            dict(filterType="PRICE_FILTER", tickSize="0.01"),
                            dict(filterType="LOT_SIZE", stepSize="0.001", minQty="0.001", maxQty="100000"),
                            dict(filterType="MIN_NOTIONAL", notional="5"),
                        ],
                    )
                ]
            }
        elif path.endswith("/instruments"):
            data = [
                dict(
                    instType="SWAP",
                    ctType="linear",
                    settleCcy="USDT",
                    state="live",
                    instId="TEST-USDT-SWAP",
                    ctValCcy="TEST",
                    ctVal="0.01",
                    ctMult="1",
                    listTime=str(asof - 100 * 86400000),
                    tickSz="0.01",
                    lotSz="0.1",
                    minSz="0.1",
                    maxMktSz="10000000",
                    lever="50",
                )
            ]
        elif path.endswith("/bookTicker"):
            data = [dict(symbol="TESTUSDT", bidPrice="99.99", askPrice="100.01", time=asof)]
        elif path.endswith("/premiumIndex"):
            data = [
                dict(
                    symbol="TESTUSDT",
                    lastFundingRate="0.0001",
                    markPrice="100",
                    indexPrice="100",
                    nextFundingTime=asof + 3600000,
                    time=asof,
                )
            ]
        elif path.endswith("/tickers"):
            data = [dict(instId="TEST-USDT-SWAP", bidPx="99.99", askPx="100.01", ts=str(asof))]
        elif path.endswith(("/depth", "/books")):
            if venue == "binance":
                data = dict(
                    lastUpdateId=10, T=asof, E=asof, bids=[["99.99", "1000"]], asks=[["100.01", "1000"]]
                )
            else:
                data = [
                    dict(
                        ts=str(asof),
                        bids=[["99.99", "100000", "0", "1"]],
                        asks=[["100.01", "100000", "0", "1"]],
                    )
                ]
        elif path.endswith(("/klines", "/history-candles")):
            width = {
                "1d": 86400000,
                "1Dutc": 86400000,
                "4h": 14400000,
                "4H": 14400000,
                "1h": 3600000,
                "1H": 3600000,
                "15m": 900000,
            }[p.get("interval", p.get("bar"))]
            final, rows = asof // width * width, []
            for i in range(300):
                start, price = final - (300 - i) * width, 90 + i * 0.03 + math.sin(i / 4)
                if start <= int(p.get("endTime", p.get("after"))):
                    row = [start, str(price), str(price + 1), str(price - 1), str(price + 0.2)]
                    rows.append(
                        row
                        + (
                            ["300000", start + width - 1, "30000000"]
                            if venue == "binance"
                            else ["30000000", "300000", "30000000", "1"]
                        )
                    )
            data = rows[-int(p["limit"]) :]
            if venue == "okx":
                data.reverse()
        elif path.endswith("/openInterestHist"):
            data = [
                dict(timestamp=asof - 3600000, sumOpenInterest="10000", sumOpenInterestValue="1000000"),
                dict(timestamp=asof, sumOpenInterest="10100", sumOpenInterestValue="1010000"),
            ]
        elif path.endswith("/open-interest"):
            data = [dict(ts=str(asof), oi="1010000", oiCcy="10100", oiUsd="1010000")]
        elif path.endswith("/fundingRate"):
            data = [dict(fundingTime=asof - 3600000, fundingRate="0.0001", markPrice="100")]
        elif path.endswith("/funding-rate-history"):
            data = [dict(fundingTime=str(asof - 3600000), realizedRate="0.0001", fundingRate="0.0001")]
        elif path.endswith("/funding-rate"):
            data = [dict(ts=str(asof), fundingRate="0.0001", nextFundingTime=str(asof + 3600000))]
        else:
            raise AssertionError(path)
        return httpx.Response(200, json=data if venue == "binance" else {"code": "0", "data": data})

    await scanner.api.close()
    scanner.api = VenueAPI(venue, settings, recorder, httpx.MockTransport(handle))
    scanner.streams.api = scanner.api
    selected = []

    async def select(symbols):
        selected.extend(symbols)

    scanner.streams.select = select
    try:
        result = await scanner.scan_once()
        assert result["eligible"] == 1 and result["errors"] == 0
        assert store.get("watchlist")[0]["exchange"] == venue
        market = store.get("market:TESTUSDT")
        assert market["exchange"] == venue and market["derivatives"]["funding_rate"] == 0.0001
        assert market["derivatives"]["ticker_observed_ms"] == asof
        # One real-time OI sample cannot be represented as a measured OI change.
        assert (market["derivatives"]["oi_change_pct"] is None) == (venue == "okx")
        assert "TESTUSDT" in selected and requests
    finally:
        await scanner.stop()
        recorder.running = False
        await writer
        assert recorder.written > 0 and recorder.healthy
        store.close()
