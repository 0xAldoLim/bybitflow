import asyncio
import math

import httpx

from bybit_flow.ingestion import Bybit
from bybit_flow.liquidity import SpreadHistory
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Recorder, Store, now_ms


async def test_full_rest_scan_to_ranked_persistent_watchlist(settings, instrument_row):
    """Synthetic official-schema transport; no network and no profitability assertion."""
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    writer = asyncio.create_task(recorder.run())
    scanner = Scanner(settings, store, recorder)
    asof = now_ms()
    history = SpreadHistory()
    for i in range(12, 0, -1):
        known = asof - i * 300_000
        history.add(known, known, 99.99, 100.01)
    store.put("spreads:TESTUSDT", history.export())
    instrument_row["launchTime"] = str(asof - 100 * 86_400_000)
    requests = []

    def handle(request):
        endpoint = request.url.path.removeprefix("/v5/market/")
        params = request.url.params
        requests.append(endpoint)
        if endpoint == "time":
            result = {"timeSecond": str(asof // 1000)}
        elif endpoint == "instruments-info":
            result = {"list": [instrument_row], "nextPageCursor": ""}
        elif endpoint == "tickers":
            result = {
                "list": [
                    {
                        "symbol": "TESTUSDT",
                        "bid1Price": "99.99",
                        "ask1Price": "100.01",
                        "fundingRate": "0.0001",
                        "nextFundingTime": str(asof + 3600000),
                        "openInterestValue": "10000000",
                        "markPrice": "100",
                        "indexPrice": "100",
                    }
                ]
            }
        elif endpoint == "orderbook":
            result = {
                "s": "TESTUSDT",
                "u": 10,
                "seq": 100,
                "cts": asof,
                "b": [["99.99", "1000"]],
                "a": [["100.01", "1000"]],
            }
        elif endpoint == "kline":
            width = 86_400_000 if params["interval"] == "D" else int(params["interval"]) * 60000
            final = asof // width * width
            rows = []
            for i in range(300):
                start = final - (300 - i) * width
                p = 90 + i * 0.03 + math.sin(i / 4)
                if start <= int(params["end"]):
                    rows.append(
                        [str(start), str(p), str(p + 1), str(p - 1), str(p + 0.2), "300000", "30000000"]
                    )
            result = {"list": list(reversed(rows))[: int(params["limit"])], "symbol": "TESTUSDT"}
        elif endpoint == "open-interest":
            result = {
                "list": [
                    {"timestamp": str(asof - 3600000), "openInterest": "10000"},
                    {"timestamp": str(asof), "openInterest": "10100"},
                ],
                "nextPageCursor": "",
            }
        elif endpoint == "funding/history":
            result = {
                "list": [{"fundingRateTimestamp": str(asof - 3600000), "fundingRate": ".0001"}]
                if int(params["endTime"]) >= asof - 3600000
                else []
            }
        else:
            raise AssertionError(endpoint)
        return httpx.Response(200, json={"retCode": 0, "time": asof, "result": result})

    await scanner.api.close()
    scanner.api = Bybit(settings, recorder, httpx.MockTransport(handle))
    selected = []

    async def select(symbols):
        selected.extend(symbols)

    scanner.streams.select = select
    try:
        result = await scanner.scan_once()
        assert result["discovered"] == 1 and result["eligible"] == 1 and result["errors"] == 0
        assert store.get("watchlist")[0]["symbol"] == "TESTUSDT"
        assert selected == ["TESTUSDT"]
        assert store.get("market:TESTUSDT")["derivatives"]["oi_change_pct"] > 0
        assert {"kline", "tickers", "orderbook", "open-interest", "funding/history"} <= set(requests)
    finally:
        await scanner.stop()
        recorder.running = False
        await writer
        assert recorder.written > 0 and recorder.healthy
        store.close()
