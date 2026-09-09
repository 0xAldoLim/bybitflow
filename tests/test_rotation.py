import json

from bybit_flow.orderflow import Book, Tape
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Recorder, Store
from bybit_flow.streams import Streams


async def test_incremental_rotation_preserves_retained_coverage(settings):
    store = Store(settings.data_dir)
    streams = Streams(settings, store, Recorder(store, settings))
    messages = []

    class Socket:
        async def send(self, body):
            messages.append(json.loads(body))

    streams.ws, streams.connected = Socket(), True
    streams.selected = ("BTCUSDT", "OLDUSDT")
    kept = Tape(1000)
    kept.coverage_start = 12345
    streams.books = {"BTCUSDT": Book(), "OLDUSDT": Book()}
    streams.tapes = {"BTCUSDT": kept, "OLDUSDT": Tape(1000)}
    streams.liquidations = {"BTCUSDT": [], "OLDUSDT": []}
    retained_book = streams.books["BTCUSDT"]
    await streams.select(["BTCUSDT", "NEWUSDT"])
    assert streams.tapes["BTCUSDT"] is kept and kept.coverage_start == 12345
    assert streams.books["BTCUSDT"] is retained_book
    assert not streams.books["NEWUSDT"].valid and "OLDUSDT" not in streams.tapes
    assert [m["op"] for m in messages] == ["unsubscribe", "subscribe"]
    store.close()


async def test_closed_candle_cache_keyed_by_boundary(settings, bars):
    store = Store(settings.data_dir)
    scanner = Scanner(settings, store, Recorder(store, settings))
    calls = []

    async def candles(symbol, interval, asof, limit):
        calls.append(asof)
        return bars

    scanner.api.candles = candles
    await scanner.cached_candles("TESTUSDT", "60", bars[-1].end + 1000, 200)
    await scanner.cached_candles("TESTUSDT", "60", bars[-1].end + 2000, 200)
    assert len(calls) == 1
    await scanner.cached_candles("TESTUSDT", "60", bars[-1].end + 3_600_000, 200)
    assert len(calls) == 2
    await scanner.api.close()
    store.close()
