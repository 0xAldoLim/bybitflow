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


async def test_short_fresh_history_reused_but_larger_requests_and_stale_history_retry(settings, bars):
    store = Store(settings.data_dir)
    scanner = Scanner(settings, store, Recorder(store, settings))
    calls = []
    short = bars[-30:]

    async def candles(symbol, interval, asof, limit):
        calls.append(limit)
        return short

    scanner.api.candles = candles
    at = bars[-1].end + 1000
    assert await scanner.cached_candles("NEWUSDT", "60", at, 200) == short
    assert await scanner.cached_candles("NEWUSDT", "60", at + 1000, 200) == short
    assert calls == [200]
    await scanner.cached_candles("NEWUSDT", "60", at, 300)
    await scanner.cached_candles("NEWUSDT", "60", at + 1000, 200)
    assert calls == [200, 300]
    # A new boundary refreshes; stale partial responses remain retryable.
    await scanner.cached_candles("NEWUSDT", "60", at + 3_600_000, 200)
    await scanner.cached_candles("NEWUSDT", "60", at + 3_601_000, 200)
    assert calls == [200, 300, 200, 200]
    await scanner.api.close()
    await scanner.spot_collector.close()
    store.close()


def test_runtime_indexes_preserve_all_active_setups_and_original_snapshot(settings, signal):
    from bybit_flow.ml.store import FeatureStore

    store = Store(settings.data_dir)
    states = ["ALERTED", "PENDING CONFIRMATION", "CONFIRMED", "RECONCILING", "EXPIRED", "RESOLVED"]
    with store.db:
        for number, state in enumerate(states):
            row = dict(id=str(number), state=state, created_ms=number)
            store.db.execute(
                "INSERT INTO signals VALUES(?,?,?,?,?)",
                (str(number), "TESTUSDT", number, state, json.dumps(row)),
            )
        for ident, schema, at in (("original", "candidate-v9", 1), ("later", "candidate-v10", 2)):
            store.db.execute(
                "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
                (ident, signal.id, "generation", at, schema, '{"immutable":true}'),
            )
    assert [row["id"] for row in store.active_signals()] == ["0", "3", "2", "1"]
    assert FeatureStore(store).capture(signal, 3, "generation") == "original"
    assert store.db.execute("SELECT count(*) FROM ml_snapshots").fetchone()[0] == 2
    plan = store.db.execute(
        "EXPLAIN QUERY PLAN SELECT id FROM ml_snapshots WHERE signal_id=? AND stage=? "
        "ORDER BY decision_ms,id LIMIT 1",
        (signal.id, "generation"),
    ).fetchall()
    assert any("ml_snapshot_original_lookup" in row[3] for row in plan)
    store.close()
