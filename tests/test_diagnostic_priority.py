import json
import sqlite3

from bybit_flow.db_runtime import DIAGNOSTIC_KEYS, MetadataWriter
from bybit_flow.storage import Store


async def test_market_diagnostic_burst_retains_and_persists_global_health(settings):
    store = Store(settings.data_dir)
    store.enable_runtime()
    try:
        for key in DIAGNOSTIC_KEYS:
            store.put(key, {"at_ms": 123456, "state": "HEALTHY"})
        # Thirty streams publish multiple display snapshots in one loop turn.
        for i in range(30):
            for prefix in ("feed:", "depth_recovery:", "liquidity:"):
                store.put(prefix + "binance:MARKET" + str(i), {"at_ms": 123456})
        assert len(store._metadata.pending) <= 64
        for key in DIAGNOSTIC_KEYS:
            assert store.get(key)["at_ms"] == 123456
        assert store.db.health.snapshot()["diagnostic_metadata_dropped"] > 0
    finally:
        await store.stop_runtime()
        store.close()
    reader = sqlite3.connect(settings.data_dir / "research.sqlite")
    try:
        for key in DIAGNOSTIC_KEYS:
            raw = reader.execute("SELECT payload FROM kv WHERE key=?", (key,)).fetchone()
            assert json.loads(raw[0])["at_ms"] == 123456
    finally:
        reader.close()


async def test_market_snapshot_cannot_displace_an_all_global_queue(settings):
    store = Store(settings.data_dir)
    writer = MetadataWriter(store.db, store.db.health, capacity=2)
    try:
        writer.offer("runtime_health", '{"at_ms":100}')
        writer.offer("clock_health", '{"at_ms":100}')
        writer.offer("feed:binance:BTCUSDT", '{"at_ms":101}')
        assert len(writer.pending) == 2
        assert writer.read("runtime_health") == (True, {"at_ms": 100})
        assert writer.read("clock_health") == (True, {"at_ms": 100})
        assert writer.read("feed:binance:BTCUSDT") == (False, None)
    finally:
        await writer.close()
        store.close()
