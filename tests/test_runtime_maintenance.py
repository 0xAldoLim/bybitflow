import asyncio
import json
from pathlib import Path

import pytest

from bybit_flow.leases import RangeLease, overlaps
from bybit_flow.packing import compact
from bybit_flow.storage import Recorder, Store


async def test_native_subscription_recovers_after_recorder_failure(settings):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from bybit_flow.native_streams import NativeStreams
    from bybit_flow.orderflow import Book, Tape

    stream = NativeStreams.__new__(NativeStreams)
    stream.sessions, stream.reconnects = {}, {}
    stream.books, stream.tapes, stream.liquidations = {"TEST": Book()}, {"TEST": Tape()}, {"TEST": []}
    stream.api = SimpleNamespace(name="okx")
    stream.store = Mock()
    stream.record = Mock()
    stream.recorder = SimpleNamespace(healthy=True)
    recovered = asyncio.Event()
    calls = []

    async def socket(symbol):
        calls.append(symbol)
        if len(calls) == 1:
            stream.recorder.healthy = False
            raise OSError("persistence unavailable")
        recovered.set()
        await asyncio.Event().wait()

    stream.okx = socket
    task = asyncio.create_task(stream.run_symbol("TEST"))
    await asyncio.sleep(0.02)
    assert not task.done()
    assert calls == ["TEST"]
    stream.recorder.healthy = True
    await asyncio.wait_for(recovered.wait(), 4)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(calls) == 2


def envelope(at):
    return dict(
        source="control/subscribed",
        symbol="TESTUSDT",
        event_ms=at,
        receipt_ms=at,
        schema_version=1,
        complete=True,
        payload=json.dumps({"symbols": ["TESTUSDT"]}),
    )


def test_unrelated_lease_allows_compaction_but_overlap_is_preserved(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    for at in (1000, 2000, 3000):
        recorder.flush([envelope(at)])
    protected = next(m for m in store.rows("segments") if m["min_receipt_ms"] == 1000)
    with RangeLease(store, 500, 1500, "reader"):
        result = compact(store)
        assert result["segments"] == 2
        assert Path(protected["raw"]).exists()
    store.close()


def test_lease_heartbeat_cursor_and_expiry(settings, monkeypatch):
    import bybit_flow.leases as leases

    store = Store(settings.data_dir)
    clock = [1_000_000]
    monkeypatch.setattr(leases, "now_ms", lambda: clock[0])
    with RangeLease(store, 0, 5000, "reader") as lease:
        assert overlaps(store, 0, 100)
        clock[0] += 60_000
        lease.heartbeat(start=2000)
        assert not overlaps(store, 0, 1000)
        assert overlaps(store, 3000, 4000)
        row = store.db.execute("SELECT * FROM storage_leases").fetchone()
        assert json.loads(row["reason"])["cursor_ms"] == 2000
        clock[0] += leases.TTL_MS + 1
        assert not overlaps(store, 3000, 4000)
    store.close()


async def test_optional_backpressure_preserves_critical_capacity_and_recovers(settings):
    cfg = settings.model_copy(update={"queue_size": 100, "recorder_segment_rows": 100})
    store = Store(cfg.data_dir)
    rec = Recorder(store, cfg)
    for i in range(90):
        rec.offer("ws/publicTrade.OPTIONAL", "OPTIONAL", i, {"data": []})
    assert rec.events_dropped == 20
    assert rec.queue.qsize() == 70
    assert rec.offer("ws/publicTrade.BTCUSDT", "BTCUSDT", 100, {"data": []})
    rec.running = False
    await rec.run()
    assert rec.healthy and rec.state == "NORMAL"
    assert rec.queue.empty()
    from bybit_flow.packing import raw_stream

    with raw_stream(store.rows("segments")[0]["raw"]) as stream:
        rows = [json.loads(row) for row in stream]
    gaps = [json.loads(r["payload"]) for r in rows if not r["complete"]]
    assert gaps and gaps[0]["coverage_gap"]
    store.close()


async def test_transient_writer_failure_retries_same_batch_without_restart(settings, monkeypatch):
    store = Store(settings.data_dir)
    cfg = settings.model_copy(update={"recorder_segment_rows": 100})
    rec = Recorder(store, cfg)
    from bybit_flow.recorder_worker import ProcessWriter

    real = ProcessWriter.write
    attempts = []

    async def flaky(writer, ident, batch):
        attempts.append(len(batch))
        if len(attempts) == 1:
            raise OSError("temporary writer failure")
        await real(writer, ident, batch)
        rec.running = False

    monkeypatch.setattr(ProcessWriter, "write", flaky)
    for i in range(100):
        rec.offer("control/test", "ALL", i, {})
    await asyncio.wait_for(rec.run(), 10)
    assert attempts == [100, 100]
    assert rec.written == 100 and rec.healthy and rec.retries == 1
    assert len(store.rows("segments")) == 1
    store.close()


def test_exclusive_maintenance_claim_blocks_new_reader(settings):
    store = Store(settings.data_dir)
    with RangeLease(store, 0, 1000, "pruning", exclusive=True):
        with pytest.raises(BlockingIOError):
            with RangeLease(store, 500, 2000, "reader"):
                pass
    assert not overlaps(store, 500, 2000)
    store.close()


def test_writer_commit_acknowledgement_retry_is_idempotent(settings):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    recorder.flush([envelope(1000)], ident="retry-batch")
    original = store.rows("segments")[0]
    recorder.flush([envelope(1000)], ident="retry-batch")
    assert len(store.rows("segments")) == 1
    assert store.rows("segments")[0] == original
    store.close()


def test_malformed_execution_is_preserved_as_gap(settings):
    from bybit_flow.packing import raw_stream

    store = Store(settings.data_dir)
    invalid = envelope(1000) | dict(
        source="ws/publicTrade.TESTUSDT",
        payload=json.dumps({"data": [dict(T=1000, p="-1", v="1", i="bad", S="Buy")]}),
    )
    Recorder(store, settings).flush([invalid])
    with raw_stream(store.rows("segments")[0]["raw"]) as stream:
        row = json.loads(next(stream))
    assert not row["complete"] and row["source"] == "control/gap"
    assert json.loads(row["payload"])["original_envelope"] == invalid
    store.close()
