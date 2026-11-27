"""Raw recording is a research sink. It must never stop scanning or alerts."""

import asyncio

import pytest

from bybit_flow.cleanup_reconciliation import LATCH, reconcile_failed_cleanup, storage_blocked
from bybit_flow.storage import Recorder, StorageBudgetExceeded, Store, now_ms


@pytest.fixture
def best_effort(settings):
    return settings.model_copy(
        update={"recording_required": False, "queue_size": 100, "recorder_segment_seconds": 1}
    )


def test_default_configuration_does_not_require_recording():
    from bybit_flow.config import Settings

    assert Settings(_env_file=None).recording_required is False


async def test_full_storage_drops_raw_rows_but_stays_healthy(best_effort, monkeypatch):
    store = Store(best_effort.data_dir)
    recorder = Recorder(store, best_effort)
    writes = []

    class Writer:
        def __init__(self, owner):
            pass

        async def write(self, ident, batch):
            writes.append(len(batch))
            raise StorageBudgetExceeded("Recording budget reached")

        def close(self):
            pass

    monkeypatch.setattr("bybit_flow.recorder_worker.ProcessWriter", Writer)
    for i in range(5):
        assert recorder.offer("native/binance/ws/publicTrade.X", "BTCUSDT", i, {"i": i}) is True
    task = asyncio.create_task(recorder.run())
    try:
        for _ in range(3000):  # Generous: shared CI runners can stall for seconds.
            if recorder.suspensions:
                break
            await asyncio.sleep(0.01)
        assert recorder.suspensions >= 1 and writes
        assert recorder.healthy is True
        assert recorder.state == "STORAGE_FULL_NOT_RECORDING"
        assert recorder.pending_bytes == 0 and recorder.queue.empty()
        assert recorder.events_dropped >= 5
        # While suspended, new events are counted as gaps and never queued or raised.
        assert recorder.offer("native/binance/ws/publicTrade.X", "BTCUSDT", 9, {"i": 9}) is False
        assert recorder.queue.empty()
        await recorder.wait_for_capacity()  # Never blocks the live feed.
        assert store.get("recorder_health")["healthy"] is True
        assert "alerts continue" in store.get("recorder_gap")["reason"]
    finally:
        recorder.running = False
        await asyncio.wait_for(task, 5)


def test_queue_overflow_sheds_instead_of_failing_closed(best_effort):
    recorder = Recorder(Store(best_effort.data_dir), best_effort)
    results = [recorder.offer("fixture", "BTCUSDT", i, {"i": i}) for i in range(150)]
    assert results.count(True) == 100 and results.count(False) == 50
    assert recorder.healthy is True and recorder.overflow_count == 50


def test_required_mode_keeps_the_legacy_fail_closed_overflow(best_effort):
    cfg = best_effort.model_copy(update={"recording_required": True})
    recorder = Recorder(Store(cfg.data_dir), cfg)
    with pytest.raises(RuntimeError):
        for i in range(150):
            recorder.offer("fixture", "BTCUSDT", i, {"i": i})
    assert recorder.healthy is False


def test_scanner_is_not_storage_blocked_in_best_effort_mode(best_effort):
    from types import SimpleNamespace

    from bybit_flow.scanner_discovery import MarketDiscoveryCoordinator

    recorder = Recorder(Store(best_effort.data_dir), best_effort)
    recorder.state = "STORAGE_FULL_NOT_RECORDING"
    coordinator = MarketDiscoveryCoordinator.__new__(MarketDiscoveryCoordinator)
    coordinator.runtime = SimpleNamespace(recorder=recorder)
    assert coordinator.storage_blocked() is False


def test_interrupted_cleanup_is_retryable_not_a_permanent_block(settings):
    store = Store(settings.data_dir)
    store.put(
        LATCH,
        dict(
            status="FAILED",
            run_id="cleanup:" + "a" * 32,
            reason="Cleanup ownership expired/lost; stopped",
        ),
    )
    result = reconcile_failed_cleanup(store, fresh=True)
    assert result["state"] == "SAFE_TO_RETRY" and result["interrupted"] is True
    assert result["unverifiable"] and not storage_blocked(result)
    # The failure record itself is preserved for the next run's recovery audit.
    assert store.get(LATCH)["status"] == "FAILED"


def test_integrity_failure_without_audit_still_blocks_cleanup(settings):
    store = Store(settings.data_dir)
    store.put(
        LATCH,
        dict(
            status="FAILED",
            run_id="cleanup:" + "b" * 32,
            at_ms=now_ms(),
            reason="Cleanup durable ML row identities changed; stopped",
        ),
    )
    result = reconcile_failed_cleanup(store, fresh=True)
    assert result["state"] == "AUDIT_UNAVAILABLE" and storage_blocked(result)
