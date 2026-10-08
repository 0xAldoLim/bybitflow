import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from bybit_flow.identity import claim
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store


def test_delivery_claim_uses_active_index_with_large_terminal_history(settings, signal):
    store = Store(settings.data_dir)
    payload = signal.model_dump_json()
    with store.db:
        store.db.executemany(
            "INSERT INTO signals VALUES(?,?,?,?,?)",
            ((f"old-{i}", signal.symbol, i, "EXPIRED", payload) for i in range(2000)),
        )
    store.signal(signal)
    first = claim(store, signal, 1000)
    other = signal.model_copy(update={"id": "related", "horizon_profile": "SWING"})
    store.signal(other)
    before = dict(store.db.execute("SELECT id,payload FROM signals"))
    statements = []
    store.db.set_trace_callback(statements.append)
    result = claim(store, other, 1001)
    store.db.set_trace_callback(None)
    query = next(sql for sql in statements if sql.startswith("SELECT a.cluster_id,s.payload"))
    plan = [row[3] for row in store.db.execute("EXPLAIN QUERY PLAN " + query)]
    assert any("USING INDEX signals_active_lifecycle" in step for step in plan)
    assert "SCAN s" not in plan
    assert result["relationship"] == "CONFIRMING_HORIZON"
    assert result["cluster_id"] == first["cluster_id"]
    assert result["primary"].id == signal.id
    assert dict(store.db.execute("SELECT id,payload FROM signals")) == before
    assert {s["id"] for s in store.active_signals()} == {signal.id, other.id}
    store.close()


async def test_scan_retry_clears_previous_failure_before_waiting_for_public_data(settings):
    store = Store(settings.data_dir)
    scanner = Scanner.__new__(Scanner)
    scanner.scan_lock = asyncio.Lock()
    scanner.select_source = AsyncMock()
    scanner.store = store
    scanner.status = dict(
        state="error", error_type="ValueError", reason="Old clock warning", live_feed_preserved=False
    )
    scanner.api = SimpleNamespace(get=AsyncMock(side_effect=RuntimeError("end test at public I/O")))
    with pytest.raises(RuntimeError, match="end test at public I/O"):
        await scanner.scan_once()
    saved = store.get("scanner")
    assert saved["state"] == "scanning"
    assert saved["at_ms"] == saved["started_ms"]
    assert not {"error_type", "reason", "live_feed_preserved"} & saved.keys()
    store.close()
