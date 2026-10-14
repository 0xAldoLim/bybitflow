import json
from types import SimpleNamespace

from bybit_flow.app import create_app
from bybit_flow.storage import Store


async def test_overview_keeps_list_states_and_full_evidence_remains_in_detail(settings, signal):
    store = Store(settings.data_dir)
    signal.state = "EXPIRED"
    signal.coverage.update(tracking_end=True, monitoring="paused")
    signal.evidence["expiry_mark"] = dict(
        estimated_net_r=0.75,
        method="LIVE_EXECUTED_TRADE",
        estimated_net_pct=2.0,
        mark_price=105,
    )
    signal.evidence["raw_research_record"] = "x" * 1_000_000
    store.signal(signal)
    original = store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0]
    recorder = SimpleNamespace(
        healthy=True,
        reason="recording",
        metrics=lambda: {},
        queue=SimpleNamespace(qsize=lambda: 0),
        written=0,
        disk_bytes=0,
    )
    scanner = SimpleNamespace(status=dict(state="collecting"), exchange=signal.source, v8_cache={})
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(store=store, recorder=recorder, scanner=scanner))
    )
    app = create_app(settings)
    overview = next(route.endpoint for route in app.routes if route.path == "/api/overview")
    detail = next(route.endpoint for route in app.routes if route.path == "/api/signals/{ident}")
    response = await overview(request)
    assert "warnings" in response
    row = response["signals"][0]
    for field in (
        "id",
        "symbol",
        "direction",
        "family",
        "horizon_profile",
        "quality",
        "final_tier",
        "state",
        "created_ms",
    ):
        assert row[field] == getattr(signal, field)
    assert row["coverage"] == signal.coverage
    assert row["evidence"]["expiry_mark"] == signal.evidence["expiry_mark"]
    assert len(json.dumps(response)) < 10_000
    complete = await detail(signal.id, request)
    assert complete["signal"].evidence["raw_research_record"] == signal.evidence["raw_research_record"]
    assert store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0] == original
    store.close()


def test_dashboard_summary_retains_recent_order_and_limit(settings, signal):
    store = Store(settings.data_dir)
    for at in (100, 300, 200):
        row = signal.model_copy(update={"id": str(at), "created_ms": at})
        store.signal(row)
    assert [s["id"] for s in store.signal_summaries(2)] == ["300", "200"]
    store.close()
