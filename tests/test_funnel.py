import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.funnel import emit, status
from bybit_flow.notifications import Notifier
from bybit_flow.observations import refresh_recommendations
from bybit_flow.storage import Store


def test_funnel_idempotency_transactions_and_test_exclusion(settings, signal):
    store = Store(settings.data_dir)
    store.signal(signal)
    store.signal(signal)
    assert status(store)["cumulative"]["candidates_generated"] == 1
    signal.synthetic = True
    emit(store, "confirmed", 120000, signal=signal)
    assert status(store)["cumulative"]["confirmed"] == 0
    with pytest.raises(RuntimeError), store.db:
        store.db.execute("INSERT INTO kv VALUES('rollback','{}')")
        emit(store, "confirmed", 120000, key="rolled-back")
        raise RuntimeError("abort outer transaction")
    assert status(store)["cumulative"]["confirmed"] == 0
    emit(store, "flow_rejected", 180000, reason="NO_FLOW", key="a")
    emit(store, "flow_rejected", 180000, reason="NO_FLOW", key="a")
    report = status(store, now=200000)
    assert report["windows"]["15m"]["flow_rejected"] == 1
    assert report["top_rejections"][0]["reason"] == "NO_FLOW"
    store.close()


def test_confirmation_failure_not_hidden_by_earlier_delivery(settings):
    store = Store(settings.data_dir)
    emit(store, "discord_sent", 180000)
    store.put("confirmation_health", dict(state="DEGRADED"))
    assert status(store, now=200000)["diagnosis"] == "CONFIRMATION_RUNTIME_ERROR"
    store.close()


def test_refresh_recommendations_executes_bound_query(settings):
    refresh_recommendations(settings.data_dir, 200000)
    store = Store(settings.data_dir)
    assert store.get("horizon_recommendations") == dict(available_ms=200000, families={})
    store.close()


@pytest.mark.asyncio
async def test_connection_test_not_counted_as_real_delivery(settings):
    store = Store(settings.data_dir)
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/test/test")
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"id": "test-message"}))
    notifier = Notifier(settings, store, transport)
    assert (
        await notifier.deliver("operator-test:1", None, {}, settings.research_webhook.get_secret_value())
        == "sent"
    )
    assert status(store)["cumulative"]["discord_sent"] == 0
    assert (
        await notifier.deliver(
            "research:real:initial", "real", {}, settings.research_webhook.get_secret_value()
        )
        == "sent"
    )
    assert status(store)["cumulative"]["discord_sent"] == 1
    assert status(store)["cumulative"]["discord_http_attempts"] == 1
    store.close()
