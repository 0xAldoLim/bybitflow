import asyncio
import json

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.notifications import Notifier
from bybit_flow.storage import Store


@pytest.mark.parametrize("remaining,expected", [(0, "expired"), (4000, "too-short")])
async def test_initial_needs_time_to_complete_http(settings, signal, monkeypatch, remaining, expected):
    clock = 100_000
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: clock)
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    signal.expires_ms = clock + 60_000
    signal.trigger_expires_ms = clock + remaining
    store = Store(settings.data_dir)
    store.signal(signal)
    calls = []
    notifier = Notifier(
        settings,
        store,
        httpx.MockTransport(lambda r: calls.append(r) or httpx.Response(200, json={"id": "unexpected"})),
    )
    before = store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0]
    assert await notifier.send_research(signal) == "blocked:entry-window-" + expected
    assert calls == []
    assert store.db.execute("SELECT count(*) FROM initial_alert_claims").fetchone()[0] == 0
    assert store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0] == before
    store.close()


async def test_claim_latency_cannot_dispatch_near_expiry(settings, signal, monkeypatch):
    from bybit_flow.identity import claim

    clock = {"now": 100_000}
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: clock["now"])
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    signal.expires_ms = 200_000
    signal.trigger_expires_ms = 120_000
    store = Store(settings.data_dir)
    store.signal(signal)

    def delayed_claim(*args):
        result = claim(*args)
        clock["now"] += 16_000  # Recorded PENDLE claim used most of the remaining entry window.
        return result

    monkeypatch.setattr("bybit_flow.identity.claim", delayed_claim)
    calls = []
    notifier = Notifier(
        settings,
        store,
        httpx.MockTransport(lambda r: calls.append(r) or httpx.Response(200, json={"id": "unexpected"})),
    )
    assert await notifier.send_research(signal) == "blocked:entry-window-too-short"
    assert calls == []
    assert not notifier.was_initially_delivered(signal.id, "research")
    assert (
        store.db.execute("SELECT status FROM real_delivery_attempts").fetchone()[0]
        == "blocked:entry-window-too-short"
    )
    assert (
        store.db.execute("SELECT n FROM funnel_totals WHERE metric='discord_http_attempts'").fetchone()
        is None
    )
    assert store.active_signals()[0]["trigger_expires_ms"] == 120_000
    store.close()


@pytest.mark.parametrize(
    "event,expected", [("terminal", "terminal-setup"), ("expired", "blocked:entry-window-expired")]
)
async def test_lifecycle_change_during_client_setup_blocks_stale_initial(
    settings, signal, monkeypatch, event, expected
):
    clock = {"now": 100_000}
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: clock["now"])
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    signal.expires_ms = 200_000
    store = Store(settings.data_dir)
    store.signal(signal)
    enter = httpx.AsyncClient.__aenter__

    async def terminal_enter(client):
        if event == "terminal":
            terminal = signal.model_copy(update={"state": "EXPIRED"}, deep=True)
            store.signal(terminal, "Original entry deadline elapsed")
        else:
            clock["now"] = signal.expires_ms
        return await enter(client)

    monkeypatch.setattr(httpx.AsyncClient, "__aenter__", terminal_enter)
    calls = []
    notifier = Notifier(
        settings,
        store,
        httpx.MockTransport(lambda r: calls.append(r) or httpx.Response(200, json={"id": "unexpected"})),
    )
    assert await notifier.send_research(signal) == expected
    assert calls == []
    assert store.db.execute("SELECT status FROM outbox").fetchone()[0] == expected
    assert not notifier.was_initially_delivered(signal.id, "research")
    assert store.signals()[0]["state"] == ("EXPIRED" if event == "terminal" else signal.state)
    store.close()


async def test_valid_initial_and_later_terminal_edit_keep_visibility(settings, signal, monkeypatch):
    clock = {"now": 100_000}
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: clock["now"])
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    signal.expires_ms = 200_000
    signal.trigger_expires_ms = 130_000
    store = Store(settings.data_dir)
    store.signal(signal)
    calls = []

    def transport(request):
        calls.append((request.method, json.loads(request.content)))
        return httpx.Response(200, json={"id": "initial", "channel_id": "signals"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    assert await notifier.send_research(signal) == "sent"
    assert notifier.was_initially_delivered(signal.id, "research")
    clock["now"] = 200_001
    signal.state = "EXPIRED"
    store.signal(signal, "Original entry deadline elapsed")
    await notifier.retry_terminals()
    assert [method for method, _ in calls] == ["POST", "PATCH"]
    assert calls[1][1]["embeds"][0]["title"].startswith("ENTRY EXPIRED")
    store.close()


async def test_initial_total_timeout_is_uncertain_without_retry(settings, signal, monkeypatch):
    from bybit_flow import notifications

    monkeypatch.setattr(notifications, "now_ms", lambda: 100_000)
    monkeypatch.setattr(notifications, "INITIAL_REQUEST_TIMEOUT", 0.01, raising=False)
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    signal.expires_ms = 200_000
    store = Store(settings.data_dir)
    store.signal(signal)
    calls = []

    async def transport(request):
        calls.append(request)
        await asyncio.sleep(1)
        return httpx.Response(200, json={"id": "late"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    assert await notifier.send_research(signal) == "uncertain"
    assert await notifier.send_research(signal) == "duplicate-plan-suppressed"
    assert len(calls) == 1
    assert store.get("discord_transport")["category"] == "TIMEOUT"
    assert not notifier.was_initially_delivered(signal.id, "research")
    store.close()
