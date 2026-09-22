import json

import httpx
from pydantic import SecretStr

from bybit_flow.notifications import Notifier
from bybit_flow.storage import Store


async def test_invisible_setup_has_no_pause_and_visible_outage_has_one(settings, signal):
    store = Store(settings.data_dir)
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    calls = []

    def transport(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "pause-card"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    signal.coverage.update(monitoring="paused", monitoring_event="paused", pause_since_ms=1000)
    store.signal(signal)
    for status in (None, "duplicate-suppressed", "dry-run", "disabled", "rejected", "uncertain", "sending"):
        if status:
            with store.db:
                store.db.execute(
                    "INSERT OR REPLACE INTO outbox VALUES(?,?,?,?,?,?)",
                    ("research:" + signal.id + ":initial", signal.id, status, "{}", None, 1),
                )
        assert await notifier.send_research(signal, update=True) == "blocked:no-visible-initial"
    assert store.active_signals()[0]["coverage"]["monitoring"] == "paused"
    assert not calls
    with store.db:
        store.db.execute("UPDATE outbox SET status=?,message_id=?", ("sent", "initial-card"))
    for _ in range(3):
        await notifier.send_research(signal, update=True)
    assert len(calls) == 1 and calls[0]["embeds"][0]["title"].startswith("MONITORING PAUSED")
    assert not notifier.was_initially_delivered(signal.id, "validated")
    visible = store.get("discord_visibility:research:" + signal.id)
    assert visible["user_visible_initial"] and visible["initial_message_id"] == "initial-card"
    child = signal.model_copy(deep=True)
    child.id = "mentioned-only-child"
    assert await notifier.send_research(child, update=True) == "blocked:no-visible-initial"
    signal.coverage["monitoring_event"] = "resumed"
    for _ in range(2):
        await notifier.send_research(signal, update=True)
    assert len(calls) == 2
    store.close()
