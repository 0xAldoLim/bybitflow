import json

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.notifications import Notifier
from bybit_flow.scoring import tier
from bybit_flow.storage import Store


@pytest.mark.parametrize(
    "points,grade",
    [
        (0, "D"),
        (49.9, "D"),
        (50, "C"),
        (64.9, "C"),
        (65, "B"),
        (74.9, "B"),
        (75, "A"),
        (84.9, "A"),
        (85, "S"),
        (89.9, "S"),
        (90, "SS"),
        (94.9, "SS"),
        (95, "SSS"),
        (100, "SSS"),
    ],
)
async def test_all_research_grades_send_without_claiming_validation(settings, signal, points, grade):
    assert tier(points) == grade
    cfg = settings.model_copy(
        update={
            "research_alerts": True,
            "sss_research": True,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/mock/test"),
        }
    )
    signal.quality, signal.raw_tier, signal.final_tier = points, grade, "RESEARCH"
    signal.state = "CONFIRMED"
    store = Store(settings.data_dir)
    store.signal(signal)
    payloads = []

    def handle(request):
        payloads.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "mock-id"})

    notifier = Notifier(cfg, store, httpx.MockTransport(handle))
    assert await notifier.send_research(signal) == "sent"
    card = payloads[0]["embeds"][0]
    assert card["title"].startswith(f"{grade} RESEARCH · UNCALIBRATED")
    fields = {f["name"]: f["value"] for f in card["fields"]}
    assert "Stop" in fields["Entry zone / invalidation"]
    assert "TP1" in fields["Targets"] and "TP2" in fields["Targets"]
    assert (await notifier.send_public(signal)).startswith("blocked")
    store.close()


async def test_validated_lower_grade_still_uses_requested_research_channel(settings, signal):
    cfg = settings.model_copy(
        update={
            "research_alerts": True,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/mock/test"),
        }
    )
    signal.quality, signal.raw_tier, signal.final_tier = 80, "A", "A"
    signal.validation_status = "validated"
    signal.state = "CONFIRMED"
    store = Store(settings.data_dir)
    store.signal(signal)
    notifier = Notifier(
        cfg, store, httpx.MockTransport(lambda request: httpx.Response(200, json={"id": "mock-id"}))
    )
    assert await notifier.send_research(signal) == "sent"
    assert (await notifier.send_public(signal)).startswith("blocked")
    store.close()
