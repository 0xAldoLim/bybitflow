"""Opposite horizon labels and scores cannot substitute for causal justification."""

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.notification_conflicts import assess
from bybit_flow.notifications import Notifier
from bybit_flow.storage import Store

NOW = 1_000_000


def plans(signal):
    old = signal.model_copy(deep=True)
    old.id, old.state, old.direction = "original-short", "ALERTED", "SHORT"
    old.horizon_profile, old.setup_timeframe = "SWING", "240"
    old.entry, old.zone, old.stop, old.tp1, old.tp2 = 100, (99, 101), 110, 70, 60
    old.expires_ms, old.holding_deadline_ms = NOW + 60000, NOW + 86_400_000
    old.coverage = dict(
        monitoring="active",
        monitor_status="MONITOR_READY",
        last_monitor_ms=NOW,
        v22_current=dict(state="STABLE", available_ms=NOW),
    )
    old.evidence = dict(
        frozen_structure=dict(
            available_ms=NOW - 60000, structure_event_id="older-swing", frozen_reference=100
        )
    )
    new = old.model_copy(deep=True)
    new.id, new.state, new.direction = "counter-long", "CONFIRMED", "LONG"
    new.horizon_profile, new.setup_timeframe = "CORE_INTRADAY", "60"
    new.entry, new.zone, new.stop, new.tp1, new.tp2 = 85, (84, 86), 80, 95, 100
    new.expires_ms, new.holding_deadline_ms = NOW + 120000, NOW + 14_400_000
    new.evidence = dict(
        production_policies=dict(trade_quality="trade-validity-v22"),
        frozen_structure=dict(
            available_ms=NOW - 60000, structure_event_id="new-intraday", frozen_reference=82
        ),
        delivery_confirmation=dict(
            available_ms=NOW,
            passed=True,
            structure_support=True,
            participation_support=True,
            metrics=dict(flow=dict(strong=True)),
        ),
        flow_quality=dict(available_ms=NOW, flow_trust_score=1),
        impact_efficiency=dict(
            available_ms=NOW, coverage_complete=True, contradicted=False, state="EFFICIENT_DIRECTIONAL_IMPACT"
        ),
        entry_quality=dict(available_ms=NOW, acceptable=True, quote_state="QUOTE_FRESH"),
    )
    return old, new


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_nested_counter_move_has_specific_evidenced_reason(settings, signal, direction):
    old, new = plans(signal)
    if direction == "SHORT":
        for s in (old, new):
            s.direction = "LONG" if s.direction == "SHORT" else "SHORT"
            s.entry, s.stop, s.tp1, s.tp2 = (200 - getattr(s, k) for k in ("entry", "stop", "tp1", "tp2"))
            s.zone = (200 - s.zone[1], 200 - s.zone[0])
    before = old.model_dump_json(), new.model_dump_json()
    result = assess(new.model_dump(), old.model_dump(), NOW, settings)
    assert result["allowed"] and not result["reasons"]
    assert "Distinct frozen level" in result["explanation"]
    assert "complete counter-plan stays between" in result["explanation"]
    assert old.id in result["explanation"]
    assert before == (old.model_dump_json(), new.model_dump_json())


@pytest.mark.parametrize(
    "defect",
    [
        "same-horizon",
        "unknown-horizon",
        "longer-deadline",
        "paused",
        "stale-monitor",
        "deteriorating",
        "old-structure-missing",
        "same-structure",
        "future-structure",
        "crosses-stop",
        "crosses-target",
        "stale-quote",
        "unconfirmed",
        "weak-flow",
        "untrusted",
        "no-response",
        "incomplete-flow",
        "source-mismatch",
        "venue-mismatch",
        "null-evidence",
    ],
)
def test_every_missing_justification_blocks_even_score_100(settings, signal, defect):
    old, new = plans(signal)
    new.quality = 100
    if defect == "same-horizon":
        new.horizon_profile = old.horizon_profile
    elif defect == "unknown-horizon":
        new.horizon_profile = "LEGACY"
    elif defect == "longer-deadline":
        new.holding_deadline_ms = old.holding_deadline_ms + 1
    elif defect == "paused":
        old.coverage["monitoring"] = "paused"
    elif defect == "stale-monitor":
        old.coverage["last_monitor_ms"] = NOW - settings.trade_stale_ms - 1
    elif defect == "deteriorating":
        old.coverage["v22_current"]["state"] = "DETERIORATING"
    elif defect == "old-structure-missing":
        old.evidence.clear()
    elif defect == "same-structure":
        new.evidence["frozen_structure"]["structure_event_id"] = "older-swing"
    elif defect == "future-structure":
        new.evidence["frozen_structure"]["available_ms"] = NOW + 1
    elif defect == "crosses-stop":
        new.tp2 = old.stop
    elif defect == "crosses-target":
        new.stop = old.tp1
    elif defect == "stale-quote":
        new.evidence["entry_quality"]["available_ms"] = NOW - settings.book_stale_ms - 1
    elif defect == "unconfirmed":
        new.evidence["delivery_confirmation"]["passed"] = False
    elif defect == "weak-flow":
        new.evidence["delivery_confirmation"]["metrics"]["flow"]["strong"] = False
    elif defect == "untrusted":
        new.evidence["flow_quality"]["flow_trust_score"] = 0.59
    elif defect == "no-response":
        new.evidence["impact_efficiency"]["state"] = "NEUTRAL_RESPONSE"
    elif defect == "incomplete-flow":
        new.evidence["impact_efficiency"]["coverage_complete"] = False
    elif defect == "source-mismatch":
        new.evidence["impact_efficiency"]["source"] = "wrong-venue"
    elif defect == "null-evidence":
        new.evidence["entry_quality"] = None
    elif defect == "venue-mismatch":
        new.source = "binance" if old.source == "bybit" else "bybit"
    assert not assess(new.model_dump(), old.model_dump(), NOW, settings)["allowed"]


async def test_only_justified_opposition_is_sent_with_reason_and_no_lifecycle_mutation(
    settings, signal, monkeypatch
):
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: NOW)
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    old, new = plans(signal)
    store = Store(settings.data_dir)
    store.signal(old)
    store.signal(new)
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            (f"research:{old.id}:initial", old.id, "sent", "{}", "original", NOW),
        )
    store.put(f"discord_initial_webhook_id:{old.id}", "123")
    original = store.db.execute("SELECT payload FROM signals WHERE id=?", (old.id,)).fetchone()[0]
    cards = []

    def transport(request):
        import json

        cards.append(json.loads(request.content)["embeds"][0])
        return httpx.Response(200, json={"id": "counter"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))

    async def prepare(s):
        return None

    notifier.prepare_initial = prepare
    notifier.finalize_initial = lambda s: None
    assert await notifier.send_research(new) == "sent"
    reasons = [f["value"] for f in cards[0]["fields"] if f["name"] == "Why an opposite setup is allowed"]
    assert len(reasons) == 1 and old.id in reasons[0]
    assert "fresh strong structure/flow confirmation" in reasons[0]
    assert store.db.execute("SELECT payload FROM signals WHERE id=?", (old.id,)).fetchone()[0] == original
    assert len(store.active_signals()) == 2
    store.close()


async def test_pause_during_dispatch_revokes_exception(settings, signal, monkeypatch):
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: NOW)
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")
    old, new = plans(signal)
    store = Store(settings.data_dir)
    store.signal(old)
    store.signal(new)
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            (f"research:{old.id}:initial", old.id, "sent", "{}", "original", NOW),
        )
    original_enter = httpx.AsyncClient.__aenter__

    async def enter(client):
        old.coverage["monitoring"] = "paused"
        store.monitor(old)
        return await original_enter(client)

    monkeypatch.setattr(httpx.AsyncClient, "__aenter__", enter)

    def forbidden(request):
        pytest.fail("An exception cannot survive lost monitoring proof")

    notifier = Notifier(settings, store, httpx.MockTransport(forbidden))

    async def prepare(s):
        return None

    notifier.prepare_initial = prepare
    notifier.finalize_initial = lambda s: None
    assert await notifier.send_research(new) == "blocked:opposing-visible-active-setup"
    assert "discord_opposition_exception" not in new.evidence
    store.close()
