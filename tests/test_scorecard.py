import json

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.notifications import Notifier
from bybit_flow.scorecard import outcome, render, scorecard, verdict
from bybit_flow.storage import Store


def add(store, ident, tier, state, reason, sent=True):
    payload = dict(
        id=ident,
        direction="LONG",
        entry=100,
        stop=99,
        tp1=103,
        raw_tier=tier,
        family="liquidity_sweep",
        horizon_profile="CORE_INTRADAY",
    )
    with store.db:
        store.db.execute(
            "INSERT INTO signals VALUES(?,?,?,?,?)", (ident, "TESTUSDT", 1000, state, json.dumps(payload))
        )
        store.db.execute(
            "INSERT INTO transitions(signal_id,at_ms,state,reason) VALUES(?,?,?,?)",
            (ident, 2000, state, reason),
        )
        if sent:
            store.db.execute(
                "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
                (f"research:{ident}:initial", ident, "sent", "{}", "1", 1500),
            )


def test_outcome_classification():
    assert outcome("RESOLVED", "PLANNED_TARGET_CROSSED") == "target"
    assert outcome("INVALIDATED", "PLANNED_STOP_CROSSED") == "stop"
    assert outcome("INVALIDATED", "Stop observed during downtime") == "stop"
    assert outcome("INVALIDATED", "Required live feed lost") == "invalidated"
    assert outcome("EXPIRED", "Tracking period ended") == "expired"
    assert outcome("ALERTED", "") == "open"


def test_scorecard_counts_only_delivered_alerts_by_grade(settings):
    store = Store(settings.data_dir)
    add(store, "a", "B", "RESOLVED", "PLANNED_TARGET_CROSSED")
    add(store, "b", "B", "INVALIDATED", "PLANNED_STOP_CROSSED")
    add(store, "c", "B", "EXPIRED", "Tracking period ended")
    add(store, "d", "D", "INVALIDATED", "PLANNED_STOP_CROSSED")
    add(store, "never-sent", "A", "RESOLVED", "PLANNED_TARGET_CROSSED", sent=False)
    report = scorecard(store)
    assert report["all"]["alerts"] == 4
    b = report["by_tier"]["B"]
    assert (b["target"], b["stop"], b["expired"]) == (1, 1, 1)
    assert b["target_rate_of_resolved"] == 0.5
    assert b["average_planned_rr"] == 3.0 and b["breakeven_target_rate"] == 0.25
    assert b["gross_r_before_costs"] == 2.0
    assert b["verdict"] == "INSUFFICIENT SAMPLE"
    assert "A" not in report["by_tier"] and list(report["by_tier"]) == ["B", "D"]
    text = render(report)
    assert "ALL DELIVERED" in text and "BY TIER" in text


def test_verdict_needs_a_sample_and_respects_the_interval():
    assert verdict([0.05, 0.15], 0.25, 149) == "BELOW BREAKEVEN"
    assert verdict([0.30, 0.45], 0.25, 149) == "ABOVE BREAKEVEN"
    assert verdict([0.20, 0.30], 0.25, 149) == "INCONCLUSIVE"
    assert verdict([0.0, 0.2], 0.25, 12) == "INSUFFICIENT SAMPLE"


@pytest.mark.parametrize(
    "minimum,grade,expected",
    [
        ("C", "D", "blocked:BELOW_MIN_ALERT_TIER"),
        ("C", "F", "blocked:BELOW_MIN_ALERT_TIER"),
        ("C", "C", "sent"),
        ("C", "A", "sent"),
        ("A", "B", "blocked:BELOW_MIN_ALERT_TIER"),
        ("F", "F", "sent"),
    ],
)
async def test_minimum_grade_decides_initial_delivery(
    settings, signal, notification_clock, minimum, grade, expected
):
    cfg = settings.model_copy(
        update={
            "research_alerts": True,
            "min_alert_tier": minimum,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/mock/test"),
        }
    )
    signal.raw_tier, signal.final_tier, signal.state = grade, "RESEARCH", "CONFIRMED"
    store = Store(settings.data_dir)
    store.signal(signal)
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json={"id": "mock-id"})

    notifier = Notifier(cfg, store, httpx.MockTransport(handle))
    assert await notifier.send_research(signal) == expected
    assert bool(calls) == (expected == "sent")


def test_default_minimum_grade_is_c():
    from bybit_flow.config import Settings

    assert Settings(_env_file=None).min_alert_tier == "C"


def test_scorecard_groups_by_recorded_hypothesis_tags(settings):
    store = Store(settings.data_dir)
    add(store, "a", "B", "RESOLVED", "PLANNED_TARGET_CROSSED")
    add(store, "b", "B", "INVALIDATED", "PLANNED_STOP_CROSSED")
    add(store, "c", "B", "EXPIRED", "Tracking period ended")
    for ident, tags in (("a", {"absorption_with_trade": True}), ("b", {"absorption_with_trade": False})):
        payload = json.loads(
            store.db.execute("SELECT payload FROM signals WHERE id=?", (ident,)).fetchone()[0]
        )
        payload["evidence"] = {"forward_hypotheses": tags}
        with store.db:
            store.db.execute("UPDATE signals SET payload=? WHERE id=?", (json.dumps(payload), ident))
    report = scorecard(store)
    assert report["by_hypothesis"]["absorption_with_trade"]["target"] == 1
    assert report["by_hypothesis"]["not absorption_with_trade"]["stop"] == 1
    assert sum(row["alerts"] for row in report["by_hypothesis"].values()) == 2  # Untagged alert is absent.
    assert "BY HYPOTHESIS" in render(report)
