import json

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow import daily_status
from bybit_flow.daily_status import DAY, due, embed, mention, note_block, summary
from bybit_flow.notifications import Notifier
from bybit_flow.storage import Store

HOOK = "https://discord.com/api/webhooks/mock/test"
MONITOR = "https://discord.com/api/webhooks/mock/monitor"


def configured(settings, **update):
    return settings.model_copy(
        update={
            "research_alerts": True,
            "daily_status": True,
            "research_webhook": SecretStr(HOOK),
            "monitoring_webhook": SecretStr(MONITOR),
        }
        | update
    )


def transport(sent):
    def handle(request):
        sent.append((str(request.url), json.loads(request.content)))
        return httpx.Response(200, json={"id": "mock-id"})

    return httpx.MockTransport(handle)


@pytest.mark.parametrize(
    "setting,content,allowed",
    [
        ("", None, {"parse": []}),
        ("everyone", "@everyone", {"parse": ["everyone"]}),
        ("here", "@here", {"parse": ["everyone"]}),
        ("user:123456", "<@123456>", {"users": ["123456"]}),
        ("role:987", "<@&987>", {"roles": ["987"]}),
        ("user:not-a-number", None, {"parse": []}),
        ("@someone", None, {"parse": []}),
    ],
)
def test_mention_setting(setting, content, allowed):
    assert mention(setting) == (content, allowed)


async def test_new_setup_pings_and_follow_up_does_not(settings, signal, notification_clock):
    cfg = configured(settings, alert_mention="everyone")
    signal.state = "CONFIRMED"
    store = Store(settings.data_dir)
    store.signal(signal)
    sent = []
    notifier = Notifier(cfg, store, transport(sent))
    assert await notifier.send_research(signal) == "sent"
    assert sent[0][1]["content"] == "@everyone"
    assert sent[0][1]["allowed_mentions"] == {"parse": ["everyone"]}
    quiet = Notifier(configured(settings), store, transport(sent))
    other = signal.model_copy(update={"id": "second", "symbol": "OTHERUSDT"})
    store.signal(other)
    assert await quiet.send_research(other) == "sent"
    assert "content" not in sent[-1][1] and sent[-1][1]["allowed_mentions"] == {"parse": []}


async def test_held_back_setups_are_counted_once_with_their_reason(settings, signal, notification_clock):
    cfg = configured(settings, min_alert_tier="C", alert_blocked_rules=["SWING|trend_pullback"])
    store = Store(settings.data_dir)
    notifier = Notifier(cfg, store, transport([]))
    low = signal.model_copy(update={"id": "low", "raw_tier": "D", "state": "CONFIRMED"})
    wrong = signal.model_copy(
        update={"id": "wrong", "raw_tier": "A", "horizon_profile": "SWING", "state": "CONFIRMED"}
    )
    for s in (low, wrong):
        store.signal(s)
        for _ in range(3):  # The scanner retries every loop; the count must not grow.
            assert (await notifier.send_research(s)).startswith("blocked:")
    blocks = store.get(daily_status.BLOCKS)
    assert {k: v[0] for k, v in blocks.items()} == {"low": "BELOW_MIN_ALERT_TIER", "wrong": "RULE_BLOCKED"}
    report = summary(store, cfg, max(v[1] for v in blocks.values()) + 1000)
    assert report["held_back"] == {"BELOW_MIN_ALERT_TIER": 1, "RULE_BLOCKED": 1}


def test_block_history_is_bounded(settings, signal):
    store = Store(settings.data_dir)
    for i in range(520):
        note_block(store, signal.model_copy(update={"id": f"s{i}"}), "BELOW_MIN_ALERT_TIER", at_ms=i)
    blocks = store.get(daily_status.BLOCKS)
    assert len(blocks) == 500 and "s0" not in blocks and "s519" in blocks


def test_summary_and_embed_state_what_happened(settings, signal):
    cfg = configured(settings)
    store = Store(settings.data_dir)
    now = 10 * DAY
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            ("research:a:initial", "a", "sent", "{}", "1", now - 3_600_000),
        )
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            ("research:old:initial", "old", "sent", "{}", "2", now - 3 * DAY),
        )
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            ("research:u:initial", "u", "uncertain", "{}", None, now - 1000),
        )
        store.db.execute(
            "INSERT INTO transitions(signal_id,at_ms,state,reason) VALUES('a',?,'CONFIRMED','evaluation')",
            (now - 4_000_000,),
        )
    store.put("scanner", dict(state="collecting", at_ms=now - 120_000))
    store.put("recorder_health", dict(state="NORMAL"))
    store.put("storage_status", dict(usage_percent=61.4))
    report = summary(store, cfg, now)
    assert report["alerts_sent"] == 1 and report["confirmed"] == 1 and report["uncertain"] == 1
    assert report["last_alert_ms"] == now - 3_600_000
    card = embed(report)["embeds"][0]
    assert card["title"] == "DAILY STATUS · running"
    assert "**1 setup alerts sent** in the last 24h (last one 1.0h ago)" in card["description"]
    assert "storage 61%" in card["description"] and "not confirmed by Discord" in card["description"]
    assert "stops arriving" in card["footer"]["text"]
    store.put("scanner", dict(state="blocked_by_storage", at_ms=now))
    assert embed(summary(store, cfg, now))["embeds"][0]["title"] == "DAILY STATUS · NEEDS ATTENTION"


async def test_status_is_sent_once_per_day_to_the_monitoring_channel(settings):
    cfg = configured(settings, daily_status_hour_utc=0)
    store = Store(settings.data_dir)
    sent = []
    notifier = Notifier(cfg, store, transport(sent))
    now = 20 * DAY + 3_600_000
    assert await daily_status.send(notifier, now) == "sent"
    assert await daily_status.send(notifier, now + 6 * 3_600_000) == "already-attempted"
    assert await daily_status.send(notifier, now + DAY) == "sent"
    assert len(sent) == 2 and all(url.startswith(MONITOR) for url, _ in sent)
    assert sent[0][1]["allowed_mentions"] == {"parse": []}
    assert due(now, 0) + 1 == due(now + DAY, 0)


async def test_status_falls_back_to_the_signal_channel_and_can_be_disabled(settings):
    store = Store(settings.data_dir)
    sent = []
    only_signal = configured(settings, monitoring_webhook=SecretStr(""))
    assert await daily_status.send(Notifier(only_signal, store, transport(sent)), 30 * DAY) == "sent"
    assert sent[0][0].startswith(HOOK)
    off = configured(settings, daily_status=False)
    assert await daily_status.send(Notifier(off, store, transport(sent)), 40 * DAY) == "disabled"
    none = configured(settings, monitoring_webhook=SecretStr(""), research_webhook=SecretStr(""))
    assert await daily_status.send(Notifier(none, store, transport(sent)), 50 * DAY) == "dry-run"


def test_defaults():
    from bybit_flow.config import Settings

    cfg = Settings(_env_file=None)
    assert cfg.daily_status is True and cfg.alert_mention == "" and cfg.daily_status_hour_utc == 0
