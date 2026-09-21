from datetime import datetime

import pytest

from bybit_flow.macro import parse, state
from bybit_flow.models import Candle
from bybit_flow.reconciliation import advance
from bybit_flow.storage import Store


def bar(start, low=98, high=102):
    return Candle(start, 60000, 100, high, low, 100, 100, 10000)


@pytest.mark.parametrize(
    "low,high,expected",
    [(94, 102, "INVALIDATED"), (98, 116, "RESOLVED"), (94, 116, "INVALIDATED"), (98, 102, None)],
)
def test_downtime_frozen_plan_stop_target_and_ambiguity(signal, low, high, expected):
    signal.horizon_profile = "CORE_INTRADAY"
    signal.evidence["observed_entry_ms"] = 1
    original = (signal.entry, signal.zone, signal.stop, signal.tp1, signal.tp2, signal.quality)
    result = advance(signal, [bar(60000, low, high)], 60000, 120000, 180000)
    assert result.get("state") == expected
    assert result["coverage_complete"]
    assert original == (signal.entry, signal.zone, signal.stop, signal.tp1, signal.tp2, signal.quality)
    if low == 94 and high == 116:
        assert result["ambiguous"]


def test_missing_downtime_bar_never_resumes(signal):
    result = advance(signal, [bar(120000)], 60000, 180000, 180000)
    assert not result["coverage_complete"]
    assert result["cursor_ms"] == 60000


def test_downtime_deadline(signal):
    signal.holding_deadline_ms = 120000
    result = advance(signal, [bar(60000)], 60000, 180000, 180000)
    assert result["state"] == "EXPIRED"
    assert result["effective_ms"] == 120000


@pytest.mark.parametrize(
    "currency,impact,time,expected",
    [
        ("USD", "High", "2026-03-09T08:30:00-04:00", 1),
        ("USD", "High", "2026-03-06T08:30:00-05:00", 1),
        ("USD", "Medium", "2026-03-09T08:30:00-04:00", 0),
        ("USD", "Low", "2026-03-09T08:30:00-04:00", 0),
        ("EUR", "High", "2026-03-09T08:30:00-04:00", 0),
        ("USD", "High", "2026-03-09T18:30:00-04:00", 0),
    ],
)
def test_calendar_currency_impact_session_and_dst(currency, impact, time, expected):
    events = parse([dict(title="Fixture event", country=currency, impact=impact, date=time)])
    assert len(events) == expected
    if events:
        assert events[0]["scheduled_ms"] == int(datetime.fromisoformat(time).timestamp() * 1000)


def test_calendar_merge_and_stale_cache_fail_open(settings):
    store = Store(settings.data_dir)
    events = parse(
        [
            dict(title="Event " + str(m), country="USD", impact="High", date=f"2026-03-09T08:{m}:00-04:00")
            for m in (30, 45)
        ]
    )
    now = events[0]["scheduled_ms"]
    store.put("macro_calendar", dict(fetched_ms=now, events=events))
    current = state(store, settings, now)
    assert current["macro_pause_active"]
    assert current["pause_until_ms"] == events[-1]["scheduled_ms"] + 1800000
    assert len(current["events"]) == 2
    assert not state(store, settings, now + 7 * 3600000)["macro_pause_active"]
    assert state(store, settings, now + 7 * 3600000)["status"] == "DEGRADED"
    store.close()


async def test_restart_terminal_catchup_notifies_once_and_never_resumes(settings, signal):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from bybit_flow.reconciliation import reconcile
    from bybit_flow.scanner import Scanner
    from bybit_flow.storage import Recorder

    store = Store(settings.data_dir)
    signal.state, signal.horizon_profile = "ALERTED", "CORE_INTRADAY"
    signal.coverage = {"checked_ms": 60000, "pause_notice_sent": True}
    signal.evidence["observed_entry_ms"] = 1000
    store.signal(signal)
    scanner = Scanner(settings, store, Recorder(store, settings))
    await scanner.api.close()
    scanner.api = SimpleNamespace(name="bybit", candles=AsyncMock(return_value=[bar(60000, 94, 102)]))
    scanner.notifier = SimpleNamespace(send_research=AsyncMock(return_value="sent"))
    assert not await reconcile(scanner, signal, 180000)
    assert signal.state == "INVALIDATED"
    assert signal.coverage["monitoring_event"] is None
    assert scanner.notifier.send_research.await_count == 1
    await scanner.evaluate()
    assert scanner.notifier.send_research.await_count == 1
    assert store.db.execute("SELECT count(*) FROM transitions WHERE state='INVALIDATED'").fetchone()[0] == 1
    store.close()
