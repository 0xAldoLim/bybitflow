"""Retired chart setups remain readable but cannot become orphan live alerts."""

import json

import pytest

from bybit_flow.lifecycle import retire_tradingview
from bybit_flow.notifications import Notifier, embed
from bybit_flow.storage import Store, now_ms


def historical(store, signal, state):
    signal.source = "tradingview"
    signal.state = state
    with store.db:
        store.db.execute(
            "INSERT INTO signals VALUES(?,?,?,?,?)",
            (signal.id, signal.symbol, signal.created_ms, state, signal.model_dump_json()),
        )


@pytest.mark.asyncio
async def test_unseen_tradingview_retirement_is_idempotent_and_silent(settings, signal):
    store = Store(settings.data_dir)
    historical(store, signal, "ALERTED")
    original = signal.model_dump()
    assert retire_tradingview(store) == 1
    assert retire_tradingview(store) == 0
    saved = json.loads(store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0])
    for field in ("entry", "stop", "tp1", "tp2", "quality", "created_ms", "source", "version"):
        assert saved[field] == original[field]
    assert saved["state"] == "INVALIDATED"
    assert saved["coverage"]["terminal_reason"] == "TRADINGVIEW_SUBSYSTEM_RETIRED"
    assert (
        store.db.execute(
            "SELECT count(*) FROM transitions WHERE signal_id=? AND reason='TRADINGVIEW_SUBSYSTEM_RETIRED'",
            (signal.id,),
        ).fetchone()[0]
        == 1
    )
    assert (
        store.db.execute(
            "SELECT notification_status FROM terminal_events WHERE signal_id=?", (signal.id,)
        ).fetchone()[0]
        == "unseen"
    )
    assert (
        await Notifier(settings, store).send_research(
            signal.model_copy(update={"state": "INVALIDATED"}), update=True
        )
        == "blocked:no-visible-initial"
    )
    store.close()


def test_visible_retirement_has_one_compact_pending_update(settings, signal):
    store = Store(settings.data_dir)
    historical(store, signal, "ALERTED")
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            (f"research:{signal.id}:initial", signal.id, "sent", "{}", "123456", now_ms()),
        )
    assert retire_tradingview(store) == 1
    assert retire_tradingview(store) == 0
    assert (
        store.db.execute(
            "SELECT notification_status FROM terminal_events WHERE signal_id=?", (signal.id,)
        ).fetchone()[0]
        == "pending"
    )
    saved = json.loads(store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0])
    card = embed(signal.model_validate(saved), "http://127.0.0.1:8000")
    assert card["embeds"][0]["fields"][0]["value"] == "TradingView subsystem retired"
    store.close()


def test_terminal_tradingview_history_stays_readable_and_new_creation_fails(settings, signal):
    store = Store(settings.data_dir)
    historical(store, signal, "RESOLVED")
    assert retire_tradingview(store) == 0
    assert store.db.execute("SELECT state FROM signals WHERE id=?", (signal.id,)).fetchone()[0] == "RESOLVED"
    new = signal.model_copy(update={"id": "new-chart-setup", "state": "ALERTED"})
    with pytest.raises(ValueError, match="TRADINGVIEW_SUBSYSTEM_RETIRED"):
        store.signal(new)
    store.close()
