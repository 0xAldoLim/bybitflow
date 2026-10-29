"""Published plans produce visible results and cannot send same-horizon opposition."""

import json

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.notifications import Notifier, embed
from bybit_flow.storage import Store


def configured(settings):
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/test")


def initial(store, signal, status="sent", message="original"):
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            (f"research:{signal.id}:initial", signal.id, status, "{}", message, 1000),
        )
    store.put(f"discord_initial_webhook_id:{signal.id}", "123")


def resolve(store, signal, outcome="TARGET", method="LIVE_EXECUTED_TRADE", ambiguous=False):
    signal.state = "RESOLVED" if outcome == "TARGET" else "INVALIDATED"
    signal.evidence.update(
        primary_outcome="UNCLEAR" if ambiguous else outcome,
        observed_entry_ms=2000,
        terminal_event=dict(effective_ms=3000, method=method, ambiguous=ambiguous),
    )
    store.signal(signal)


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
@pytest.mark.parametrize("outcome,title", [("TARGET", "TP1 REACHED"), ("STOP", "STOP REACHED")])
def test_explicit_price_result(direction, outcome, title, signal, settings):
    store = Store(settings.data_dir)
    signal.direction = direction
    if direction == "SHORT":
        signal.stop, signal.tp1, signal.tp2 = 105, 85, 80
    resolve(store, signal, outcome)
    card = embed(signal, "http://localhost")["embeds"][0]
    assert card["title"].startswith(title)
    text = str(card["fields"])
    assert "Observed <t:3:F>" in text
    assert "account fills and realized profit are not verified" in text
    assert ("+15.00%" if outcome == "TARGET" else "-5.00%") in text
    store.close()


def test_ambiguous_recovery_does_not_claim_win(settings, signal):
    store = Store(settings.data_dir)
    resolve(store, signal, method="CLOSED_1M_OHLC", ambiguous=True)
    card = embed(signal, "http://localhost")["embeds"][0]
    assert not card["title"].startswith("TP1 REACHED")
    assert "Planned price result" not in str(card)
    store.close()


@pytest.mark.parametrize("outcome", ["TARGET", "STOP"])
async def test_terminal_edits_and_notifies_once_across_restart(settings, signal, outcome):
    configured(settings)
    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    store.signal(signal)
    initial(store, signal)
    receipt = store.db.execute("SELECT * FROM outbox").fetchone()[:]
    resolve(store, signal, outcome)
    frozen = store.db.execute("SELECT payload FROM signals").fetchone()[0]
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(200, json={"id": "result"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    await notifier.retry_terminals()
    await notifier.retry_terminals()
    assert [r.method for r in calls] == ["PATCH", "POST"]
    assert json.loads(calls[1].content)["embeds"][0]["title"].startswith(
        "TP1 REACHED" if outcome == "TARGET" else "STOP REACHED"
    )
    # Simulate crash after result POST, before terminal status commit.
    with store.db:
        store.db.execute("UPDATE terminal_events SET notification_status='pending',next_ms=0")
    store.close()
    store = Store(settings.data_dir)
    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    await notifier.retry_terminals()
    assert [r.method for r in calls] == ["PATCH", "POST", "PATCH"]
    assert store.db.execute("SELECT * FROM outbox WHERE key LIKE '%:initial'").fetchone()[:] == receipt
    assert store.db.execute("SELECT payload FROM signals").fetchone()[0] == frozen
    store.close()


async def test_uncertain_price_result_is_never_resent(settings, signal):
    configured(settings)
    store = Store(settings.data_dir)
    initial(store, signal)
    resolve(store, signal)
    calls = []

    def transport(request):
        calls.append(request.method)
        if request.method == "POST":
            raise httpx.ReadTimeout("uncertain", request=request)
        return httpx.Response(200, json={"id": "original"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    await notifier.retry_terminals()
    assert (
        store.db.execute("SELECT status FROM outbox WHERE key LIKE '%:price-result'").fetchone()[0]
        == "uncertain"
    )
    with store.db:
        store.db.execute("UPDATE terminal_events SET notification_status='pending',next_ms=0")
    await notifier.retry_terminals()
    assert calls == ["PATCH", "POST", "PATCH"]
    store.close()


@pytest.mark.parametrize(
    "status,message", [("uncertain", "123"), ("dry-run", None), ("rejected", None), ("sent", None)]
)
async def test_unseen_initial_has_no_result_posts(settings, signal, status, message):
    configured(settings)
    store = Store(settings.data_dir)
    initial(store, signal, status, message)
    resolve(store, signal)

    def forbidden(request):
        pytest.fail("An unseen initial must not produce any lifecycle card")

    notifier = Notifier(settings, store, httpx.MockTransport(forbidden))
    assert await notifier.send_price_result(signal) == "blocked:no-visible-initial"
    await notifier.retry_terminals()
    store.close()


@pytest.mark.parametrize("other_source", ["bybit", "binance"])
async def test_visible_opposite_same_horizon_blocked_without_lifecycle_changes(
    settings, signal, notification_clock, other_source
):
    configured(settings)
    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    signal.horizon_profile = "CORE_INTRADAY"
    store.signal(signal)
    initial(store, signal)
    opposite = signal.model_copy(deep=True)
    opposite.id, opposite.direction, opposite.state = "opposite", "SHORT", "CONFIRMED"
    opposite.source = other_source
    opposite.stop, opposite.tp1, opposite.tp2 = 105, 85, 80
    store.signal(opposite)
    before = dict(store.db.execute("SELECT id,payload FROM signals"))
    calls = []

    def transport(request):
        calls.append(request)
        return httpx.Response(200, json={"id": "later"})

    notifier = Notifier(settings, store, httpx.MockTransport(transport))
    assert await notifier.send_research(opposite) == "blocked:opposing-visible-active-setup"
    assert not calls
    assert before == dict(store.db.execute("SELECT id,payload FROM signals"))
    assert len(store.active_signals()) == 2
    opposite.coverage["monitoring_event"] = "paused"
    assert await notifier.send_research(opposite, update=True) == "blocked:no-visible-initial"
    assert not calls
    # Closing the original releases the alert guard, without reviving an expired entry.
    resolve(store, signal)
    assert await notifier.send_research(opposite) == "sent"
    assert len(calls) == 1
    store.close()


@pytest.mark.parametrize(
    "condition", ["never-sent", "retired-destination", "same-direction", "deadline-passed"]
)
def test_conflict_guard_is_narrow(settings, signal, notification_clock, condition):
    configured(settings)
    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    signal.horizon_profile = "CORE_INTRADAY"
    store.signal(signal)
    if condition != "never-sent":
        initial(store, signal)
    new = signal.model_copy(deep=True)
    new.id, new.direction = "new", "SHORT"
    if condition == "retired-destination":
        store.put(f"discord_initial_webhook_id:{signal.id}", "old")
    elif condition == "same-direction":
        new.direction = "LONG"
    elif condition == "deadline-passed":
        signal.holding_deadline_ms = 999
        store.signal(signal)
    assert Notifier(settings, store).initial_delivery_block(new.id, new) is None
    store.close()


async def test_dispatch_rechecks_newly_published_opposition(
    settings, signal, notification_clock, monkeypatch
):
    configured(settings)
    store = Store(settings.data_dir)
    signal.state, signal.horizon_profile = "CONFIRMED", "CORE_INTRADAY"
    store.signal(signal)
    other = signal.model_copy(deep=True)
    other.id, other.direction, other.state = "opposition", "SHORT", "ALERTED"
    original_enter = httpx.AsyncClient.__aenter__

    async def enter(client):
        store.signal(other)
        initial(store, other)
        return await original_enter(client)

    monkeypatch.setattr(httpx.AsyncClient, "__aenter__", enter)

    def forbidden(request):
        pytest.fail("Opposing alert became visible during HTTP client setup")

    notifier = Notifier(settings, store, httpx.MockTransport(forbidden))
    assert await notifier.send_research(signal) == "blocked:opposing-visible-active-setup"
    assert (
        store.db.execute("SELECT status FROM outbox WHERE signal_id=?", (signal.id,)).fetchone()[0]
        == "blocked:opposing-visible-active-setup"
    )
    store.close()


async def test_live_short_target_reaches_durable_discord_worker(settings, signal):
    from decimal import Decimal
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from bybit_flow.lifecycle import tick
    from bybit_flow.models import Trade
    from bybit_flow.orderflow import Book, Tape

    configured(settings)
    store = Store(settings.data_dir)
    signal.state, signal.direction, signal.horizon_profile = "ALERTED", "SHORT", "CORE_INTRADAY"
    signal.stop, signal.tp1, signal.tp2 = 105, 85, 80
    store.signal(signal)
    initial(store, signal)
    tape, book = Tape(), Book()
    for at, price in ((2000, "100"), (3000, "85")):
        tape.add(Trade(signal.symbol, at, at, str(at), "Sell", Decimal(price), Decimal(1)))
    tape.coverage_start = 1000
    book.valid = True
    book.event_ms = book.receipt_ms = 3000
    book.bids, book.asks = {Decimal("84.99"): Decimal(100)}, {Decimal("85.01"): Decimal(100)}
    streams = SimpleNamespace(books={signal.symbol: book}, tapes={signal.symbol: tape}, select=AsyncMock())
    scanner = SimpleNamespace(
        store=store,
        settings=settings,
        exchange=signal.source,
        api=None,
        streams=streams,
        recorder=SimpleNamespace(healthy=True),
        reconcile_pending=set(),
    )
    await tick(scanner, 3000)
    saved = store.signals()[0]
    assert saved["state"] == "RESOLVED"
    assert saved["evidence"]["primary_outcome"] == "TARGET"
    assert saved["evidence"]["terminal_event"]["effective_ms"] == 3000
    assert not store.active_signals()
    calls = []

    def transport(request):
        calls.append(request.method)
        return httpx.Response(200, json={"id": "result"})

    await Notifier(settings, store, httpx.MockTransport(transport)).retry_terminals()
    assert calls == ["PATCH", "POST"]
    store.close()


@pytest.mark.parametrize("destination", [None, "retired"])
async def test_unknown_or_retired_destination_cannot_post_result(settings, signal, destination):
    configured(settings)
    store = Store(settings.data_dir)
    initial(store, signal)
    store.put(f"discord_initial_webhook_id:{signal.id}", destination)
    resolve(store, signal)

    def forbidden(request):
        pytest.fail("A receipt from an unknown or retired destination is not visible here")

    notifier = Notifier(settings, store, httpx.MockTransport(forbidden))
    assert await notifier.send_price_result(signal) == "blocked:no-visible-initial"
    await notifier.retry_terminals()
    store.close()
