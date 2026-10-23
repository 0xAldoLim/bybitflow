"""Last-mile evidence, all-valid delivery, visibility and frozen published plans."""

import json
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr
from test_setup_v22 import NOW, book, tagged

from bybit_flow.identity import fingerprint
from bybit_flow.models import Candle, Signal, Trade
from bybit_flow.notifications import Notifier, embed
from bybit_flow.orderflow import Tape
from bybit_flow.quality_v22 import apply
from bybit_flow.setup_ranking import rank
from bybit_flow.setup_runtime import Runtime
from bybit_flow.storage import Store


def harness(signal, settings, instrument, monkeypatch, transport=None):
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: NOW)
    monkeypatch.setattr("bybit_flow.setup_runtime.now_ms", lambda: NOW)
    settings = settings.model_copy(
        update=dict(
            research_alerts=True,
            research_webhook=SecretStr("https://discord.com/api/webhooks/signals/testing"),
            monitoring_webhook=SecretStr("https://discord.com/api/webhooks/monitoring/testing"),
        )
    )
    s, b = tagged(signal, settings, instrument)
    s.state = "CONFIRMED"
    apply(s, 100.01, NOW, minimum_rr=settings.min_net_rr, book=b, settings=settings)
    store = Store(settings.data_dir)
    scanner = SimpleNamespace(
        settings=settings,
        store=store,
        exchange=s.source,
        recorder=SimpleNamespace(healthy=True),
        streams=SimpleNamespace(books={s.symbol: b}, tapes={s.symbol: object()}, connected=True),
        candle_cache={
            (s.symbol, s.execution_timeframe): (NOW, [Candle(0, 60000, 100, 100.5, 99.5, 100.3, 1, 100)])
        },
    )
    runtime = Runtime(scanner)

    async def fresh(s, *args):
        return deepcopy(s.evidence["flow"]) | dict(quality=deepcopy(s.evidence["flow_quality"]))

    runtime.fresh_flow = fresh
    notifier = Notifier(settings, store, transport)
    notifier.prepare_initial, notifier.finalize_initial = runtime.prepare_delivery, runtime.finalize_delivery
    notifier.guard_initial = runtime.dispatch_guard
    scanner.notifier = notifier
    store.signal(s)
    return s, b, store, scanner, runtime, notifier


def transport_recorder():
    posts = []

    def send(request):
        posts.append(json.loads(request.content))
        return httpx.Response(200, json=dict(id=str(len(posts)), channel_id="test-channel"))

    return posts, httpx.MockTransport(send)


@pytest.mark.asyncio
async def test_delivery_snapshot_uses_changed_executable_price(signal, settings, instrument, monkeypatch):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    decision = store.db.execute(
        "SELECT payload FROM ml_snapshots WHERE signal_id=? AND stage='decision'", (s.id,)
    ).fetchone()[0]
    moved = book(bid="100.19", ask="100.20")
    b.bids, b.asks = moved.bids, moved.asks
    assert await notifier.send_research(s) == "sent"
    snap = s.evidence["delivery_execution_snapshot"]
    assert snap["executable_price"] == 100.2
    assert snap["setup_score"] == s.quality
    assert "100.2 observed ask" in posts[0]["embeds"][0]["fields"][0]["value"]
    assert (
        store.db.execute(
            "SELECT payload FROM ml_snapshots WHERE signal_id=? AND stage='decision'", (s.id,)
        ).fetchone()[0]
        == decision
    )
    persisted = json.loads(store.db.execute("SELECT payload FROM signals WHERE id=?", (s.id,)).fetchone()[0])
    assert persisted["evidence"]["delivery_execution_snapshot"] == snap
    s.state = "ALERTED"
    store.signal(s)
    jobs = dict(store.db.execute("SELECT signal_id,payload FROM setup_path_jobs").fetchall())
    delivery_job = json.loads(jobs[s.id + ":delivery"])
    assert delivery_job["visible_at_entry"] and delivery_job["stage"] == "delivery"
    assert delivery_job["decision_ms"] >= delivery_job["delivered_ms"]
    assert delivery_job["entry"] == 100.2
    assert delivery_job["risk"] == abs(s.entry - s.stop)
    assert json.loads(jobs[s.id])["stage"] == "decision"
    store.close()


@pytest.mark.asyncio
async def test_accounting_latency_cannot_dispatch_stale_snapshot(signal, settings, instrument, monkeypatch):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    original = store.put

    def delayed(key, value):
        original(key, value)
        if key == "v22_http_dispatched:" + s.id and value is True:
            monkeypatch.setattr("bybit_flow.setup_runtime.now_ms", lambda: NOW + 10000)

    monkeypatch.setattr(store, "put", delayed)
    assert await notifier.send_research(s) == "blocked:EXECUTION_QUOTE_STALE"
    assert not posts
    assert store.get("v22_http_dispatched:" + s.id) is False
    assert not store.db.execute(
        "SELECT 1 FROM setup_path_jobs WHERE signal_id=?", (s.id + ":delivery",)
    ).fetchone()
    store.close()


@pytest.mark.asyncio
async def test_identical_visible_historical_setup_keeps_its_lifecycle(
    signal, settings, instrument, monkeypatch
):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    old = deepcopy(s)
    s.evidence["trigger_bar_end"] = old.evidence["trigger_bar_end"] = 60000
    old.id = "already-active-v211"
    old.evidence["production_policies"]["trade_quality"] = "trade-validity-v21.1"
    old.evidence.pop("v22", None)
    store.signal(old)
    assert await notifier.send_research(old) == "sent"
    old.state = "ALERTED"
    store.monitor(old)
    frozen = store.db.execute("SELECT payload FROM signals WHERE id=?", (old.id,)).fetchone()[0]
    s.family = "breakout_retest"
    assert await notifier.send_research(s) == "duplicate-plan-suppressed"
    assert store.db.execute("SELECT payload FROM signals WHERE id=?", (old.id,)).fetchone()[0] == frozen
    assert {s.id, old.id} <= {r["id"] for r in store.active_signals()}
    assert len(posts) == 1
    store.close()


@pytest.mark.asyncio
async def test_deteriorated_delivery_rr_blocks_http_and_safe_unattempted_retry(
    signal, settings, instrument, monkeypatch
):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)

    def deteriorate(signal):
        b.bids, b.asks = book(bid="100.99", ask="101.00").bids, book(bid="100.99", ask="101.00").asks
        return runtime.finalize_delivery(signal)

    notifier.finalize_initial = deteriorate
    result = await notifier.send_research(s)
    assert result.startswith("blocked:") and "RR_DEGRADED" in result
    assert not posts and store.get("v22_http_dispatched:" + s.id) is False
    b.bids, b.asks = book().bids, book().asks
    notifier.finalize_initial = runtime.finalize_delivery
    assert await notifier.send_research(s) == "sent"
    assert len(posts) == 1
    store.close()


@pytest.mark.parametrize("fault", ["stale", "spread", "depth", "disconnected"])
@pytest.mark.asyncio
async def test_bad_delivery_evidence_never_dispatches(signal, settings, instrument, monkeypatch, fault):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    if fault == "stale":
        b.event_ms = b.receipt_ms = NOW - 6000
    elif fault == "spread":
        wide = book(bid="99.9", ask="100.1")
        b.bids, b.asks = wide.bids, wide.asks
    elif fault == "depth":
        b.asks = book(quantity="0.001").asks
    else:
        scanner.streams.connected = False
    assert (await notifier.send_research(s)).startswith("blocked:")
    assert not posts
    store.close()


@pytest.mark.asyncio
async def test_twelve_independent_valid_setups_all_reach_discord(signal, settings, instrument, monkeypatch):
    posts, transport = transport_recorder()
    base, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    signals = []
    for i in range(12):
        s = deepcopy(base)
        s.id, s.symbol = f"independent-{i}", f"MARKET{i}USDT"
        s.evidence["market_factor"] = dict(beta_to_btc=1.2)
        scanner.streams.books[s.symbol] = b
        scanner.streams.tapes[s.symbol] = object()
        scanner.candle_cache[(s.symbol, s.execution_timeframe)] = scanner.candle_cache[
            (base.symbol, base.execution_timeframe)
        ]
        store.signal(s)
        signals.append(s)
    assert len(rank(signals, NOW)) == 12
    assert all(r["shared_exposure"] == "HIGH" for r in rank(signals, NOW).values())
    for s in signals:
        assert await notifier.send_research(s) == "sent"
        s.state = "ALERTED"
        store.signal(s)
    assert len(posts) == 12
    assert store.db.execute("SELECT count(*) FROM outbox WHERE status='sent'").fetchone()[0] == 12
    store.close()


@pytest.mark.parametrize("quality,expected", [(94, "disabled"), (96, "sent")])
@pytest.mark.asyncio
async def test_existing_sss_only_opt_in_uses_final_v22_score(
    signal, settings, instrument, monkeypatch, quality, expected
):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    scanner.settings.research_alerts = False
    scanner.settings.sss_research = True
    # Exercise qualification ordering independently of the scoring arithmetic.
    monkeypatch.setattr("bybit_flow.setup_scoring.combine", lambda *args: quality)
    apply(s, 100.01, NOW, book=b, settings=scanner.settings)
    store.signal(s)
    assert await notifier.send_research(s) == expected
    assert len(posts) == int(expected == "sent")
    store.close()


@pytest.mark.asyncio
async def test_cross_family_duplicate_is_monitored_but_has_no_orphan_update(
    signal, settings, instrument, monkeypatch
):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    child = deepcopy(s)
    child.id, child.family = "other-recognizer", "breakout_retest"
    store.signal(child)
    assert fingerprint(child) == fingerprint(s)
    assert await notifier.send_research(s) == "sent"
    assert await notifier.send_research(child) == "duplicate-plan-suppressed"
    child.coverage.update(monitoring="paused", monitoring_event="paused", pause_since_ms=NOW)
    store.monitor(child)
    assert await notifier.send_research(child, update=True) == "blocked:no-visible-initial"
    assert len(posts) == 1
    assert {s.id, child.id} <= {r["id"] for r in store.active_signals()}
    store.close()


@pytest.mark.asyncio
async def test_frozen_card_and_one_deterioration_per_episode(signal, settings, instrument, monkeypatch):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    assert await notifier.send_research(s) == "sent"
    s.state = "ALERTED"
    store.signal(s)
    snapshot = deepcopy(s.evidence["delivery_execution_snapshot"])
    original = s.quality
    s.quality, s.tp1 = 1, 101
    s.evidence["v22"]["setup_score"] = 1
    s.evidence["delivery_execution_snapshot"]["setup_score"] = 1
    s.coverage["v22_current"] = dict(state="DETERIORATING", reason="Persistent adverse response")
    store.monitor(s)
    saved = Signal.model_validate_json(
        store.db.execute("SELECT payload FROM signals WHERE id=?", (s.id,)).fetchone()[0]
    )
    assert saved.quality == original and saved.tp1 == 106
    assert saved.evidence["delivery_execution_snapshot"] == snapshot
    assert saved.coverage["v22_current"]["state"] == "DETERIORATING"
    assert await notifier.send_deterioration(saved, NOW) == "sent"
    assert await notifier.send_deterioration(saved, NOW) == "already-attempted"
    assert len(posts) == 2
    saved.state = "RESOLVED"
    store.signal(saved)
    assert await notifier.send_deterioration(saved, NOW + 60000) == "blocked:terminal-or-unpublished"
    store.close()


def test_card_is_compact_has_one_primary_score_and_explains_limits(signal, settings, instrument):
    s, b = tagged(signal, settings, instrument)
    apply(s, 100.01, NOW, book=b, settings=settings)
    card = embed(s, "http://localhost")["embeds"][0]
    assert "Setup Score" in card["description"]
    assert "win probability" in card["footer"]["text"]
    assert "Strengths / limiting factors" in {r["name"] for r in card["fields"]}
    assert len(card["fields"]) <= 12
    assert sum(len(r["name"]) + len(r["value"]) for r in card["fields"]) + len(card["description"]) < 4500
    assert "ML Research" not in {r["name"] for r in card["fields"]}


@pytest.mark.asyncio
async def test_native_flow_refresh_runs_without_cross_thread_store_access(
    signal, settings, instrument, monkeypatch
):
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch)
    scanner.context = {s.symbol: dict(instrument=instrument)}
    tape = Tape(source=s.source, symbol=s.symbol)
    tape.reset(1)
    for i in range(60):
        at = 60000 + 1000 * i
        tape.add(
            Trade(
                s.symbol,
                at,
                at,
                str(i),
                "Buy",
                Decimal(100) + Decimal(i) / 1000,
                Decimal(1) + Decimal(i) / 100,
                exchange=s.source,
            )
        )
    flow = await Runtime.fresh_flow(runtime, s, b, tape, NOW)
    assert flow["available"] and flow["quality"]["window_end_ms"] == NOW
    flow["quality"]["flow_trust_score"] = -999
    cached = await Runtime.fresh_flow(runtime, s, b, tape, NOW + 1)
    assert cached["quality"]["flow_trust_score"] >= 0
    assert len(runtime.windows) == 1
    tape.reset(NOW)
    assert await Runtime.fresh_flow(runtime, s, b, tape, NOW) is None
    store.close()


@pytest.mark.asyncio
async def test_current_health_requires_distinct_windows_and_deduplicates_episode(
    signal, settings, instrument, monkeypatch
):
    posts, transport = transport_recorder()
    s, b, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    assert await notifier.send_research(s) == "sent"
    s.state = "ALERTED"
    store.signal(s)
    flow = deepcopy(s.evidence["flow"])
    flow["quality"] = deepcopy(s.evidence["flow_quality"])
    flow["quality"]["price_displacement_bps"] = -10
    for at in (NOW, NOW, NOW + 60000, NOW + 60000, NOW + 120000):
        flow["quality"].update(available_ms=at, window_end_ms=at)
        await runtime.observe_health(s, flow, b.features(NOW), True, b, at)
    assert s.coverage["v22_current"]["state"] == "DETERIORATING"
    assert s.coverage["v22_current"]["bad_windows"] == 3
    assert len(posts) == 2
    assert s.evidence["delivery_execution_snapshot"]["setup_score"] == s.quality
    store.close()
