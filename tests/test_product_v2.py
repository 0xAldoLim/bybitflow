import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.horizons import assign
from bybit_flow.lifecycle import advance_trades
from bybit_flow.ml.features import snapshot
from bybit_flow.ml.inference import compatibility_reasons
from bybit_flow.ml.monitored import pending
from bybit_flow.ml.store import FeatureStore
from bybit_flow.models import Trade
from bybit_flow.notifications import Notifier, embed
from bybit_flow.orderflow import Tape
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store
from bybit_flow.v2 import FROZEN_FIELDS, SCHEMA, SCORE_PROFILE, initialize, overlaps_legacy, status
from bybit_flow.v2_evidence import combine, liquidation_sequence, location, mechanism, score, trade_offer


def trade(i, *, symbol="TESTUSDT", amount=1, price=100, source="binance"):
    return Trade(
        symbol,
        i * 1000,
        i * 1000,
        str(i),
        "Buy" if i % 2 else "Sell",
        Decimal(str(price)),
        Decimal(str(amount)),
        source,
    )


def populated_tape(source="binance", symbol="TESTUSDT", scale=1):
    tape = Tape(2000, source=source, symbol=symbol)
    for i in range(1, 181):
        tape.add(trade(i, symbol=symbol, amount=(1 + i % 10) * scale, source=source))
    return tape


def decision(signal):
    s = signal.model_copy(deep=True)
    s.created_ms = 1000
    s.evidence = dict(
        trigger_bar_end=1000,
        structural_trigger=dict(valid=True, level=99, available_ms=1000),
        context_features=dict(regime="trending up", efficiency=0.5),
        setup_features=dict(atr=2),
        execution_features=dict(atr=2),
        range=dict(low=98, high=110),
        flow=dict(
            available=True,
            delta_pct=30,
            delta_persistence=0.9,
            price_change=0.4,
            buy_notional=10000,
            sell_notional=5000,
            quality=dict(flow_trust_score=0.9),
        ),
        flow_quality=dict(window_end_ms=2000),
        trade_size_state=dict(
            available=True,
            coverage_complete=True,
            source=s.source,
            source_ms=2000,
            available_ms=2000,
            large_trade_direction="BUY",
        ),
        event_rotation_state=dict(
            available=True,
            coverage_complete=True,
            source=s.source,
            source_ms=2000,
            available_ms=2000,
            direction="UP",
        ),
        execution=dict(stop_noise_ratio=1),
        volume_profile=dict(
            available=True,
            coverage_complete=True,
            source=s.source,
            source_ms=2000,
            available_ms=2000,
            poc=98,
            val=98,
            vah=110,
            hvn=[],
        ),
        auction=dict(state="DISCOVERY_UP"),
    )
    s.risk = dict(accepted=True, net_rr=2.8, cost_per_base=0.2)
    s.coverage = dict(trade_window_complete=True)
    return s


def test_trade_quantile_classification_is_prior_only_and_closed_summary_does_not_change():
    tape = populated_tape()
    before = tape.event_features.snapshot(120_000, 180_000, 180_000, True)
    assert before[0]["available"]
    assert before[0]["quantile_known_ms"] < 180_000
    assert before[0]["quantiles"]["q97"] < 2000
    tape.add(trade(181, amount=1_000_000))
    after = tape.event_features.snapshot(120_000, 180_000, 181_000, True)
    for key in ("quantiles", "delta_by_size_bucket", "buy_notional_by_size", "sell_notional_by_size"):
        assert before[0][key] == after[0][key]
    assert before[1]["latest"] == after[1]["latest"]


def test_trade_size_is_symbol_relative_and_source_separated():
    a, b = populated_tape(), populated_tape("okx", "OTHERUSDT", 1000)
    ar = a.event_features.snapshot(120_000, 180_000, 180_000, True)[0]
    br = b.event_features.snapshot(120_000, 180_000, 180_000, True)[0]
    assert ar["source"] == "binance" and br["source"] == "okx"
    assert br["quantiles"]["q90"] == pytest.approx(ar["quantiles"]["q90"] * 1000)
    for bucket in ar["delta_by_size_bucket"]:
        assert br["delta_by_size_bucket"][bucket] == pytest.approx(ar["delta_by_size_bucket"][bucket] * 1000)
    with pytest.raises(ValueError, match="symbol-specific"):
        a.add(trade(181, symbol="OTHERUSDT"))


def test_event_rotation_threshold_is_causal_immutable_and_state_bounded():
    tape = populated_tape()
    closed = deepcopy(list(tape.event_features.rotations))
    assert closed
    assert all(r["threshold_known_ms"] <= r["start_ms"] <= r["end_ms"] for r in closed)
    assert all(r["notional"] >= r["threshold_notional"] for r in closed)
    tape.add(trade(181, price=101))
    assert list(tape.event_features.rotations)[: len(closed)] == closed
    for i in range(182, 3000):
        tape.add(trade(i))
    state = tape.event_features
    assert len(state.sizes) <= 512 and len(state.seconds) <= 181 and len(state.rotations) <= 128
    assert not state.snapshot(1000, 900_000, 3_000_000, True)[0]["available"]
    tape.reset(3_000_000)
    assert not tape.event_features.rotations and not tape.event_features.sizes


def test_future_or_incomplete_event_window_is_not_ready():
    state = populated_tape().event_features
    assert not state.snapshot(120_000, 200_000, 180_000, True)[0]["available"]
    assert not state.snapshot(120_000, 180_000, 180_000, False)[1]["available"]


def test_late_receipt_does_not_leave_a_falsely_complete_event_window():
    tape = populated_tape()
    tape.add(replace(trade(181), receipt_ms=500_000))
    size, event = tape.event_features.snapshot(120_000, 182_000, 200_000, True)
    assert not size["available"] and not event["available"]
    assert not size["coverage_complete"]


def test_invalid_print_or_wrong_source_cannot_mutate_incremental_state():
    tape = populated_tape()
    before = len(tape.trades), len(tape.event_features.sizes), tape.last_event
    for t in (trade(181, source="okx"), trade(181, amount=float("inf"))):
        with pytest.raises(ValueError):
            tape.add(t)
        assert before == (len(tape.trades), len(tape.event_features.sizes), tape.last_event)


def test_aggression_without_response_is_absorption_not_conviction(signal):
    s = decision(signal)
    s.family = "liquidity_sweep"
    s.evidence["flow"].update(
        delta_pct=-50,
        price_change=0,
        absorption_long=True,
        sell_notional=20000,
        defended_notional={"LONG": 10000},
    )
    result = mechanism(s, True)
    assert result["state"] == "DEFENDED_BID_ABSORPTION" and result["initiative_fraction"] == 0
    s.evidence["flow"]["absorption_long"] = False
    assert mechanism(s, True)["state"] == "INEFFICIENT_AGGRESSION"


def test_efficient_directional_displacement_and_aliases(signal):
    s = decision(signal)
    original = mechanism(s, True)
    assert original["state"] == "CONVICTION_BUY"
    s.evidence["flow"].update(stacked_buy=100, cvd=1e15, same_side_run=500)
    assert mechanism(s, True)["score"] == original["score"]
    s.evidence["flow"]["quality"]["flow_trust_score"] = 0.2
    assert mechanism(s, True)["score"] < original["score"]


def test_supported_large_trade_or_event_opposition_reduces_credit(signal):
    s = decision(signal)
    original = mechanism(s, True)["score"]
    s.evidence["trade_size_state"]["large_trade_direction"] = "SELL"
    assert mechanism(s, True)["score"] < original
    s.evidence["trade_size_state"]["available"] = False
    assert mechanism(s, True)["score"] < original


def test_balanced_center_is_weak_reversal_location_not_a_blanket_trend_penalty(signal):
    s = decision(signal)
    s.family = "range_rejection"
    s.entry = 104
    center = location(s, 2000)["score"]
    s.entry = 98.1
    assert location(s, 2000)["score"] > center
    s.entry = 104
    s.family = "trend_pullback"
    assert location(s, 2000)["score"] > center


def test_path_barriers_deduplicate_and_do_not_change_original_plan(signal):
    s = decision(signal)
    original = (s.entry, s.stop, s.tp1, s.tp2)
    clear = trade_offer(s, 2000)
    assert clear["path_state"] == "OPEN_PATH"
    s.evidence["volume_profile"].update(poc=105, hvn=[105, 106, 107])
    blocked = trade_offer(s, 2000)
    assert blocked["barrier_count"] == 3 and blocked["path_state"] == "HEAVY_FRICTION"
    assert blocked["score"] < clear["score"] and original == (s.entry, s.stop, s.tp1, s.tp2)
    s.evidence["volume_profile"]["available"] = False
    assert trade_offer(s, 2000)["score"] <= clear["score"]


def test_liquidation_acceleration_prevents_fade_and_decay_needs_prior_shock():
    row = dict(
        source="binance",
        source_ms=1000,
        available_ms=1000,
        production_ready=True,
        trusted_flow=True,
        long_liquidation_notional_1m=10000,
        short_liquidation_notional_1m=1,
        liquidation_intensity_percentile=0.99,
        liquidation_acceleration=2,
        liquidation_price_response_bps=-20,
    )
    flow = dict(delta_pct=-40, cvd_acceleration=-100)
    first = liquidation_sequence(row, flow, {}, 1000)
    assert first["continuation_risk"] and not first["fade_confirmed"]
    row.update(
        source_ms=2000, available_ms=2000, liquidation_acceleration=-0.7, liquidation_price_response_bps=10
    )
    flow.update(delta_pct=30, cvd_acceleration=100)
    recovered = liquidation_sequence(row, flow, first, 2000)
    assert recovered["state"] == "FADE_CONFIRMING"
    assert not liquidation_sequence(row, flow, {}, 2000)["fade_confirmed"]
    assert not liquidation_sequence(row, flow, first | dict(source="okx"), 2000)["fade_confirmed"]
    assert not liquidation_sequence(row, flow, first | dict(available_ms=3000), 2000)["fade_confirmed"]
    assert liquidation_sequence(row | dict(source_ms=3000), flow, first, 2000)["state"] == "UNAVAILABLE"


@pytest.mark.parametrize("components", [(100, 0, 100), (100, 100, 0), (0, 100, 100)])
def test_score_bottleneck_never_creates_elite_tier(components):
    assert combine(*components, 100) < 75


def test_missing_required_evidence_cannot_improve_score_or_confidence(signal):
    s = decision(signal)
    score(s, True, 2000)
    baseline = s.quality
    s.evidence["volume_profile"]["available"] = False
    score(s, True, 2000)
    assert s.quality <= baseline
    s.coverage["trade_window_complete"] = False
    score(s, True, 2000)
    assert s.quality == 0 and s.evidence["v2"]["evidence_confidence"] == 0


@pytest.mark.parametrize("state", ["WATCHLIST", "PENDING CONFIRMATION", "CONFIRMED", "ALERTED"])
def test_cutover_preserves_all_original_plan_fields_and_history(settings, signal, state):
    store = Store(settings.data_dir)
    old = decision(signal)
    old.feature_schema_version = "candidate-v10"
    old.state = state
    old.quality, old.raw_tier = 68.5, "B"
    old.evidence["score_components"] = {"orderflow": dict(earned=20)}
    store.signal(old)
    before = deepcopy(store.signals()[0])
    snapshots = [tuple(r) for r in store.db.execute("SELECT * FROM ml_snapshots")]
    marker = initialize(store, 5000, commit="a" * 40)
    assert initialize(store, 9000, commit="b" * 40) == marker
    with pytest.raises(ValueError, match="immutable"):
        store.put("bybitflow_v2_cutover", dict(started_ms=10000))
    changed = old.model_copy(deep=True)
    changed.entry, changed.stop, changed.tp1 = 200, 190, 250
    changed.zone = (199, 201)
    changed.quality, changed.raw_tier = 99, "SSS"
    changed.expires_ms += 99999
    changed.feature_schema_version = SCHEMA
    changed.evidence["v2"] = {"quality_score": 99}
    changed.coverage["monitoring"] = "paused"
    store.signal(changed)
    saved = store.signals()[0]
    assert {k: saved[k] for k in FROZEN_FIELDS} == {k: before[k] for k in FROZEN_FIELDS}
    assert saved["coverage"]["monitoring"] == "paused" and "v2" not in saved["evidence"]
    assert snapshots == [tuple(r) for r in store.db.execute("SELECT * FROM ml_snapshots")]
    changed.state = "RESOLVED"
    changed.evidence["primary_outcome"] = "TARGET"
    store.signal(changed, "Original target observed")
    assert store.signals()[0]["state"] == "RESOLVED"
    assert store.signals()[0]["evidence"]["primary_outcome"] == "TARGET"
    store.close()


def test_one_new_plan_one_schema_one_frozen_decision_and_v2_card(settings, signal):
    store = Store(settings.data_dir)
    initialize(store, 500, commit="a" * 40)
    s = decision(signal)
    assign(s, "CORE_INTRADAY")
    score(s, True, 2000)
    s.state = "CONFIRMED"
    store.signal(s)
    store.signal(s)
    rows = store.db.execute("SELECT stage,schema_version,count(*) FROM ml_snapshots GROUP BY 1,2").fetchall()
    assert {(r[0], r[1], r[2]) for r in rows} == {("generation", SCHEMA, 1), ("decision", SCHEMA, 1)}
    assert len(store.signals()) == 1
    assert s.evidence["score_profile"] == SCORE_PROFILE
    assert any(f["name"] == "Evidence · v2" for f in embed(s, "http://localhost")["embeds"][0]["fields"])
    assert status(store)["v20_decisions"] == 1
    store.close()


def test_old_and_new_snapshots_use_their_own_catalog_and_schema(signal):
    old = signal.model_copy(update={"feature_schema_version": "candidate-v10"})
    legacy = snapshot(old, 2000, "decision")
    new = snapshot(signal, 2000, "decision")
    assert legacy["schema_version"] == "candidate-v10" and "v2_location" not in legacy["values"]
    assert new["schema_version"] == SCHEMA and "v2_location" in new["values"]


def test_legacy_monitored_recovery_survives_global_schema_change(settings, signal, monkeypatch):
    monkeypatch.setattr("bybit_flow.storage.now_ms", lambda: 2000)
    store = Store(settings.data_dir)
    old = decision(signal)
    old.feature_schema_version = "candidate-v10"
    old.source = "binance"
    old.holding_deadline_ms = 3000
    old.evidence["score_components"] = {"orderflow": dict(earned=20)}
    store.signal(old)
    initialize(store, 5000, commit="a" * 40)
    assert pending(store, "binance", 6000)[0]["schema_version"] == "candidate-v10"
    assert FeatureStore(store).dataset(6000, schema_version=SCHEMA) == []
    store.close()


def test_cutover_does_not_recreate_old_active_opportunity(signal):
    old = signal.model_copy(update={"created_ms": 1000, "feature_schema_version": "candidate-v10"})
    old.evidence["trigger_bar_end"] = 500
    new = old.model_copy(deep=True)
    new.created_ms = 3000
    new.feature_schema_version = SCHEMA
    assert overlaps_legacy(new, [old.model_dump()], dict(started_ms=2000))
    new.symbol = "OTHERUSDT"
    assert not overlaps_legacy(new, [old.model_dump()], dict(started_ms=2000))


@pytest.mark.asyncio
async def test_old_pending_plan_never_enters_new_scorer_or_confirmation(settings, signal):
    store = Store(settings.data_dir)
    signal.feature_schema_version = "candidate-v10"
    signal.state = "PENDING CONFIRMATION"
    store.signal(signal)
    initialize(store, 2000, commit="a" * 40)
    scanner = object.__new__(Scanner)
    scanner.store = store
    scanner.api = SimpleNamespace(name="bybit")
    scanner.recorder = SimpleNamespace(critical_symbols=set())
    scanner.reconcile_pending = set()
    scanner.notifier = SimpleNamespace(send_research=AsyncMock())
    await scanner.evaluate()
    assert store.signals()[0]["state"] == "PENDING CONFIRMATION"
    scanner.notifier.send_research.assert_not_awaited()
    store.close()


def test_incompatible_old_model_cannot_apply_to_v20(signal):
    model = dict(
        id="old",
        feature_schema_version="candidate-v10",
        created_ms=0,
        source=signal.source,
        stage="decision",
        strategy_versions=[signal.version],
        periods=dict(holdout=dict(end=2000)),
    )
    registry = SimpleNamespace(is_degraded=lambda _: False)
    reasons = compatibility_reasons(model, signal, snapshot(signal, 2000, "decision"), 2000, registry)
    assert "feature schema changed; retraining required" in reasons


def test_original_lifecycle_observes_entry_then_original_target_after_cutover(settings, signal):
    store = Store(settings.data_dir)
    old = decision(signal)
    old.feature_schema_version, old.horizon_profile, old.state = "candidate-v10", "CORE_INTRADAY", "ALERTED"
    old.created_ms, old.expires_ms, old.holding_deadline_ms = 1000, 10_000, 20_000
    store.signal(old)
    before = deepcopy(store.signals()[0])
    initialize(store, 5000, commit="a" * 40)
    tape = Tape(source=old.source, symbol=old.symbol)
    tape.add(trade(6, price=old.entry, source=old.source))
    tape.add(trade(7, price=old.tp1, source=old.source))
    advance_trades(old, tape, 8000)
    store.signal(old, "Original target crossed")
    after = store.signals()[0]
    assert after["state"] == "RESOLVED" and after["evidence"]["primary_outcome"] == "TARGET"
    assert after["evidence"]["observed_entry_ms"] == 6000
    assert {k: after[k] for k in FROZEN_FIELDS} == {k: before[k] for k in FROZEN_FIELDS}
    store.close()


async def test_v20_opportunity_has_one_initial_discord_delivery(settings, signal, monkeypatch):
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: 2000)
    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/123/offline-fixture")
    store = Store(settings.data_dir)
    initialize(store, 500, commit="a" * 40)
    s = decision(signal)
    s.state = "CONFIRMED"
    score(s, True, 2000)
    store.signal(s)
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"id": "v20-card", "channel_id": "test-channel"})

    notifier = Notifier(settings, store, httpx.MockTransport(respond))
    assert await notifier.send_research(s) == "sent"
    await notifier.send_research(s)
    assert len(calls) == 1 and notifier.was_initially_delivered(s.id, "research")
    assert store.db.execute("SELECT count(*) FROM outbox WHERE status='sent'").fetchone()[0] == 1
    store.close()


def test_installed_package_can_initialize_outside_a_git_checkout(settings, monkeypatch):
    monkeypatch.chdir(settings.data_dir)
    monkeypatch.setattr("bybit_flow.v2.ROOT", settings.data_dir)
    store = Store(settings.data_dir)
    marker = initialize(store, 5000, commit="a" * 40)
    assert len(marker["research_manifest_hash"]) == len(marker["research_ledger_hash"]) == 64
    assert marker["candidate_schema"] == SCHEMA
    store.close()


def test_packaged_research_identity_matches_portable_committed_documents():
    root = Path(__file__).resolve().parents[1] / "docs" / "v2"
    identity = json.loads(files("bybit_flow").joinpath("research_v2.json").read_text(encoding="utf-8"))
    for key, name in (
        ("research_manifest_hash", "INSILICO_SOURCE_MANIFEST.md"),
        ("research_ledger_hash", "INSILICO_RESEARCH_LEDGER.md"),
    ):
        assert identity[key] == hashlib.sha256((root / name).read_bytes()).hexdigest()
