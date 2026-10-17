from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from bybit_flow.models import Candle
from bybit_flow.production import confirmation_entry_window, evaluate_intraday_confirmation


def stressed_setup(signal, direction):
    trigger = 900_000
    sign = 1 if direction == "LONG" else -1
    signal.direction = direction
    signal.family = "liquidity_sweep"
    signal.horizon_profile = "SHORT_INTRADAY"
    signal.created_ms = trigger + 1000
    signal.expires_ms = trigger + 900_000
    signal.trigger_expires_ms = signal.created_ms + 120_000
    signal.version += ":flow-quality-v1"
    signal.evidence.update(
        wick_regime=dict(state="WICKY"),
        structural_trigger=dict(valid=True, level=100, extreme=100 - sign * 2, available_ms=trigger),
        trigger_bar_end=trigger,
        volume_profile=dict(
            available=True,
            coverage_complete=True,
            available_ms=trigger + 600_000,
            end_ms=trigger + 600_000,
            profile_confidence="HIGH",
            **{"effective_rejection_low" if sign > 0 else "effective_rejection_high": True},
        ),
    )
    flow = dict(
        available=True,
        delta_pct=sign * 25,
        delta_persistence=0.8,
        cvd_slope=sign * 10,
        initiative_long=sign > 0,
        initiative_short=sign < 0,
        quality=dict(flow_trust_score=1),
    )
    bars = (
        [
            Candle(trigger, 300_000, 99, 102, 99, 101, 10, 1000),
            Candle(trigger + 300_000, 300_000, 101, 103, 100, 102, 10, 1000),
        ]
        if sign > 0
        else [
            Candle(trigger, 300_000, 101, 101, 98, 99, 10, 1000),
            Candle(trigger + 300_000, 300_000, 99, 100, 97, 98, 10, 1000),
        ]
    )
    return trigger, flow, bars


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_entry_window_survives_required_bars_without_weakening_confirmation(signal, direction):
    trigger, flow, bars = stressed_setup(signal, direction)
    old_deadline = signal.trigger_expires_ms
    before = signal.model_dump()
    timing = confirmation_entry_window(signal, signal.created_ms)
    assert signal.model_dump() == before  # Pure scheduling cannot rewrite an existing plan.
    ready = trigger + 600_000
    assert old_deadline < ready < timing["trigger_expires_ms"] <= signal.expires_ms
    assert timing["entry_window_start_ms"] == ready
    assert timing["trigger_expires_ms"] - ready == 120_000
    early = evaluate_intraday_confirmation(signal, flow, bars, ready - 1)
    assert not early["passed"] and not early["reclaim_hold_support"]
    assert evaluate_intraday_confirmation(signal, flow, bars, ready)["passed"]
    weak = deepcopy(flow)
    weak.update(delta_pct=0, cvd_slope=0)
    assert not evaluate_intraday_confirmation(signal, weak, bars, ready)["passed"]
    signal.evidence["volume_profile"]["profile_confidence"] = "LOW"
    assert not evaluate_intraday_confirmation(signal, flow, bars, ready)["passed"]


@pytest.mark.parametrize("change", ["normal", "core", "continuation"])
def test_other_entry_windows_keep_their_existing_two_minutes(signal, change):
    stressed_setup(signal, "LONG")
    if change == "normal":
        signal.evidence["wick_regime"]["state"] = "NORMAL"
    elif change == "core":
        signal.horizon_profile = "CORE_INTRADAY"
    else:
        signal.family = "trend_pullback"
    assert (
        confirmation_entry_window(signal, signal.created_ms)["trigger_expires_ms"]
        == signal.created_ms + 120_000
    )


def test_confirmation_window_never_extends_the_original_setup_expiry(signal):
    trigger, _, _ = stressed_setup(signal, "LONG")
    signal.expires_ms = trigger + 650_000
    assert confirmation_entry_window(signal, signal.created_ms)["trigger_expires_ms"] == signal.expires_ms


def test_late_discovery_keeps_two_minutes_from_observation(signal):
    trigger, _, _ = stressed_setup(signal, "LONG")
    now = trigger + 650_000
    assert confirmation_entry_window(signal, now)["trigger_expires_ms"] == now + 120_000


@pytest.mark.parametrize("existing", [False, True])
async def test_generation_schedules_only_new_plans_and_never_rearms_saved_plans(
    settings, instrument, signal, monkeypatch, existing
):
    from bybit_flow.horizons import assign
    from bybit_flow.scanner import Scanner
    from bybit_flow.storage import Store

    trigger, _, execution = stressed_setup(signal, "LONG")
    signal.evidence["initial_executable_quote"] = dict(available_ms=signal.created_ms, executable_price=100)
    base = signal.model_copy(deep=True)
    monkeypatch.setattr("bybit_flow.scanner.candidates", lambda *args, **kwargs: [base.model_copy(deep=True)])
    setup_bars = [Candle(0, trigger, 99, 103, 98, 101, 10, 1000)]

    async def cached(symbol, timeframe, now, limit):
        return execution if timeframe == "5" else setup_bars

    store = Store(settings.data_dir)
    scanner = SimpleNamespace(
        settings=settings.model_copy(update={"horizon_profiles": ["SHORT_INTRADAY"]}),
        store=store,
        flow_ready=lambda *args: True,
        cached_candles=cached,
        volume_research=AsyncMock(),
        generation_profile=Mock(return_value={}),
        remember_structures=Mock(),
        observe_generation=Mock(),
        observe_duplicate=Mock(),
    )
    try:
        if existing:
            assign(signal, "SHORT_INTRADAY")
            store.signal(signal)
            original = store.db.execute("SELECT payload FROM signals").fetchone()[0]
        await Scanner.discover_horizons(scanner, instrument, base.created_ms)
        saved = store.signals()[0]
        assert saved["entry"] == base.entry and saved["stop"] == base.stop
        assert saved["evidence"]["initial_executable_quote"] == base.evidence["initial_executable_quote"]
        if existing:
            assert store.db.execute("SELECT payload FROM signals").fetchone()[0] == original
            assert saved["trigger_expires_ms"] == base.trigger_expires_ms
        else:
            assert saved["trigger_expires_ms"] == trigger + 720_000
            original = store.db.execute("SELECT payload FROM signals").fetchone()[0]
        await Scanner.discover_horizons(scanner, instrument, base.created_ms + 60_000)
        assert store.db.execute("SELECT payload FROM signals").fetchone()[0] == original
        assert store.db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    finally:
        store.close()
