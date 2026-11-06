"""Focused causal sequence checks using actual tape/footprint calculations."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from bybit_flow.impact_efficiency import assess as impact
from bybit_flow.models import Candle, Trade
from bybit_flow.orderflow import Book
from bybit_flow.production import evaluate_intraday_confirmation
from bybit_flow.reversal_sequence import POLICY, augment, delivery_ready, observe, selected_flow
from bybit_flow.scanner_confirmation import ConfirmationEngine
from bybit_flow.strategy import candidates

NOW = 180000


def scenario(signal, direction="LONG", absorption=False):
    sign = 1 if direction == "LONG" else -1
    signal.direction, signal.family, signal.horizon_profile = direction, "range_rejection", "CORE_INTRADAY"
    signal.version += ":flow-quality-v1"
    signal.evidence.update(
        production_policies=dict(reversal_timing=POLICY),
        structural_trigger=dict(valid=True, available_ms=60000, level=100),
        frozen_structure=dict(available_ms=60000, frozen_reference=100, reasons=[]),
        execution_features=dict(atr=0.5),
        trigger_bar_end=60000,
    )
    book = Book()
    book.valid = True
    trades = []
    for second in range(75):
        if second < 45:
            offset = 0.03 - second * 0.001 if absorption else 0.1 - second * 0.006
            size = 5 + second % 3
        elif second < 60:
            offset = -0.014 + (second - 45) * 0.002 if absorption else -0.16 + (second - 45) * 0.004
            size = 6 if absorption else 1 + second % 3 * 0.1
        else:
            offset, size = -0.05 + (second - 60) * 0.02, 4 + second % 4
        price = Decimal(str(100 + sign * offset))
        time = 105000 + second * 1000
        side = ("Buy" if sign > 0 else "Sell") if second >= 60 else ("Sell" if sign > 0 else "Buy")
        trades.append(
            Trade(signal.symbol, time, time, str(second), side, price, Decimal(str(size)), signal.source)
        )
        if absorption and second < 60:
            book.changes.append((time + 1, "bid" if sign > 0 else "ask", price, Decimal("20")))
    ofi = dict(
        source=signal.source,
        symbol=signal.symbol,
        available_ms=NOW,
        source_ms=NOW - 1000,
        state="READY",
        epoch=3,
        windows={
            "15": dict(
                coverage_complete=True,
                start_ms=165000,
                end_ms=NOW,
                source=signal.source,
                symbol=signal.symbol,
                touch=dict(directional=2, persistence=1),
            )
        },
    )
    kwargs = dict(coverage_start=1000, book_start=1000)
    return trades, book, ofi, kwargs


def run(signal, trades, book, ofi, kwargs):
    return observe(signal, trades, Decimal("0.01"), 0.5, book, {}, ofi, NOW, **kwargs)


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
@pytest.mark.parametrize("absorption", [False, True])
def test_actual_pressure_failure_turn_and_hold_confirm(signal, direction, absorption):
    data = scenario(signal, direction, absorption)
    row = run(signal, *data)
    assert row["state"] == "CONFIRMED"
    assert row["absorption"] == absorption
    if not absorption:
        assert row["exhaustion"]
    assert row["context_flow"]["delta_pct"] * (1 if direction == "LONG" else -1) < -10
    signal.evidence["reversal_sequence"] = row
    signal.coverage["trade_window_complete"] = True
    baseline_flow = deepcopy(row["context_flow"])
    signal.evidence["flow"] = baseline_flow
    signal.evidence["flow_quality"] = baseline_flow["quality"]
    bars = [Candle(0, 60000, 100, 101, 99, 100, 10, 1000)]
    result = evaluate_intraday_confirmation(signal, baseline_flow, bars, NOW)
    assert not result["baseline_passed"] and result["passed"]
    assert result["flow_confirmation_mode"] == "LOCAL_STAGED_REVERSAL"
    signal.evidence["confirmation"] = result
    signal.evidence["flow_confirmation_mode"] = result["flow_confirmation_mode"]
    process = impact(signal, NOW)
    assert process["coverage_complete"] and not process["contradicted"]
    assert process["assessment_window"] == "STAGED_TRIGGER_15S"
    assert signal.evidence["flow"] == baseline_flow  # Full raw context is never rewritten.
    assert selected_flow(signal, NOW + 15001) is None
    assert not delivery_ready(signal, NOW + 15001)
    assert not augment(signal, dict(passed=False), NOW, factor_ok=False)["passed"]


@pytest.mark.parametrize(
    "fault", ["reset", "source", "ofi", "gap", "continuing", "no_hold", "cascade", "future", "frozen_future"]
)
def test_incomplete_or_adverse_sequence_never_supplies_confirmation(signal, fault):
    trades, book, ofi, kwargs = scenario(signal)
    if fault == "reset":
        kwargs["book_start"] = 150000
    elif fault == "source":
        trades[0] = replace(trades[0], exchange="okx")
    elif fault == "ofi":
        ofi["windows"]["15"]["touch"]["directional"] = -1
    elif fault == "gap":
        trades = [t for t in trades if t.event_ms < 145000 or t.event_ms >= 157000]
    elif fault == "continuing":
        trades = [
            replace(t, price=t.price - Decimal("0.3")) if 150000 <= t.event_ms < 165000 else t for t in trades
        ]
    elif fault == "no_hold":
        trades[-1] = replace(trades[-1], price=Decimal("99.9"))
    elif fault == "cascade":
        kwargs["liquidation"] = dict(state="LONG_LIQUIDATION_CASCADE", available_ms=NOW)
    elif fault == "future":
        ofi["available_ms"] = NOW + 1
    else:
        signal.evidence["frozen_structure"]["available_ms"] = NOW + 1
    row = run(signal, trades, book, ofi, kwargs)
    assert row["state"] != "CONFIRMED"
    signal.evidence["reversal_sequence"] = row
    assert not augment(signal, dict(passed=False), NOW)["passed"]
    assert augment(signal, dict(passed=True), NOW)["passed"]  # Existing baseline remains available.


def test_old_policy_and_frozen_plan_are_unchanged(signal):
    data = scenario(signal)
    row = run(signal, *data)
    signal.evidence["reversal_sequence"] = row
    signal.evidence["production_policies"].pop("reversal_timing")
    before = signal.model_dump()
    baseline = dict(passed=False, reason_codes=["UNCONFIRMED_LIQUIDITY_SWEEP"])
    assert augment(signal, baseline, NOW) is baseline
    assert signal.model_dump() == before


@pytest.mark.parametrize("horizon", ["SHORT_INTRADAY", "CORE_INTRADAY", "SWING", "EXTENDED_SWING"])
def test_only_new_supported_candidates_receive_distinct_policy_identity(instrument, monkeypatch, horizon):
    from test_quality_v211 import bar, flat

    bars = flat()
    bar(bars, -1, 97, 101, 99)
    feature = dict(regime="range", atr=2, low=98, high=102, ma20=100, sweep_long=True, sweep_short=False)
    monkeypatch.setattr("bybit_flow.strategy.candle_features", lambda *a: dict(feature))
    monkeypatch.setattr("bybit_flow.quality_v211.trend_qualification", lambda *a: {})
    args = dict(
        families=["range_rejection"],
        execution_window_ms=60000,
        quality_policy="trade-validity-v22",
        horizon=horizon,
    )
    old = candidates(instrument, bars, bars, bars, bars[-1].end, **args)
    new = candidates(instrument, bars, bars, bars, bars[-1].end, reversal_timing=True, **args)
    assert old and len(old) == len(new)
    for prior, current in zip(old, new):
        tagged = horizon != "EXTENDED_SWING"
        assert (POLICY in current.version) == tagged
        assert (prior.id != current.id) == tagged
        assert not prior.evidence["production_policies"].get("reversal_timing")
        assert (prior.entry, prior.stop, prior.tp1, prior.expires_ms) == (
            current.entry,
            current.stop,
            current.tp1,
            current.expires_ms,
        )


async def test_reconnect_during_worker_is_not_latched_and_delivery_keeps_original_decision(
    signal, monkeypatch
):
    trades, book, ofi, _ = scenario(signal)
    book.event_ofi_start_ms = 1000
    book.event_ofi_epoch = 3
    book.fresh = lambda *_: True
    book.features = lambda *_: {}
    tape = SimpleNamespace(coverage_start=1000, window=lambda *_: trades)
    runtime = SimpleNamespace(
        cached_features=lambda *_: dict(atr=0.5), settings=SimpleNamespace(book_stale_ms=5000)
    )
    engine = ConfirmationEngine(runtime)
    monkeypatch.setattr("bybit_flow.ofi.event_assess", lambda *_: ofi)
    assert await engine.refresh_reversal(
        signal, {"instrument": SimpleNamespace(tick=Decimal("0.01"))}, tape, book, [], NOW
    )
    assert signal.evidence["reversal_sequence"]["state"] == "CONFIRMED"
    decision = deepcopy(signal.evidence)
    signal.state = "CONFIRMED"
    assert await engine.refresh_reversal(
        signal, {"instrument": SimpleNamespace(tick=Decimal("0.01"))}, tape, book, [], NOW
    )
    assert (
        signal.evidence == decision and signal.coverage["delivery_reversal_sequence"]["state"] == "CONFIRMED"
    )
    original = __import__("bybit_flow.reversal_sequence", fromlist=["observe"]).observe

    def reset(*args, **kwargs):
        row = original(*args, **kwargs)
        book.event_ofi_epoch += 1
        return row

    monkeypatch.setattr("bybit_flow.reversal_sequence.observe", reset)
    assert not await engine.refresh_reversal(
        signal, {"instrument": SimpleNamespace(tick=Decimal("0.01"))}, tape, book, [], NOW
    )
    assert signal.evidence == decision


@pytest.mark.parametrize("horizon", ["CORE_INTRADAY", "SWING"])
async def test_staged_plan_reaches_existing_discord_path_with_fresh_delivery_evidence(
    signal, settings, instrument, monkeypatch, horizon
):
    from test_v22_delivery import harness, transport_recorder

    from bybit_flow.quality_v22 import POLICY as QUALITY_POLICY

    posts, transport = transport_recorder()
    s, book, store, scanner, runtime, notifier = harness(signal, settings, instrument, monkeypatch, transport)
    trades, observed_book, ofi, _ = scenario(s)
    s.evidence["production_policies"]["trade_quality"] = QUALITY_POLICY
    s.horizon_profile = horizon
    row = run(s, trades, observed_book, ofi, dict(coverage_start=1000, book_start=1000))
    assert row["state"] == "CONFIRMED"
    # A delayed send must refresh evidence, not reuse a once-valid old turn.
    frozen = deepcopy(row) | dict(available_ms=NOW - 20000, window_end_ms=NOW - 20000)
    s.evidence.update(
        reversal_sequence=frozen,
        flow=row["context_flow"],
        flow_quality=row["context_flow"]["quality"],
        flow_confirmation_mode="LOCAL_STAGED_REVERSAL",
    )
    s.gates = []
    scanner.context = {s.symbol: dict(instrument=instrument)}
    scanner.cached_features = lambda *_: dict(atr=0.5)
    scanner.confirmation = ConfirmationEngine(scanner)
    scanner.streams.tapes[s.symbol] = SimpleNamespace(
        coverage_start=1000,
        last_event=NOW - 1000,
        last_receipt=NOW - 1000,
        window=lambda *_: trades,
    )
    book.event_ms = book.receipt_ms = NOW
    book.event_ofi_start_ms = 1000
    book.event_ofi_epoch = 3
    monkeypatch.setattr("bybit_flow.scanner_confirmation.now_ms", lambda: NOW)
    monkeypatch.setattr("bybit_flow.setup_runtime.now_ms", lambda: NOW)
    monkeypatch.setattr("bybit_flow.notifications.now_ms", lambda: NOW)
    monkeypatch.setattr("bybit_flow.ofi.event_assess", lambda *_: deepcopy(ofi))
    monkeypatch.setattr("bybit_flow.setup_runtime.event_assess", lambda *_: deepcopy(ofi))
    store.signal(s)
    decision = store.db.execute(
        "SELECT payload FROM ml_snapshots WHERE signal_id=? AND stage='decision'", (s.id,)
    ).fetchone()[0]
    try:
        assert await notifier.send_research(s) == "sent"
        assert len(posts) == 1
        assert s.evidence["delivery_confirmation"]["flow_confirmation_mode"] == "LOCAL_STAGED_REVERSAL"
        assert s.evidence["reversal_sequence"] == frozen
        assert (
            store.db.execute(
                "SELECT payload FROM ml_snapshots WHERE signal_id=? AND stage='decision'", (s.id,)
            ).fetchone()[0]
            == decision
        )
        assert notifier.was_initially_delivered(s.id, "research")
        book.event_ofi_epoch += 1
        assert runtime.dispatch_guard(s) == "blocked:REVERSAL_SEQUENCE_STALE"
        book.event_ofi_epoch -= 1
        scanner.streams.tapes[s.symbol].coverage_start = NOW
        assert runtime.dispatch_guard(s) == "blocked:REVERSAL_SEQUENCE_STALE"
        scanner.streams.tapes[s.symbol].coverage_start = 1000
        monkeypatch.setattr("bybit_flow.setup_runtime.now_ms", lambda: NOW + 15001)
        assert runtime.dispatch_guard(s) == "blocked:REVERSAL_SEQUENCE_STALE"
    finally:
        store.close()
