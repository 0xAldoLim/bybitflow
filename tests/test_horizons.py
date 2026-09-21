import json
from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.evidence import factor_context, select_deep
from bybit_flow.horizons import PROFILES, assign, same_thesis, session_context
from bybit_flow.ml.features import snapshot
from bybit_flow.models import Candle, Signal
from bybit_flow.observations import advance, metrics, recommend, start
from bybit_flow.orderflow import Book
from bybit_flow.packing import compact, raw_bytes
from bybit_flow.replay import segment_rows
from bybit_flow.retention import prune_recordings
from bybit_flow.storage import Recorder, Store
from bybit_flow.synthetic import test_signal as send_test_signal


def test_session_baseline_uses_prior_observations_only(settings, signal):
    from bybit_flow.evidence import session_baseline

    store = Store(settings.data_dir)
    signal.entry_session = "ASIA"
    for minute in range(20):
        result = session_baseline(store, signal, {}, {"spread_bps": 2}, minute * 60_000)
        assert result["spread_relative"] is None
    result = session_baseline(store, signal, {}, {"spread_bps": 10}, 20 * 60_000)
    assert result["spread_relative"] == 5
    signal.entry_session = "NEW_YORK"
    assert session_baseline(store, signal, {}, {"spread_bps": 10}, 21 * 60_000)["spread_relative"] is None
    store.close()


def test_flow_persistence_uses_contiguous_time_blocks():
    from decimal import Decimal

    from bybit_flow.evidence import flow_response
    from bybit_flow.models import Trade

    trades = [
        Trade("BTCUSDT", i * 1000, i * 1000, str(i), "Buy" if i < 6 else "Sell", Decimal(100), Decimal(1))
        for i in range(8)
    ]
    result = flow_response(trades, {})
    assert result["delta_persistence"] == 0.75
    assert result["cvd_acceleration"] is None


def test_derivatives_missing_is_not_directional_evidence():
    from bybit_flow.evidence import derivatives_context

    bars = [Candle(i, 1, 100, 102, 99, 100 + i, 1, 100) for i in range(2)]
    assert derivatives_context({}, bars)["price_up_oi_up"] is None
    observed = derivatives_context({"oi_change_pct": -2, "funding_rate": 0.0001}, bars)
    assert observed["price_up_oi_down"] is True
    assert observed["deleveraging_score"] == 0.2


def test_interrupted_migration_resumes_without_overwriting_origins(settings, signal):
    store = Store(settings.data_dir)
    store.signal(signal)
    original = store.db.execute(
        "SELECT payload FROM signal_origins WHERE signal_id=?", (signal.id,)
    ).fetchone()[0]
    store.put("horizons_migration_complete", False)
    store.close()
    store = Store(settings.data_dir)
    assert store.get("horizons_migration_complete") is True
    assert (
        store.db.execute("SELECT payload FROM signal_origins WHERE signal_id=?", (signal.id,)).fetchone()[0]
        == original
    )
    store.close()


def test_horizon_counts_do_not_double_count_lifecycle_updates(settings, signal):
    store = Store(settings.data_dir)
    store.signal(signal)
    signal.state = "EXPIRED"
    store.signal(signal)
    assert store.get("horizon_counts") == {"LEGACY": 1}
    store.close()
    store = Store(settings.data_dir)
    assert store.get("horizon_counts") == {"LEGACY": 1}
    store.close()


@pytest.mark.parametrize(
    "name,frames,maximum",
    [
        ("SHORT_INTRADAY", ("60", "15", "5"), 120),
        ("CORE_INTRADAY", ("240", "60", "15"), 240),
        ("SWING", ("D", "240", "60"), 2880),
        ("EXTENDED_SWING", ("D", "240", "60"), 10080),
    ],
)
def test_horizon_contract_and_round_trip(signal, name, frames, maximum):
    signal.evidence["trigger_bar_end"] = 0
    assign(signal, name)
    assert (signal.context_timeframe, signal.setup_timeframe, signal.execution_timeframe) == frames
    assert signal.holding_deadline_ms == signal.created_ms + maximum * 60_000
    assert Signal.model_validate_json(signal.model_dump_json()) == signal
    assert PROFILES[name].research_only == (name == "EXTENDED_SWING")


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_new_horizon_second_target_follows_first(signal, direction):
    signal.direction = direction
    signal.tp1 = 140 if direction == "LONG" else 60
    signal.tp2 = 120 if direction == "LONG" else 80
    signal.evidence["trigger_bar_end"] = 0
    assign(signal, "CORE_INTRADAY")
    assert (signal.tp2 - signal.tp1) * (1 if direction == "LONG" else -1) > 0


@pytest.mark.parametrize(
    "date,expected",
    [
        ("2026-01-15T13:30:00", "EU_NY_OVERLAP"),
        ("2026-07-15T12:30:00", "EU_NY_OVERLAP"),
        ("2026-01-15T12:30:00", "EUROPE"),
        ("2026-07-15T07:30:00", "ASIA_EU_OVERLAP"),
        ("2026-01-15T07:30:00", "ASIA"),
        ("2026-01-15T23:30:00", "OFF_SESSION"),
    ],
)
def test_iana_sessions_dst_overlap(date, expected):
    now = int(datetime.fromisoformat(date).replace(tzinfo=UTC).timestamp() * 1000)
    assert session_context(now)["primary"] == expected


def test_thesis_material_difference_not_deduplicated(signal):
    other = signal.model_copy(deep=True)
    other.horizon_profile = "SWING"
    assert same_thesis(signal, other)
    other.stop = 90
    assert not same_thesis(signal, other)


@pytest.mark.asyncio
async def test_materially_different_horizon_can_alert_during_symbol_cooldown(settings, signal):
    from bybit_flow.notifications import Notifier

    settings.research_alerts = True
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/test/example")
    store = Store(settings.data_dir)
    notifier = Notifier(
        settings,
        store,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"id": "test"})),
    )
    signal.horizon_profile = "CORE_INTRADAY"
    store.signal(signal)
    assert await notifier.send_research(signal) == "sent"
    swing = signal.model_copy(deep=True)
    swing.id = "different-swing"
    swing.horizon_profile = "SWING"
    swing.stop -= 20
    swing.tp1 += 30
    store.signal(swing)
    assert await notifier.send_research(swing) == "sent"
    duplicate = swing.model_copy(update={"id": "duplicate-swing"})
    store.signal(duplicate)
    assert await notifier.send_research(duplicate) == "duplicate-plan-suppressed"
    assert store.db.execute("SELECT count(*) FROM outbox WHERE status='sent'").fetchone()[0] == 2
    store.close()


def test_old_payload_survives_restart_without_reclassification(settings, signal):
    store = Store(settings.data_dir)
    signal.state = "ALERTED"
    signal.quality = 92
    signal.raw_tier = "SS"
    store.signal(signal)
    original = store.db.execute(
        "SELECT payload FROM signal_origins WHERE signal_id=?", (signal.id,)
    ).fetchone()[0]
    store.close()
    store = Store(settings.data_dir)
    loaded = Signal.model_validate(store.active_signals()[0])
    assert loaded.horizon_profile == "LEGACY" and loaded.quality == 92 and loaded.raw_tier == "SS"
    loaded.state = "RESOLVED"
    store.signal(loaded, "normal legacy resolution")
    assert (
        store.db.execute("SELECT payload FROM signal_origins WHERE signal_id=?", (signal.id,)).fetchone()[0]
        == original
    )
    assert store.db.execute("SELECT count(*) FROM signals").fetchone()[0] == 1
    assert store.db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    store.close()


@pytest.mark.parametrize("stop_first", [False, True])
def test_expiry_afterlife_respects_path_and_primary_history(settings, signal, stop_first):
    store = Store(settings.data_dir)
    signal.created_ms = 0
    signal.evidence = {"trigger_bar_end": 0, "score_components": {"regime": {"earned": 10}}}
    assign(signal, "CORE_INTRADAY")
    signal.state = "EXPIRED"
    signal.invalidation = "Primary horizon expired"
    store.signal(signal)
    terminal = 14_400_000
    start(store, signal, terminal, checkpoints=[242])
    bars = [
        Candle(terminal, 60_000, 100, 101 if stop_first else 116, 94 if stop_first else 99, 100, 10, 1000),
        Candle(terminal + 60_000, 60_000, 100, 116, 99, 115, 10, 1000),
    ]
    p = advance(store, signal.id, bars, terminal + 120_000)
    assert p["primary_outcome"] == "EXPIRED" and p["operational_status"] == "EXPIRED"
    assert p["late_target_hit"] and p["research_observation_status"] == "COMPLETE"
    assert p["extended_same_rules_outcome"] == ("STOP" if stop_first else "TARGET")
    assert store.signals()[0]["state"] == "EXPIRED"
    assert metrics(store)[0]["primary_win_rate"] == 0
    assert store.db.execute("SELECT count(*) FROM research_labels").fetchone()[0] == 1
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == 0
    assert recommend(store, signal, terminal + 120_001)["status"] == "insufficient evidence"
    store.close()


def test_observation_gap_never_creates_extended_winner(settings, signal):
    store = Store(settings.data_dir)
    signal.created_ms = 0
    signal.evidence = {"trigger_bar_end": 0, "score_components": {"regime": {"earned": 10}}}
    assign(signal, "CORE_INTRADAY")
    signal.state = "EXPIRED"
    start(store, signal, 14_400_000, checkpoints=[243])
    p = advance(store, signal.id, [Candle(14_520_000, 60_000, 100, 116, 99, 115, 10, 1000)], 14_580_000)
    assert not p["coverage_complete"]
    assert p["extended_same_rules_outcome"] == "UNCLEAR"
    assert p["timing_classification"] == "UNCLEAR"
    store.close()


def test_lossless_packing_preserves_replay_and_hashes(settings):
    store = Store(settings.data_dir)
    rec = Recorder(store, settings)
    for at in (1000, 2000, 3000):
        rec.flush(
            [
                dict(
                    source="test",
                    symbol="T",
                    event_ms=at,
                    receipt_ms=at,
                    schema_version=1,
                    complete=True,
                    payload="{}",
                )
            ]
        )
    manifests = store.rows("segments")
    original = {m["raw"]: raw_bytes(m["raw"]) for m in manifests}
    before = list(segment_rows(list(original)))
    result = compact(store)
    assert result["segments"] == 3
    assert all(raw_bytes(path) == data for path, data in original.items())
    assert list(segment_rows(list(original))) == before
    assert len(store.rows("segments")) == 3
    store.close()


def test_active_and_unresolved_evidence_blocks_cleanup(settings, signal):
    cfg = settings.model_copy(update={"recording_retention_enabled": True, "max_storage_gb": 1e-6})
    store = Store(settings.data_dir)
    Recorder(store, settings).flush(
        [
            dict(
                source="test",
                symbol="T",
                event_ms=1000,
                receipt_ms=1000,
                schema_version=1,
                complete=True,
                payload="{}",
            )
        ]
    )
    store.signal(signal)
    store.put("ml_monitor", {"status": "observed", "at_ms": 30_000_000})
    result = prune_recordings(store, cfg, 30_000_000, dry_run=True)
    assert result["bytes_freed"] == 0
    assert result["status"] == "dry-run"
    store.close()


def test_exploration_stays_eligible_and_deterministic():
    rows = [dict(symbol=str(i), eligible=i != 19, rank_score=100 - i) for i in range(20)]
    selected, metadata = select_deep(rows, ["BTC"], [], 10, 42)
    assert len(selected) == 10 and "19" not in selected
    exploratory = [s for s in selected if metadata[s]["selection_reason"] == "EXPLORE"]
    assert len(exploratory) == 1 and int(exploratory[0]) >= 8
    assert 0 < metadata[exploratory[0]]["selection_probability"] < 1
    assert select_deep(rows, ["BTC"], [], 10, 42)[0] == selected


def test_beta_uses_aligned_returns_and_residual():
    import math

    prices = [100 * math.exp(0.001 * i + 0.005 * math.sin(i)) for i in range(70)]

    def bars(power):
        return [
            Candle(i * 60_000, 60_000, p**power, p**power, p**power, p**power, 1, 1)
            for i, p in enumerate(prices)
        ]

    result = factor_context(bars(2), bars(1), bars(1))
    assert result["beta_to_btc"] == pytest.approx(2)
    assert result["residual_return"] == pytest.approx(0, abs=1e-12)


def test_single_frame_book_is_not_persistent_evidence():
    b = Book()
    b.apply(
        {
            "type": "snapshot",
            "ts": 1000,
            "data": {"u": 1, "seq": 1, "b": [["99.99", "10"]], "a": [["100.01", "1"]]},
        },
        1000,
    )
    f = b.features(1000)
    assert f["obi_touch"] == pytest.approx(9 / 11)
    assert f["obi_samples"] == 1
    assert b.features(62_000)["obi_persistence"] == 0


def test_future_afterlife_never_enters_decision_features(signal):
    signal.evidence["post_terminal_mfe"] = 1000
    signal.evidence["late_target_hit"] = True
    signal.evidence["flow"] = {"cvd_slope": 123, "receipt_ms": 3000}
    row = snapshot(signal, 2000, "decision")
    assert row["values"]["flow_cvd_slope"] is None
    assert not any("late" in k or "terminal" in k for k in row["values"])


@pytest.mark.asyncio
async def test_synthetic_discord_delivery_does_not_contaminate_research(settings):
    store = Store(settings.data_dir)
    settings.research_webhook = SecretStr("https://discord.com/api/webhooks/mock/test")
    received = []

    def handle(request):
        received.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "test-message"})

    result = await send_test_signal(settings, store, httpx.MockTransport(handle))
    assert result["status"] == "sent" and result["risk_passed"]
    assert "NOT A REAL TRADE" in received[0]["embeds"][0]["title"]
    for table in ("signals", "ml_snapshots", "ml_labels", "research_labels"):
        assert store.db.execute("SELECT count(*) FROM " + table).fetchone()[0] == 0
    store.close()


def test_horizon_recommendation_is_cached_causal_and_never_queries_large_labels(settings, signal):
    from bybit_flow.observations import recommend
    from bybit_flow.storage import Store

    store = Store(settings.data_dir)
    store.put(
        "horizon_recommendations",
        dict(
            available_ms=1000,
            families={signal.family: dict(samples=120, successful_horizon_counts={"SWING": 70})},
        ),
    )
    statements = []
    store.db.set_trace_callback(statements.append)
    assert recommend(store, signal, 1001)["recommended_horizon_profile"] == "SWING"
    assert recommend(store, signal, 999)["recommended_horizon_profile"] is None
    assert not any("research_labels" in q for q in statements)
    store.close()
