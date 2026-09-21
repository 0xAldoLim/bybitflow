import json
from decimal import Decimal

from bybit_flow.evidence import factor_context, session_baseline
from bybit_flow.levels import targets
from bybit_flow.models import Candle, Trade
from bybit_flow.orderflow import Tape
from bybit_flow.path_research import advance, register
from bybit_flow.profiles import build
from bybit_flow.storage import Store


def test_profile_filters_future_receipts_and_requires_complete_tape(bars):
    tape = Tape()
    tape.reset(0)
    for i in range(60):
        tape.add(Trade("TESTUSDT", i * 1000, i * 1000, str(i), "Buy", Decimal(113), Decimal(1)))
    profile = build(tape, Decimal(".01"), 1, 0, 60000, 60000, "bybit", "TESTUSDT")
    assert profile["available"] and profile["poc"] == 113
    levels = targets(100, 95, "LONG", "trend_pullback", bars, Decimal(".01"), bars[-1].end, profile)
    assert levels["tp1"] == 113
    assert "volume" in levels["target_method"]
    profile["available_ms"] = bars[-1].end + 1
    assert (
        "unavailable"
        in targets(100, 95, "LONG", "trend_pullback", bars, Decimal(".01"), bars[-1].end, profile)[
            "profile_evidence"
        ]
    )
    tape.coverage_start = 1
    assert not build(tape, Decimal(".01"), 1, 0, 60000, 60000, "bybit", "TESTUSDT")["available"]


def test_percentiles_are_prior_only_and_horizon_isolated(settings, signal):
    store = Store(settings.data_dir)
    for i in range(20):
        session_baseline(store, signal, {"trades": 10}, {}, i * 60000)
    result = session_baseline(store, signal, {"trades": 100}, {}, 1200000)
    assert result["trade_count_percentile"] == 1
    signal.horizon_profile = "SWING"
    assert session_baseline(store, signal, {"trades": 100}, {}, 1260000)["trade_count_percentile"] is None
    store.close()


def test_factor_provenance_excludes_future_bars(bars):
    asof = bars[-4].end
    result = factor_context(bars, bars, [], asof=asof)
    assert result["available_ms_btc"] == asof
    assert result["fit_end_ms_btc"] < asof
    assert abs(result["residual_return"]) < 1e-12


def test_swing_grid_frozen_and_ambiguous_path_not_complete(settings, signal):
    store = Store(settings.data_dir)
    signal.horizon_profile = "SWING"
    signal.trigger_expires_ms = 180000
    signal.holding_deadline_ms = 180000
    signal.evidence.update(
        score_components={"x": 1}, setup_features={"atr": 4}, execution={"short_horizon_noise": 6}
    )
    signal.risk = {"cost_per_base": 0.1}
    with store.db:
        register(store, signal, 60000)
    raw = store.db.execute("SELECT payload FROM swing_path_jobs").fetchone()[0]
    signal.stop = 90
    with store.db:
        register(store, signal, 60000)
    assert store.db.execute("SELECT payload FROM swing_path_jobs").fetchone()[0] == raw
    with store.db:
        store.db.execute("UPDATE swing_path_jobs SET deadline_ms=180000")
    advance(
        store,
        signal.id,
        [
            Candle(60000, 60000, 100, 116, 94, 101, 10, 1000),
            Candle(120000, 60000, 101, 117, 100, 116, 10, 1000),
        ],
        180000,
    )
    result = json.loads(store.db.execute("SELECT payload FROM swing_path_labels").fetchone()[0])
    assert not result["complete"]
    assert result["policies"]["incumbent"]["outcome"] == "STOP"
    assert result["stop"] == 95
    store.close()


def test_schema_upgrade_does_not_recapture_old_decision(settings, signal):
    from bybit_flow.ml.store import FeatureStore

    store = Store(settings.data_dir)
    original = FeatureStore(store).capture(signal, 1000, "decision")
    with store.db:
        store.db.execute("UPDATE ml_snapshots SET schema_version='prior-schema' WHERE id=?", (original,))
    signal.entry = 200
    assert FeatureStore(store).capture(signal, 2000, "decision") == original
    assert store.db.execute("SELECT count(*) FROM ml_snapshots").fetchone()[0] == 1
    store.close()


def test_operator_ml_summary_does_not_scan_history(settings):
    from bybit_flow.ml.registry import Registry

    store = Store(settings.data_dir)
    store.put("ml_summary_cache", dict(models=[], snapshots=100, complete_labels=25, summary_at_ms=1234))
    statements = []
    store.db.set_trace_callback(statements.append)
    report = Registry(store).cached_summary()
    assert report["snapshots"] == 100 and report["summary_at_ms"] == 1234
    assert not any("FROM ml_labels" in s or "FROM ml_snapshots" in s for s in statements)
    store.close()


def test_same_execution_window_cannot_enter_its_own_baseline(settings, signal):
    store = Store(settings.data_dir)
    for i in range(19):
        session_baseline(store, signal, {"trades": 10}, {}, i * 60000, window_end_ms=i * 60000)
    session_baseline(store, signal, {"trades": 100}, {}, 1200000, window_end_ms=1200000)
    same = session_baseline(store, signal, {"trades": 100}, {}, 1201000, window_end_ms=1200000)
    assert same["trade_count_percentile"] is None
    following = session_baseline(store, signal, {"trades": 200}, {}, 1260000, window_end_ms=1260000)
    assert following["trade_count_percentile"] == 1
    store.close()


def test_future_profile_cannot_define_migration():
    tape = Tape()
    tape.reset(0)
    for i in range(120):
        tape.add(Trade("TESTUSDT", i * 1000, i * 1000, str(i), "Buy", Decimal(100 + i % 3), Decimal(1)))
    prior = build(tape, Decimal(".01"), 1, 0, 60000, 60000, "bybit", "TESTUSDT")
    current = build(tape, Decimal(".01"), 1, 60000, 120000, 120000, "bybit", "TESTUSDT", prior)
    assert current["migration_available"]
    prior["available_ms"] = 120001
    assert not build(tape, Decimal(".01"), 1, 60000, 120000, 120000, "bybit", "TESTUSDT", prior)[
        "migration_available"
    ]


def test_late_recovery_alone_does_not_establish_tight_stop():
    from bybit_flow.path_research import classify_path

    p = dict(
        complete=True,
        policies={"incumbent": {"outcome": "STOP"}, "noise_buffer": {"outcome": "TARGET", "net_rr": 2.5}},
        first_stop_ms=1000,
        reclaim_ms=2000,
        first_tp1_ms=3000,
        primary_deadline=4000,
        max_stop_overshoot=2,
        decision_atr=1,
        higher_timeframe_failure=None,
        post_stop_mfe_r=3,
    )
    assert classify_path(p) == "UNRESOLVED_THESIS_VS_STOP"
    p["max_stop_overshoot"] = 0.1
    assert classify_path(p) == "THESIS_CORRECT_STOP_TOO_TIGHT"
    p["higher_timeframe_failure"] = 1500
    assert classify_path(p) == "UNRESOLVED_THESIS_VS_STOP"
    p["complete"] = False
    assert classify_path(p) == "PATH_AMBIGUOUS"


def test_swing_partial_decision_candle_cannot_fabricate_complete_path(settings, signal):
    store = Store(settings.data_dir)
    signal.horizon_profile = "SWING"
    signal.trigger_expires_ms = 180000
    signal.holding_deadline_ms = 180000
    signal.evidence.update(
        score_components={"x": 1}, setup_features={"atr": 4}, execution={"short_horizon_noise": 6}
    )
    signal.risk = {"cost_per_base": 0.1}
    with store.db:
        register(store, signal, 61000)
        store.db.execute("UPDATE swing_path_jobs SET deadline_ms=180000")
    advance(
        store,
        signal.id,
        [
            Candle(60000, 60000, 100, 101, 99, 100, 10, 1000),
            Candle(120000, 60000, 100, 116, 99, 115, 10, 1000),
        ],
        180000,
    )
    p = json.loads(store.db.execute("SELECT payload FROM swing_path_labels").fetchone()[0])
    assert p["ambiguous"] and not p["complete"]
    store.close()
