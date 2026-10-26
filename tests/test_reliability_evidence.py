"""Causal enrollment, immutable results, pressure-only admission and public projections."""

import json
from copy import deepcopy

import pytest
from test_setup_v22 import NOW, tagged
from test_v22_research import candle

from bybit_flow import forward_evidence as forward
from bybit_flow.config import Settings
from bybit_flow.decision_view import overview, rows
from bybit_flow.operational import HOUR, Priority, allowed, classify, forecast, persist, warnings
from bybit_flow.provenance import config_snapshot, status
from bybit_flow.quality_v22 import apply
from bybit_flow.recovery_status import failure_code, project, state
from bybit_flow.setup_research import advance
from bybit_flow.storage import Store


def enrolled(signal, settings, instrument):
    s, b = tagged(signal, settings, instrument)
    s.created_ms = NOW
    s.holding_deadline_ms = NOW + 300000
    s.expected_hold_min = 2
    s.trigger_expires_ms = NOW + 120000
    s.evidence["structural_tp1"] = 108
    apply(s, 100.01, NOW, book=b, settings=settings)
    store = Store(settings.data_dir)
    forward.initialize(store, NOW)
    store.signal(s)
    assert store.db.execute("SELECT count(*) FROM policy_opportunities").fetchone()[0] == 1
    return s, store


def test_prospective_enrollment_is_immutable_and_never_changes_plan(signal, settings, instrument):
    s, store = enrolled(signal, settings, instrument)
    saved = store.db.execute("SELECT payload FROM policy_opportunities").fetchone()[0]
    original = s.model_dump()
    control = forward.variants(s, NOW)
    assert s.model_dump() == original
    assert set(control) == set(forward.POLICIES)
    assert all(r["production_authority"] is False for r in control.values())
    s.quality = 5
    with store.db:
        forward.register(store, s, NOW + 1)
    assert store.db.execute("SELECT payload FROM policy_opportunities").fetchone()[0] == saved
    store.close()


def test_epoch_excludes_historical_and_future_decisions(signal, settings, instrument):
    s, b = tagged(signal, settings, instrument)
    apply(s, 100.01, NOW, book=b, settings=settings)
    store = Store(settings.data_dir)
    forward.initialize(store, NOW)
    store.signal(s)  # created before epoch
    assert store.db.execute("SELECT count(*) FROM policy_opportunities").fetchone()[0] == 0
    s.created_ms = NOW
    with store.db:
        forward.register(store, s, NOW - 1)
    assert store.db.execute("SELECT count(*) FROM policy_opportunities").fetchone()[0] == 0
    store.close()


def test_forward_outcomes_restart_idempotent_and_no_label_overwrite(signal, settings, instrument):
    s, store = enrolled(signal, settings, instrument)
    bars = [
        candle(NOW),
        candle(NOW + 60000, high=101.7, close=101.5),
        candle(NOW + 120000, opening=101.5, high=102.5, low=101.2, close=102),
        candle(NOW + 180000, opening=102, high=109, low=101.5, close=108),
    ]
    advance(store, s.id + ":forward", bars[:1], NOW + 60000)
    cursor = store.db.execute(
        "SELECT cursor_ms FROM setup_path_jobs WHERE signal_id=?", (s.id + ":forward",)
    ).fetchone()[0]
    store.close()
    store = Store(settings.data_dir)
    assert (
        store.db.execute(
            "SELECT cursor_ms FROM setup_path_jobs WHERE signal_id=?", (s.id + ":forward",)
        ).fetchone()[0]
        == cursor
    )
    advance(store, s.id + ":forward", bars[1:], NOW + 240000)
    assert forward.ingest_paths(store, NOW + 240000) == 1
    result = [tuple(r) for r in store.db.execute("SELECT * FROM policy_results")]
    assert len(result) == 7 and forward.ingest_paths(store, NOW + 240001) == 0
    assert [tuple(r) for r in store.db.execute("SELECT * FROM policy_results")] == result
    report = forward.report(store, NOW + 240001)
    assert report["opportunities"] == 1
    assert all(v["accepted"]["status"] == "INSUFFICIENT SAMPLE" for v in report["policies"].values())
    assert all("research_only:bybit" in k for k in report["policies"])
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == 0
    store.close()


def test_research_failure_cannot_prevent_deterministic_persistence(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    monkeypatch.setattr(forward, "register", lambda *a: (_ for _ in ()).throw(ValueError("shadow failure")))
    store.signal(signal)
    assert store.signals()[0]["id"] == signal.id
    assert store.db.health.snapshot()["forward_enrollment_errors"] == 1
    store.close()


@pytest.mark.parametrize(
    "metrics,expected",
    [
        ({}, "NORMAL"),
        ({"storage_fraction": 0.86}, "TRAINING_DEFERRED"),
        ({"storage_fraction": 0.92}, "RECOVERY_DEFERRED"),
        ({"event_loop_lag_ms": 300}, "RESEARCH_DEFERRED"),
        ({"recorder_pressure": 0.8}, "CRITICAL_COLLECTION_PROTECTION"),
    ],
)
def test_pressure_only_defers_background(metrics, expected, settings):
    measured = classify(metrics)
    assert measured.state == expected
    store = Store(settings.data_dir)
    store.put("operational_health", dict(at_ms=1000, defer_from=measured.defer_from))
    assert all(allowed(store, p, 1001) for p in list(Priority)[:5])
    assert allowed(store, Priority.TRAINING_BOOTSTRAP, 1001) == (measured.defer_from > 8)
    assert not allowed(store, Priority.TRAINING_BOOTSTRAP, 100000)  # stale telemetry grants no new authority
    store.close()


def test_storage_forecast_needs_real_history_and_keeps_categories():
    assert forecast([], 100)["days_to_budget"] == "UNKNOWN"
    first = dict(total=10, database=3, wal=1, raw_segments=6)
    last = dict(total=34, database=15, wal=1, raw_segments=18)
    result = forecast([(0, first), (24 * HOUR, last)], 100)
    assert result["windows"]["24h"]["database"] == 12
    assert result["windows"]["7d"] == "UNKNOWN" and result["days_to_budget"] == 2.75
    assert forecast([(0, last), (24 * HOUR, first)], 100)["days_to_budget"] == "UNKNOWN"


def test_bounded_telemetry_and_passive_checkpoint(settings):
    store = Store(settings.data_dir)
    old = 100 * HOUR
    persist(store.root, dict(at_ms=old), 1e9, True, True)
    persist(store.root, dict(at_ms=old + 49 * HOUR), 1e9)
    assert store.db.execute("SELECT count(*) FROM operational_samples").fetchone()[0] == 1
    growth = store.get("storage_forecast")
    assert growth["days_to_budget"] == "UNKNOWN" and "database" in growth["categories"]
    store.put(
        "operational_health",
        dict(
            at_ms=1000,
            event_loop_lag_ms=300,
            sqlite_busy_delta=1,
            sqlite_wal_bytes=600 * 1024 * 1024,
            recorder_pressure=0.8,
        ),
    )
    codes = {r["code"] for r in warnings(store, 1001)}
    assert codes == {"EVENT_LOOP_LAG", "SQLITE_CONTENTION", "WAL_GROWTH", "RECORDER_PRESSURE"}
    assert all("expected" in r and r["action"] for r in warnings(store, 1001))
    store.close()


def test_config_digest_is_redacted_stable_and_version_has_schema(settings):
    first = config_snapshot(settings)
    other = Settings(
        _env_file=None,
        data_dir=settings.data_dir / "private",
        research_webhook="https://secret.invalid/token",
        admin_token="TOPSECRET",
        dashboard_url="https://private.invalid",
    )
    # rest_requests_per_second differs from the test fixture; restore it.
    other.rest_requests_per_second = settings.rest_requests_per_second
    assert config_snapshot(other)["config_digest"] == first["config_digest"]
    assert not any(v in json.dumps(first) for v in ("TOPSECRET", "secret.invalid", "data_dir", "equity"))
    store = Store(settings.data_dir)
    result = status(store, settings)
    assert result["version"] == "2.2.1" and result["database"]["level"] == "009"
    store.close()


@pytest.mark.parametrize(
    "reason,code",
    [
        ("ReadTimeout", "NETWORK_TIMEOUT"),
        ("429", "RATE_LIMIT"),
        ("data gap", "DATA_GAP"),
        ("clock future", "CLOCK_DAMAGE"),
        ("pruned", "RETENTION_MISSING"),
        ("ambiguous ordering", "AMBIGUOUS_ORDERING"),
        ("invalid frozen plan", "INVALID_FROZEN_PLAN"),
    ],
)
def test_recovery_failure_categories(reason, code):
    assert failure_code(reason) == code
    assert state("COMPLETE", 9999, 1000) == "COMPLETE"
    assert state("PENDING", 9999, 1000) == "RETRY_AT"


def test_dashboard_projection_has_all_valid_setups_no_delivery_cap(settings, signal):
    store = Store(settings.data_dir)
    for i in range(35):
        s = deepcopy(signal)
        s.id = str(i)
        s.state = "CONFIRMED"
        s.quality = 50 + i
        store.signal(s)
    result = overview(rows(store), 1001, {("bybit", signal.symbol): dict(fresh=True, price=100)})
    assert len(result["opportunities"]) == 35
    assert [r["rank"] for r in result["opportunities"]] == list(range(1, 36))
    assert all(r["current_price"] == 100 and r["progress_r"] == 0 for r in result["active"])
    assert overview(rows(store), 1001)["opportunities"] == []
    assert project(store, 1001)["jobs"] == []
    store.close()


@pytest.mark.parametrize("parts", [1, 2, 3, 5])
def test_path_prefix_restart_equivalence_monotonic_cursor(settings, signal, instrument, parts):
    s, store = enrolled(signal, settings, instrument)
    ident = s.id + ":forward"
    bars = [candle(NOW + i * 60000, high=100.4 + i * 0.4, low=99.9) for i in range(5)]
    cursors = []
    for chunk in range(0, len(bars), parts):
        end = min(len(bars), chunk + parts)
        advance(store, ident, bars[chunk:end], NOW + end * 60000)
        row = store.db.execute("SELECT cursor_ms FROM setup_path_jobs WHERE signal_id=?", (ident,)).fetchone()
        if row:
            cursors.append(row[0])
        store.close()
        store = Store(settings.data_dir)
    assert cursors == sorted(cursors)
    label = store.db.execute("SELECT payload FROM setup_path_labels WHERE signal_id=?", (ident,)).fetchone()
    assert label is not None
    before = label[0]
    advance(store, ident, bars, NOW + 600000)
    assert (
        store.db.execute("SELECT payload FROM setup_path_labels WHERE signal_id=?", (ident,)).fetchone()[0]
        == before
    )
    assert forward.ingest_paths(store, NOW + 299999) == 0  # label not available yet
    assert forward.ingest_paths(store, NOW + 300000) == 1
    store.close()


def test_missing_milestones_are_unknown_not_zero_and_cohorts_do_not_pool():
    summary = forward.aggregate([dict(net_r=1, complete=True) for _ in range(31)])
    assert summary["positive_rate"] == 1
    assert summary["reach_05_rate"] is None and summary["mean_mfe"] is None
    pair = forward.paired_report(
        {
            ("a", "research_only", "bybit"): {
                "v22_actual": dict(net_r=1, complete=True, comparison_status="OBSERVED", accepted=True),
                "without_ofi": dict(net_r=1, complete=True, comparison_status="OBSERVED", accepted=False),
            }
        }
    )
    assert pair["without_ofi:research_only:bybit"]["winner_rejection_count"] == 1
    assert pair["without_ofi:research_only:bybit"]["status"] == "INSUFFICIENT SAMPLE"


def test_manual_review_cannot_promote_policy(settings):
    store = Store(settings.data_dir)
    with pytest.raises(ValueError):
        forward.record_promotion(store, dict(policy_id="auto", automatic=True), 100)
    record = dict(
        policy_id="shadow",
        parent_policy="v22_actual",
        development_period=[1, 2],
        forward_start=3,
        evaluation_end=4,
        sample_counts={"bybit": 30},
        net_r=0.1,
        mfe=0.7,
        mae=-0.5,
        loss_avoidance=2,
        winner_rejection=1,
        subgroup_stability="insufficient",
        attempted_variants=["shadow"],
        code_commit="a" * 40,
    )
    ident = forward.record_promotion(store, record, 100)
    assert forward.record_promotion(store, record, 101) == ident
    assert store.db.execute("SELECT count(*) FROM policy_promotions").fetchone()[0] == 1
    assert store.get("production_policy") is None
    store.close()


def test_bounded_views_do_not_include_private_payloads(settings, signal):
    import time

    from bybit_flow.ml.operations import trainability

    store = Store(settings.data_dir)
    start = time.monotonic()
    for i in range(100):
        s = signal.model_copy(deep=True)
        s.id = str(i)
        s.evidence["private_payload"] = "PRIVATE_DO_NOT_EXPORT"
        store.signal(s)
    public = overview(rows(store), 1001)
    assert len(public["active"]) == 100 and "PRIVATE_DO_NOT_EXPORT" not in json.dumps(public)
    assert forward.report(store, 1001)["limit"] == 5000
    assert trainability(store)["by_source"]
    # Broad envelope catches accidental full-history/unbounded processing, not
    # small timing differences between Windows, Linux and CI hosts.
    assert time.monotonic() - start < 30
    store.close()


def test_storage_categories_match_actual_nested_ml_directories(settings):
    from bybit_flow.operational import category_bytes

    for name, size in (
        ("ml/models/m/model.bin", 7),
        ("ml/datasets/export.parquet", 11),
        ("ml/maintenance/audit.json", 13),
        ("ml/horizon/report.json", 17),
        ("deployment-audits/check.json", 19),
    ):
        file = settings.data_dir / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(b"x" * size)
    measured = category_bytes(settings.data_dir)
    assert measured["models"] == 7 and measured["datasets"] == 11
    assert measured["maintenance_audit"] == 32 and measured["research_exports"] == 17
    assert measured["total"] == 67


def test_config_digest_changes_when_operational_gate_changes(settings):
    original = config_snapshot(settings)["config_digest"]
    changed = settings.model_copy(update={"max_spread_bps": settings.max_spread_bps + 1})
    assert config_snapshot(changed)["config_digest"] != original


async def test_collector_pressure_defers_storage_walk_and_checkpoint(settings, monkeypatch):
    import asyncio
    from types import SimpleNamespace

    from bybit_flow import operational

    store = Store(settings.data_dir)
    calls = []
    ticks = 0

    async def step(delay):
        nonlocal ticks
        ticks += 1
        if ticks > 1:
            raise asyncio.CancelledError

    async def inline(function, *args):
        return function(*args)

    def persist_sample(*args):
        calls.append(args)

    monkeypatch.setattr(operational.asyncio, "sleep", step)
    monkeypatch.setattr(operational.asyncio, "to_thread", inline)
    monkeypatch.setattr(operational, "now_ms", lambda: 180000)
    monkeypatch.setattr(operational, "persist", persist_sample)
    runtime = SimpleNamespace(
        store=store,
        settings=settings,
        rest_queue_depth=0,
        rest_inflight=0,
        streams=SimpleNamespace(reconnects={}),
        recorder=SimpleNamespace(metrics=lambda: {}, pressure=lambda: 0.8, healthy=True, pending_bytes=0),
    )
    with pytest.raises(asyncio.CancelledError):
        await operational.monitor(runtime)
    assert len(calls) == 1
    assert calls[0][3] is False and calls[0][4] is False  # no directory scan or checkpoint
    assert store.get("operational_health")["state"] == "CRITICAL_COLLECTION_PROTECTION"
    assert allowed(store, Priority.RECORDING, 180000) and allowed(
        store, Priority.CONFIRMATION_DELIVERY, 180000
    )
    store.close()
