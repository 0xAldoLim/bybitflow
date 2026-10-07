import json
from pathlib import Path

import pytest

from bybit_flow.ml import cli
from bybit_flow.ml.checkpoint import consumed_cursor, seal
from bybit_flow.ml.completeness import aggregate, audit_completeness, cache_readiness, cache_report
from bybit_flow.ml.labels import label_recordings
from bybit_flow.ml.operations import status
from bybit_flow.ml.recordings import worker_rows
from bybit_flow.ml.store import FeatureStore
from bybit_flow.storage import Recorder, Store


def event(at, topic="control/subscribed", payload=None, source="bybit", symbol="TESTUSDT", complete=True):
    return dict(
        source=("native/" + source + "/" if source != "bybit" else "") + topic,
        symbol=symbol,
        event_ms=at,
        receipt_ms=at,
        schema_version=1,
        complete=complete,
        payload=json.dumps(payload or {"symbols": [symbol]}),
    )


def trade(at, price=100, source="bybit", symbol="TESTUSDT"):
    return event(
        at,
        "ws/publicTrade." + symbol,
        {"data": [dict(T=at, p=str(price), v="1000", i=str(at), S="Buy")]},
        source,
        symbol,
    )


def ready_candidate(settings, signal, source="bybit"):
    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    signal = signal.model_copy(update={"source": source})
    recorder.flush([event(500, source=source), trade(700, source=source)])
    ident = FeatureStore(store).capture(signal, 1000, "decision")
    return store, recorder, ident


def test_healthy_complete_and_chained_250ms_rotation(settings, signal):
    store, recorder, ident = ready_candidate(settings, signal)
    recorder.flush([trade(1500)])
    recorder.flush([trade(1750), trade(5000, 116)])
    label_recordings(store, worker_rows(store), settings)
    before = store.db.execute("SELECT payload FROM ml_labels").fetchone()[0]
    report = audit_completeness(store, settings, asof_ms=10000)
    row = report["recent_candidates"][0]
    assert row["candidate_id"] == ident and row["audit_class"] == "COMPLETE"
    assert row["segment_chain_valid"] and not row["gap_count"]
    assert report["summary"]["completion_rate"] == 1
    assert store.db.execute("SELECT payload FROM ml_labels").fetchone()[0] == before
    assert report["continuity_rules"]["trade_stale_ms"] == 15000
    store.close()


@pytest.mark.parametrize("subscription", [True, False])
def test_predecision_not_ready_is_ineligible_not_a_later_failure(settings, signal, subscription):
    store, recorder = Store(settings.data_dir), None
    recorder = Recorder(store, settings)
    if subscription:
        recorder.flush([event(500)])
    ident = FeatureStore(store).capture(signal, 1000, "decision")
    recorder.flush([trade(1500), event(2000, "control/gap", {"reason": "connection lost"}, complete=False)])
    report = audit_completeness(store, settings, asof_ms=20_000_000)
    row = report["recent_candidates"][0]
    assert row["candidate_id"] == ident and row["eligible_at_decision"] is False
    assert row["audit_class"] == (
        "PREDECISION_COVERAGE_MISSING" if subscription else "SUBSCRIPTION_NOT_READY"
    )
    assert report["summary"]["mature_eligible"] == 0
    assert report["summary"]["incomplete_eligible"] == 0
    store.close()


def test_real_trade_gap_not_global_drop_counter(settings, signal):
    store, recorder, _ = ready_candidate(settings, signal)
    recorder.flush([trade(1500), trade(20000, 116)])
    store.put("recorder_health", dict(events_dropped=9000))
    label_recordings(store, worker_rows(store), settings)
    row = audit_completeness(store, settings, asof_ms=30000)["recent_candidates"][0]
    assert row["audit_class"] == "REAL_TRADE_FEED_GAP"
    assert row["largest_gap_ms"] == 18500
    assert row["gap_family"] != "RECORDER_DROP"
    store.close()


def test_false_gap_reconstruction_preserves_immutable_label(settings, signal):
    store, recorder, ident = ready_candidate(settings, signal, "binance")
    recorder.flush(
        [
            trade(1500, source="binance"),
            event(2000, "control/book_gap", {"reason": "TimeoutError"}, source="binance"),
            trade(5000, 116, source="binance"),
        ]
    )
    old = dict(
        policy="prints-v1",
        complete=False,
        classification="incomplete",
        net_r=None,
        data_gaps=["recording continuity lost"],
        reason="synthetic false metadata gap",
    )
    FeatureStore(store).label(ident, old, 5000)
    before = store.db.execute("SELECT payload FROM ml_labels").fetchone()[0]
    result = audit_completeness(store, settings, asof_ms=10000)
    row = result["recent_candidates"][0]
    assert row["audit_class"] == "LABELER_FALSE_GAP" and row["reconstruction_complete"]
    assert result["summary"]["complete"] == 0
    assert store.db.execute("SELECT payload FROM ml_labels").fetchone()[0] == before
    # The real labeler already ignores complete book-only notices; no strategy patch is needed.
    other = Store(settings.data_dir / "other")
    FeatureStore(other).capture(signal.model_copy(update={"source": "binance"}), 1000, "decision")
    assert label_recordings(other, worker_rows(store), settings)["complete"] == 1
    other.close()
    store.close()


def test_retention_loss_requires_unconsumed_unresolved_raw(settings, signal):
    store, recorder, _ = ready_candidate(settings, signal)
    recorder.flush([trade(1500), trade(5000, 116)])
    manifest = next(m for m in store.rows("segments") if m["max_receipt_ms"] == 5000)
    store.put("pruned_segment:" + manifest["id"], dict(at_ms=8000, sha256=manifest["sha256"]))
    Path(manifest["raw"]).unlink()
    row = audit_completeness(store, settings, asof_ms=10000)["recent_candidates"][0]
    assert row["audit_class"] == "RETENTION_LOSS"
    store.close()


def test_safe_post_label_cleanup_is_not_retention_loss(settings, signal):
    store, recorder, _ = ready_candidate(settings, signal)
    recorder.flush([trade(1500), trade(5000, 116)])
    label_recordings(store, worker_rows(store), settings)
    for manifest in store.rows("segments"):
        store.put("pruned_segment:" + manifest["id"], dict(at_ms=8000, sha256=manifest["sha256"]))
        Path(manifest["raw"]).unlink()
    row = audit_completeness(store, settings, asof_ms=10000)["recent_candidates"][0]
    assert row["audit_class"] == "COMPLETE"
    store.close()


def test_advanced_checkpoint_without_frozen_position_fails_closed(settings, signal):
    from bybit_flow.retention import protection_ranges

    store, recorder, ident = ready_candidate(settings, signal)
    recorder.flush([trade(1500)])
    checkpoint = seal(
        dict(
            policy="incremental-prints-v1",
            cursor_ms=1500,
            input_event_hash="a" * 64,
            positions={},
            subscribed=[],
            last=[],
            seen=[],
            pending=0,
            verified_boundary_ms=2000,
        ),
        3000,
    )
    store.put("primary_materialization", checkpoint)
    snapshot = json.loads(
        store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
    )
    assert consumed_cursor(store, ident, snapshot, 10000) is None
    assert any(start <= 1000 <= end for start, end, _ in protection_ranges(store, 10000))
    row = audit_completeness(store, settings, asof_ms=10000)["recent_candidates"][0]
    assert row["audit_class"] == "MATERIALIZER_CHECKPOINT_GAP"
    store.close()


def test_duplicates_are_not_independent_and_open_cohort_excluded(settings, signal):
    store, recorder, ident = ready_candidate(settings, signal)
    recorder.flush([trade(1500)])
    repeated = signal.model_copy(update={"id": "repeated"})
    duplicate = FeatureStore(store).capture(repeated, 1600, "decision")
    with store.db:
        identity = store.db.execute(
            "SELECT candidate_identity FROM candidate_identities WHERE signal_id=?", (signal.id,)
        ).fetchone()[0]
        store.db.execute(
            "UPDATE candidate_identities SET candidate_identity=? WHERE signal_id=?", (identity, repeated.id)
        )
    report = audit_completeness(store, settings, asof_ms=2000)
    assert report["summary"]["total_unique_candidates"] == 1
    assert report["summary"]["duplicate_snapshots"] == 1
    assert report["duplicate_snapshots"][0]["candidate_id"] == duplicate
    assert report["recent_candidates"][0]["candidate_id"] == ident
    assert report["recent_candidates"][0]["audit_class"] == "STILL_OPEN"
    assert report["summary"]["mature_eligible"] == 0 and report["summary"]["completion_rate"] is None
    store.close()


def test_cohort_denominator_and_health_boundaries():
    rows = [
        dict(
            mature=True,
            eligible_at_decision=True,
            label_complete=i < 40,
            audit_class="COMPLETE" if i < 40 else "REAL_TRADE_FEED_GAP",
        )
        for i in range(50)
    ]
    rows += [
        dict(mature=False, eligible_at_decision=True, label_complete=False, audit_class="STILL_OPEN")
        for _ in range(40)
    ]
    rows += [
        dict(
            mature=True,
            eligible_at_decision=False,
            label_complete=False,
            audit_class="SUBSCRIPTION_NOT_READY",
        )
        for _ in range(10)
    ]
    summary = aggregate(rows)
    assert summary["total_unique_candidates"] == 100
    assert summary["mature_eligible"] == 50 and summary["completion_rate"] == 0.8
    assert summary["status"] == "HEALTHY"
    for wins, expected in ((18, "WATCH"), (17, "DEGRADED")):
        for i, r in enumerate(rows[:30]):
            r["label_complete"] = i < wins
        assert aggregate(rows[:30])["status"] == expected
    assert aggregate(rows[:29])["status"] == "INSUFFICIENT_COHORT"


def test_source_horizon_epoch_filters_and_cli_cache(settings, signal, capsys, monkeypatch):
    store, recorder, _ = ready_candidate(settings, signal, "binance")
    recorder.flush([trade(1500, source="binance"), trade(5000, 116, source="binance")])
    second = signal.model_copy(
        update={"id": "okx-plan", "source": "okx", "horizon_profile": "SWING", "expected_hold_max": 2880}
    )
    recorder.flush([event(6000, source="okx"), trade(6500, source="okx")])
    FeatureStore(store).capture(second, 7000, "decision")
    recorder.flush([trade(7500, source="okx")])
    store.put("ml_maturity_epoch", dict(started_ms=6000))
    report = audit_completeness(store, settings, asof_ms=10000)
    assert report["by_source"]["binance"]["total_unique_candidates"] == 1
    assert report["by_source"]["bybit"]["total_unique_candidates"] == 0
    assert report["by_horizon"]["SWING"]["total_unique_candidates"] == 1
    assert report["maturity_eras"]["pre_maturity"]["total_unique_candidates"] == 1
    assert report["maturity_eras"]["post_maturity"]["total_unique_candidates"] == 1
    assert (
        len(
            audit_completeness(store, settings, source="okx", horizon="SWING", asof_ms=10000)[
                "recent_candidates"
            ]
        )
        == 1
    )
    cache_report(store, report)
    cache_readiness(store, 10000)
    monkeypatch.setattr(
        "bybit_flow.ml.completeness.EvidenceReader.read", lambda *_: pytest.fail("status must not decode raw")
    )
    assert (
        status(store, {})["primary_data_quality"]["binance"]["prospective_readiness"][
            "primary_ml_eligible_decisions"
        ]
        == 1
    )
    monkeypatch.undo()
    cli.run(["audit-completeness", "--source", "okx", "--limit", "50", "--json"], settings, store)
    assert json.loads(capsys.readouterr().out)["source"] == "okx"
    cli.run(["audit-completeness", "--limit", "50"], settings, store)
    output = capsys.readouterr().out
    assert "SUMMARY" in output and "RECENT CANDIDATES" in output and "COHORT COMPLETION" in output
    store.close()


def test_clock_damage_and_bounded_audit_never_invent_completion(settings, signal):
    store, recorder, _ = ready_candidate(settings, signal)
    recorder.flush([trade(1500), event(2000), event(1900), trade(5000, 116)])
    row = audit_completeness(store, settings, asof_ms=10000)["recent_candidates"][0]
    assert row["audit_class"] == "CLOCK_OR_ORDERING_DAMAGE" and not row["reconstruction_complete"]
    report = audit_completeness(store, settings, asof_ms=10000, max_rows=1)
    assert report["forensic_budget"]["exhausted"]
    assert not report["recent_candidates"][0]["reconstruction_complete"]
    store.close()


def test_pruned_clock_damage_uses_verified_audit_not_an_invented_trade_gap(settings, signal):
    store, recorder, ident = ready_candidate(settings, signal)
    recorder.flush([trade(1500), event(2000), event(1900), trade(5000, 116)])
    label_recordings(store, worker_rows(store), settings)
    manifest = next(m for m in store.rows("segments") if m["max_receipt_ms"] == 5000)
    store.put("pruned_segment:" + manifest["id"], dict(at_ms=8000, sha256=manifest["sha256"]))
    Path(manifest["raw"]).unlink()
    report = audit_completeness(store, settings, asof_ms=10000)
    assert report["recent_candidates"][0]["audit_class"] == "CLOCK_OR_ORDERING_DAMAGE"
    cache_report(store, report)
    assert store.get("primary_gap_summary:" + ident)["gap_family"] == "CLOCK_OR_ORDERING_DAMAGE"
    store.close()


def test_eligibility_metadata_does_not_change_frozen_features_or_plan(settings, signal):
    from bybit_flow.ml.features import snapshot

    original = signal.model_dump(mode="json")
    store, _, ident = ready_candidate(settings, signal)
    captured = json.loads(
        store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (ident,)).fetchone()[0]
    )
    assert captured["primary_ml_eligibility"]["eligible"]
    assert captured["values"] == snapshot(signal, 1000, "decision", None)["values"]
    assert "primary_ml_eligibility" not in captured["values"]
    assert signal.model_dump(mode="json") == original
    store.close()


def test_new_one_ms_clock_jitter_is_explicit_larger_damage_is_not_hidden(settings):
    store, recorder = Store(settings.data_dir), None
    recorder = Recorder(store, settings)
    for at in (1000, 999, 1001, 900):
        recorder.offer("control/clock", "ALL", at, {}, receipt_ms=at)
    rows = [recorder.queue.get_nowait() for _ in range(4)]
    assert [r["receipt_ms"] for r in rows] == [1000, 1000, 1001, 900]
    assert rows[1]["observed_receipt_ms"] == 999 and rows[1]["receipt_clock_adjustment_ms"] == 1
    assert "observed_receipt_ms" not in rows[-1]
    assert recorder.metrics()["receipt_clock_adjustments"] == 1
    recorder.flush(rows[:3])
    assert [r["receipt_ms"] for r in worker_rows(store)] == [1000, 1000, 1001]
    assert store.get("ml_recordings")["excluded"] == 0
    from bybit_flow.packing import raw_stream

    with raw_stream(store.rows("segments")[0]["raw"]) as stream:
        persisted = [json.loads(line) for line in stream]
    assert persisted[1]["observed_receipt_ms"] == 999
    recorder.flush(rows[2:])
    list(worker_rows(store))
    assert store.get("ml_recordings")["excluded"] == 2
    store.close()


@pytest.mark.parametrize("drop_source", ["binance", "bybit"])
def test_drop_attribution_is_scoped_to_venue_symbol_interval(settings, signal, drop_source):
    store, recorder, _ = ready_candidate(settings, signal)
    recorder.flush(
        [
            trade(1500),
            event(
                2000,
                "control/gap",
                {
                    "reason": "RECORDER_BACKPRESSURE",
                    "source": drop_source,
                    "gap_start_ms": 1600,
                    "gap_end_ms": 2000,
                },
                source=drop_source,
                complete=False,
            ),
            trade(5000, 116),
        ]
    )
    row = audit_completeness(store, settings, asof_ms=10000)["recent_candidates"][0]
    assert (row["audit_class"] == "RECORDER_DROP") == (drop_source == "bybit")
    if drop_source == "bybit":
        assert row["gap_count"] == 1 and row["largest_gap_ms"] == 400
    recorder._gap("native/binance/ws/publicTrade.TESTUSDT", "TESTUSDT", 7000, "RECORDER_BACKPRESSURE")
    assert recorder.gaps[("binance", "TESTUSDT")]["source"] == "binance"
    store.close()
