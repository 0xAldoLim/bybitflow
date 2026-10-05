import sqlite3
from types import SimpleNamespace

import pytest

from bybit_flow.backtest import PaperPosition
from bybit_flow.ml.bootstrap import backfill, evaluate, merged_ranges
from bybit_flow.ml.bootstrap_features import NUMERIC, project
from bybit_flow.ml.checkpoint import seal
from bybit_flow.ml.cli import fit_due, worker_step
from bybit_flow.ml.features import snapshot
from bybit_flow.ml.inference import apply, select_compatible
from bybit_flow.ml.registry import Registry
from bybit_flow.ml.store import FeatureStore, canonical, digest
from bybit_flow.models import Candle
from bybit_flow.retention import protection_ranges
from bybit_flow.storage import Recorder, Store


def frozen(signal, decision=120_000, source="binance"):
    s = signal.model_copy(deep=True)
    s.source, s.created_ms = source, decision
    s.expires_ms = decision + 180_000
    s.expected_hold_max = 5
    s.risk = dict(accepted=True, cost_per_base=0.2, net_rr=2.96)
    return snapshot(s, decision, "decision") | dict(id=s.id)


def candle(start, opening=100, high=102, low=99, close=101):
    return Candle(start, 60_000, opening, high, low, close, 100, 10_000)


@pytest.mark.parametrize(
    "direction,outcome", [("LONG", "TARGET"), ("SHORT", "TARGET"), ("LONG", "STOP"), ("LONG", "TIME_EXIT")]
)
def test_causal_ohlc_path(signal, direction, outcome):
    row = frozen(signal)
    row["signal"]["direction"] = direction
    if direction == "SHORT":
        row["signal"].update(stop=105, tp1=85)
    bars = [candle(120_000)]
    if outcome == "TARGET":
        bars.append(
            candle(180_000, high=116 if direction == "LONG" else 102, low=99 if direction == "LONG" else 84)
        )
    elif outcome == "STOP":
        bars.append(candle(180_000, low=94))
    else:
        bars.extend(candle(t) for t in range(180_000, 420_000, 60_000))
    result = evaluate(row, bars, "binance", 420_000, materialized_ms=900_000)
    assert result["outcome"] == outcome and result["complete"]
    assert result["event_available_ms"] == result["exit_ms"]
    assert result["event_available_ms"] < result["materialized_ms"]
    assert result["net_r"] == pytest.approx(
        (15 - 0.2) / 5 if outcome == "TARGET" else (-5 - 0.2) / 5 if outcome == "STOP" else (1 - 0.2) / 5
    )
    assert result["execution_fidelity"] == "proxy"
    assert not result["costs_verified"] and not result["production_execution_verified"]


def test_ohlc_ambiguity_no_entry_gaps_and_venue(signal):
    row = frozen(signal)
    both = evaluate(row, [candle(120_000, high=116, low=94)], "binance", 420_000)
    assert both["outcome"] == "AMBIGUOUS" and both["net_r"] is None
    # Open is outside entry zone; intrabar entry and target order cannot be inferred.
    entry_order = evaluate(
        row, [candle(120_000, opening=110, high=116, low=100, close=110)], "binance", 420_000
    )
    assert entry_order["outcome"] == "AMBIGUOUS"
    untouched = [
        candle(t, opening=110, high=112, low=108, close=111) for t in range(120_000, 420_000, 60_000)
    ]
    missed = evaluate(row, untouched, "binance", 420_000)
    assert missed["outcome"] == "NO_ENTRY" and not missed["complete"]
    assert evaluate(row, [candle(120_000), candle(240_000)], "binance", 420_000)["outcome"] == "INCOMPLETE"
    with pytest.raises(ValueError, match="original venue"):
        evaluate(row, untouched, "bybit", 420_000)
    row["signal"]["risk"] = {}
    assert evaluate(row, untouched, "binance", 420_000)["outcome"] == "INCOMPLETE"


def test_partial_and_unclosed_candles_cannot_create_win(signal):
    row = frozen(signal, decision=121_000)
    result = evaluate(row, [candle(120_000, high=116)], "binance", 421_000)
    assert result["outcome"] == "AMBIGUOUS"
    row = frozen(signal)
    bars = [candle(120_000)]
    assert evaluate(row, bars + [candle(180_000, high=116)], "binance", 200_000)["outcome"] == "INCOMPLETE"


def test_zone_touch_cannot_fabricate_better_entry_price(signal):
    row = frozen(signal)
    bars = [candle(120_000, opening=102, high=103, low=100.5, close=102)]
    bars.extend(candle(t) for t in range(180_000, 420_000, 60_000))
    result = evaluate(row, bars, "binance", 420_000)
    assert result["proxy_entry_price"] == 101
    assert result["net_r"] == pytest.approx(-0.2 / 5)


def test_projection_excludes_changed_definitions_and_leakage(signal):
    row = frozen(signal)
    row["schema_version"] = "candidate-v4"
    row["values"].update(ofi_normalized_l1=999, breadth_impulse_15m=999, flow_trust_score=1, quality_score=99)
    result = project(row)
    assert all(
        key not in result["values"]
        for key in (
            "ofi_normalized_l1",
            "breadth_impulse_15m",
            "flow_trust_score",
            "quality_score",
            "delta_pct",
        )
    )
    assert result["schema_version"] == "bootstrap-core-v1"
    assert result["original_schema_version"] == "candidate-v4"
    row["feature_metadata"]["net_rr"]["available_ms"] += 1
    with pytest.raises(ValueError, match="Noncausal"):
        project(row)
    row["feature_metadata"]["net_rr"]["available_ms"] -= 1
    row["feature_metadata"][NUMERIC[0]]["definition"] = "Changed calculation"
    with pytest.raises(ValueError, match="Changed"):
        project(row)


@pytest.mark.asyncio
async def test_merged_backfill_resumes_deduplicates_and_keeps_prints(settings, signal):
    store = Store(settings.data_dir)
    fs = FeatureStore(store)
    rows = []
    for index in range(3):
        row = frozen(signal, 120_000 + index * 60_000)
        row["signal"]["id"] = f"candidate-{index}"
        row["signal_id"] = f"candidate-{index}"
        ident = digest(row)
        with store.db:
            store.db.execute(
                "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
                (
                    ident,
                    row["signal_id"],
                    "decision",
                    row["decision_ms"],
                    row["schema_version"],
                    canonical(row),
                ),
            )
        rows.append(row)
        fs.label(ident, dict(policy="prints-v1", complete=False, classification="incomplete"), 600_000)
    assert len(merged_ranges(rows)) == 1

    class Market:
        name = "binance"
        calls = 0

        async def candles(self, symbol, interval, asof, limit, start):
            self.calls += 1
            return [candle(t) for t in range(start, asof, 60_000)]

    market = Market()
    result = await backfill(store, settings, "binance", market=market)
    assert market.calls == 1 and result["complete"] == 3
    assert store.db.execute("SELECT count(*) FROM ml_labels WHERE policy='prints-v1'").fetchone()[0] == 3
    await backfill(store, settings, "binance", market=market)
    assert market.calls == 1
    assert store.db.execute("SELECT count(*) FROM ml_labels WHERE policy='ohlc-path-v1'").fetchone()[0] == 3
    child = frozen(signal, 180_000)
    child["signal_id"] = child["signal"]["id"] = "duplicate-child"
    child_id = digest(child)
    with store.db:
        store.db.execute(
            "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
            (
                child_id,
                child["signal_id"],
                "decision",
                child["decision_ms"],
                child["schema_version"],
                canonical(child),
            ),
        )
        store.db.execute(
            "INSERT INTO candidate_identities VALUES(?,?,?,?,?)",
            (child["signal_id"], "same-opportunity", "candidate-0", "candidate-0", child["decision_ms"]),
        )
    await backfill(store, settings, "binance", market=market)
    assert market.calls == 1
    assert not store.db.execute(
        "SELECT 1 FROM ml_labels WHERE snapshot_id=? AND policy='ohlc-path-v1'", (child_id,)
    ).fetchone()
    store.close()


def test_checkpoint_releases_consumed_tape_and_fails_closed(settings, signal):
    store = Store(settings.data_dir)
    signal.created_ms = 1_000_000
    ident = FeatureStore(store).capture(signal, 1_000_000, "decision")
    original = protection_ranges(store, 5_000_000)[0][0]
    position = PaperPosition(signal, 1)
    values = dict(vars(position), signal=signal.model_dump(mode="json"), funding_timestamps=[])
    checkpoint = seal(
        dict(
            policy="incremental-prints-v1",
            cursor_ms=4_900_000,
            positions={ident: values},
            pending=1,
            subscribed=[],
            last=[],
            seen=[],
            input_event_hash="a" * 64,
        ),
        5_000_000,
    )
    store.put("primary_materialization", checkpoint)
    assert protection_ranges(store, 5_000_000)[0][0] == 4_000_000
    # Operational active plans retain their original interval even with a valid ML checkpoint.
    store.signal(signal)
    assert any(
        a == original and reason == "active setup" for a, _, reason in protection_ranges(store, 5_000_000)
    )
    checkpoint["cursor_ms"] += 1
    store.put("primary_materialization", checkpoint)
    assert (
        next(
            a
            for a, _, reason in protection_ranges(store, 5_000_000)
            if reason == "unresolved primary outcome"
        )
        == original
    )
    store.put(
        "primary_materialization",
        seal(
            dict(
                policy="incremental-prints-v1",
                cursor_ms=4_900_000,
                positions={ident: values},
                pending=1,
                subscribed=[],
                last=[],
                seen=[],
                input_event_hash="a" * 64,
            ),
            1,
        ),
    )
    assert (
        next(
            a
            for a, _, reason in protection_ranges(store, 5_000_000)
            if reason == "unresolved primary outcome"
        )
        == original
    )
    store.close()


def test_bootstrap_has_no_promotion_rights_and_primary_preferred(settings, signal):
    store = Store(settings.data_dir)
    registry = Registry(store)
    row = frozen(signal)
    signal.source = "binance"
    manifest = dict(
        id="b" * 32,
        created_ms=100_000,
        track="bootstrap",
        feature_schema_version="bootstrap-core-v1",
        label_policy="ohlc-path-v1",
        source="binance",
        stage="decision",
        periods=dict(holdout=dict(end=100_000)),
        model={},
        minimum_coverage=0,
    )
    registry.register(manifest)
    with pytest.raises(ValueError, match="cannot be promoted"):
        registry.promote(manifest["id"], "Named reviewer")
    primary = manifest | dict(
        id="a" * 32,
        track="primary",
        feature_schema_version="candidate-v10",
        label_policy="prints-v1",
        strategy_versions=[signal.version],
    )
    with store.db:
        store.db.execute(
            "INSERT INTO ml_models VALUES(?,?,?,?)",
            (primary["id"], 90_000, canonical(primary), digest(primary)),
        )
    assert select_compatible(registry, signal, row, 120_000)[0]["id"] == primary["id"]
    store.close()


def test_bootstrap_cannot_filter_change_quality_or_supply_probability(settings, signal, monkeypatch):
    store = Store(settings.data_dir)
    settings.ml_enabled = settings.ml_filter_research = True
    signal.source = "binance"
    original = signal.quality, signal.raw_tier, signal.entry, signal.stop, signal.tp1
    model = dict(
        id="b" * 32, track="bootstrap", model=dict(thresholds=dict(min_probability=0.7, min_quality=95))
    )
    monkeypatch.setattr("bybit_flow.ml.inference.select_compatible", lambda *a: (model, False, []))
    monkeypatch.setattr("bybit_flow.ml.inference.predict", lambda *a: [0.01])
    monkeypatch.setattr("bybit_flow.ml.inference.explain", lambda *a: {})
    apply(signal, settings, store, 2000)
    assert signal.validation_status == "bootstrap_challenger"
    assert signal.evidence["ml"]["production_authority"] == "none"
    assert signal.calibrated_probability is None and not signal.gates
    assert original == (signal.quality, signal.raw_tier, signal.entry, signal.stop, signal.tp1)
    store.close()


def test_first_ready_fit_immediate_and_later_fit_bounded():
    previous = dict(status="abstained", at_ms=10_000, attempted_count=499)
    assert not fit_due(previous, 499, 11_000)
    assert fit_due(previous, 500, 11_000)
    fit = dict(status="challenger", at_ms=10_000, trained_count=500)
    assert not fit_due(fit, 549, 11_000)
    assert fit_due(fit, 550, 11_000)
    assert fit_due(fit, 500, 10_000 + 7 * 86_400_000)
    failed_retrain = fit | dict(
        status="abstained", model_id="existing", attempted_count=500, last_success_ms=10_000
    )
    assert not fit_due(failed_retrain, 501, 11_000)


def test_worker_busy_write_defers_without_exiting(settings):
    store = SimpleNamespace(
        put=lambda *a: (_ for _ in ()).throw(sqlite3.OperationalError("database is locked"))
    )
    assert worker_step(settings, store)["status"] == "DEFERRED_SQLITE_BUSY"


def test_corrupt_bootstrap_remains_advisory(settings, signal):
    store = Store(settings.data_dir)
    settings.ml_enabled = settings.ml_filter_research = True
    signal.source = "binance"
    model = dict(id="b" * 32, created_ms=1000, track="bootstrap", label_policy="ohlc-path-v1", model={})
    Registry(store).register(model)
    with store.db:
        store.db.execute("UPDATE ml_models SET sha256='corrupt'")
    apply(signal, settings, store, 2000)
    assert signal.validation_status == "abstained"
    assert not signal.gates and signal.calibrated_probability is None
    store.close()


@pytest.mark.asyncio
async def test_storage_pressure_defers_bootstrap(settings, signal):
    store = Store(settings.data_dir)
    settings.max_storage_gb = 0.000001

    class Market:
        name = "binance"

        async def candles(self, *a, **kw):
            pytest.fail("Storage pressure must prioritize recorder and pruning")

    result = await backfill(store, settings, "binance", market=Market())
    assert result["status"] == "STORAGE_BACKPRESSURE"
    store.close()


def test_prune_consumed_slice_preserves_late_commit_and_tail(settings, signal):
    from pathlib import Path

    from bybit_flow.retention import prune_recordings

    store = Store(settings.data_dir)
    recorder = Recorder(store, settings)
    for receipt in (1_000_000, 2_000_000, 4_500_000):
        recorder.flush(
            [
                dict(
                    source="control/subscribed",
                    symbol="TESTUSDT",
                    event_ms=receipt,
                    receipt_ms=receipt,
                    schema_version=1,
                    complete=True,
                    payload='{"symbols":["TESTUSDT"]}',
                )
            ]
        )
    manifests = sorted(store.rows("segments"), key=lambda r: r["max_receipt_ms"])
    with store.db:
        store.db.execute("UPDATE segments SET at_ms=4900000")
        store.db.execute("UPDATE segments SET at_ms=5000001 WHERE id=?", (manifests[1]["id"],))
    signal.created_ms = 1_000_000
    ident = FeatureStore(store).capture(signal, signal.created_ms, "decision")
    position = PaperPosition(signal, 1)
    values = dict(vars(position), signal=signal.model_dump(mode="json"), funding_timestamps=[])
    store.put(
        "primary_materialization",
        seal(
            dict(
                policy="incremental-prints-v1",
                cursor_ms=4_900_000,
                positions={ident: values},
                pending=1,
                subscribed=[],
                last=[],
                seen=[],
                input_event_hash="a" * 64,
            ),
            5_000_000,
        ),
    )
    result = prune_recordings(store, settings, 5_000_000, force=True)
    assert result["freed_bytes"] > 0
    assert not Path(manifests[0]["raw"]).exists()
    assert Path(manifests[1]["raw"]).exists()  # Committed after the frozen replay view.
    assert Path(manifests[2]["raw"]).exists()  # Unprocessed tail / replay safety margin.
    assert store.db.execute("SELECT count(*) FROM ml_snapshots").fetchone()[0] == 1
    store.close()
