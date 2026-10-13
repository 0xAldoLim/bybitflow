import json
from decimal import Decimal, localcontext

import pytest

from bybit_flow.observations import refresh_recommendations
from bybit_flow.orderflow import Book
from bybit_flow.recording_maintenance import FINALIZED_QUERY
from bybit_flow.storage import Store

RECOMMENDATIONS = (
    "SELECT json_extract(payload,'$.coverage_complete'), "
    "json_extract(payload,'$.signal.family'), "
    "json_extract(payload,'$.extended_same_rules_outcome'), "
    "json_extract(payload,'$.best_observed_horizon') FROM research_labels "
    "WHERE available_ms<? ORDER BY available_ms DESC LIMIT 10000"
)
FINALIZED = (
    "SELECT s.decision_ms,coalesce(json_extract(l.payload,'$.exit_ms'),l.available_ms),"
    "json_extract(l.payload,'$.complete'),json_extract(l.payload,'$.classification') "
    "FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id AND l.policy='prints-v1' "
    "WHERE s.stage='decision'"
)
COMPACT_RECOMMENDATIONS = (
    "SELECT coverage_complete,family,outcome,horizon FROM research_label_summaries "
    "WHERE available_ms<? ORDER BY available_ms DESC,source_rowid LIMIT 10000"
)


def remove_projection(store, kind, table):
    with store.db:
        for action in ("insert", "update", "delete"):
            store.db.execute(f"DROP TRIGGER {table}_{action}")
        store.db.execute(f"DROP TABLE {table}")
        store.db.execute("DELETE FROM kv WHERE key=?", ("query_projection_v1:" + kind,))


def test_existing_research_history_uses_compact_projection_without_sorting(settings):
    store = Store(settings.data_dir)
    with store.db:
        for ident, at, complete, family, outcome, horizon in (
            ("old", 100, True, "range_rejection", "TARGET", "SWING"),
            ("incomplete", 200, False, "trend_pullback", "TARGET", "CORE_INTRADAY"),
            ("loss", 300, True, "range_rejection", "STOP", "SHORT_INTRADAY"),
            ("missing", 350, True, "trend_pullback", None, None),
            ("future", 500, True, "breakout_retest", "TARGET", "SWING"),
        ):
            payload = json.dumps(
                dict(
                    coverage_complete=complete,
                    signal=dict(family=family),
                    extended_same_rules_outcome=outcome,
                    best_observed_horizon=horizon,
                    unrelated_history="x" * 100_000,
                )
            )
            store.db.execute("INSERT INTO research_labels VALUES(?,?,?)", (ident, at, payload))
    remove_projection(store, "research", "research_label_summaries")
    historical = list(store.db.execute("SELECT * FROM research_labels"))
    expected = [tuple(row) for row in store.db.execute(RECOMMENDATIONS, (400,))]
    store.close()

    # An already-migrated store must receive the projection without rewriting labels.
    store = Store(settings.data_dir)
    assert list(store.db.execute("SELECT * FROM research_labels")) == historical
    assert [tuple(row) for row in store.db.execute(COMPACT_RECOMMENDATIONS, (400,))] == expected
    plan = [row[3] for row in store.db.execute("EXPLAIN QUERY PLAN " + COMPACT_RECOMMENDATIONS, (400,))]
    assert any("research_summary_recent" in step and "INDEX" in step for step in plan), plan
    assert not any("TEMP B-TREE" in step or "SCAN research_labels" in step for step in plan)
    store.close()
    refresh_recommendations(settings.data_dir, 400)
    store = Store(settings.data_dir)
    assert store.get("horizon_recommendations") == dict(
        available_ms=400,
        families=dict(
            range_rejection=dict(samples=2, successful_horizon_counts=dict(SWING=1)),
            trend_pullback=dict(samples=1, successful_horizon_counts={}),
        ),
    )
    store.close()


@pytest.mark.parametrize("kind", ["research", "retention"])
def test_projections_follow_source_writes_and_transaction_rollback(settings, kind):
    store = Store(settings.data_dir)
    if kind == "research":
        table, compact, key = "research_labels", "research_label_summaries", "signal_id"
        values = ("sample", 100, "{}")
        query, params = COMPACT_RECOMMENDATIONS, (500,)
        original = RECOMMENDATIONS
    else:
        table, compact, key = "ml_labels", "ml_label_retention", "snapshot_id"
        with store.db:
            store.db.execute(
                "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
                ("sample", "sample", "decision", 100, "candidate-v20", "{}"),
            )
        values = ("sample", "prints-v1", 100, "{}")
        query, params, original = FINALIZED_QUERY, (), FINALIZED
    insert = f"INSERT OR REPLACE INTO {table} VALUES({','.join('?' for _ in values)})"
    with store.db:
        store.db.execute(insert, values)
    for payload in (
        dict(
            complete=True,
            classification="complete",
            exit_ms=200,
            coverage_complete=True,
            signal=dict(family="range_rejection"),
            extended_same_rules_outcome="TARGET",
            best_observed_horizon="SWING",
        ),
        dict(complete=False, classification="incomplete", exit_ms=None, coverage_complete=False),
    ):
        with store.db:
            store.db.execute(f"UPDATE {table} SET payload=? WHERE {key}='sample'", (json.dumps(payload),))
        assert [tuple(row) for row in store.db.execute(query, params)] == [
            tuple(row) for row in store.db.execute(original, params)
        ]
    with store.db:
        store.db.execute(insert, values)
    assert [tuple(row) for row in store.db.execute(query, params)] == [
        tuple(row) for row in store.db.execute(original, params)
    ]
    with pytest.raises(RuntimeError, match="rollback"):
        with store.db:
            store.db.execute(f"DELETE FROM {table} WHERE {key}='sample'")
            raise RuntimeError("rollback")
    assert store.db.execute(f"SELECT count(*) FROM {compact}").fetchone()[0] == 1
    with store.db:
        store.db.execute(f"DELETE FROM {table} WHERE {key}='sample'")
    assert store.db.execute(f"SELECT count(*) FROM {compact}").fetchone()[0] == 0
    store.close()


def test_retention_projection_preserves_complete_incomplete_and_missing_exit(settings):
    store = Store(settings.data_dir)
    with store.db:
        for ident, stage, complete, classification, exit_ms in (
            ("win", "decision", True, "complete", 200),
            ("gap", "decision", False, "incomplete", None),
            ("unscored", "generation", False, "no_entry", 250),
        ):
            store.db.execute(
                "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
                (
                    ident,
                    ident,
                    stage,
                    100,
                    "candidate-v20",
                    "{}",
                ),
            )
            payload = json.dumps(
                dict(
                    complete=complete,
                    classification=classification,
                    exit_ms=exit_ms,
                    unrelated_history="x" * 100_000,
                )
            )
            store.db.execute("INSERT INTO ml_labels VALUES(?,?,?,?)", (ident, "prints-v1", 300, payload))
            store.db.execute("INSERT INTO ml_labels VALUES(?,?,?,?)", (ident, "other-policy", 400, "{}"))
    remove_projection(store, "retention", "ml_label_retention")
    historical = list(store.db.execute("SELECT * FROM ml_labels"))
    expected = sorted(tuple(row) for row in store.db.execute(FINALIZED))
    store.close()
    store = Store(settings.data_dir)
    assert list(store.db.execute("SELECT * FROM ml_labels")) == historical
    assert (
        sorted(tuple(row) for row in store.db.execute(FINALIZED_QUERY))
        == expected
        == [
            (100, 200, 1, "complete"),
            (100, 300, 0, "incomplete"),
        ]
    )
    plan = [row[3] for row in store.db.execute("EXPLAIN QUERY PLAN " + FINALIZED_QUERY)]
    assert any("ml_label_retention" in step and "INDEX" in step for step in plan), plan
    instructions = list(store.db.execute("EXPLAIN " + FINALIZED_QUERY))
    assert not any(row[1] == "Function" and "json_extract" in (row[5] or "") for row in instructions)
    store.close()


def test_research_projection_preserves_timestamp_ties_and_recent_row_limit(settings):
    store = Store(settings.data_dir)
    with store.db:
        store.db.executemany(
            "INSERT INTO research_labels VALUES(?,?,?)",
            (
                (
                    str(i),
                    100,
                    json.dumps(
                        dict(
                            coverage_complete=True,
                            signal=dict(family="range_rejection" if i % 2 else "trend_pullback"),
                            best_observed_horizon=str(i),
                        )
                    ),
                )
                for i in range(10002)
            ),
        )
    expected = [tuple(row) for row in store.db.execute(RECOMMENDATIONS, (200,))]
    actual = [tuple(row) for row in store.db.execute(COMPACT_RECOMMENDATIONS, (200,))]
    assert len(actual) == 10000
    assert actual == expected
    store.close()


@pytest.mark.parametrize("precision", [12, 28])
def test_depth_band_precomputation_preserves_boundary_inclusion(precision):
    book = Book()
    book.valid = True
    book.bids = {
        Decimal(p): Decimal(q)
        for p, q in [
            ("99.999", "2"),
            ("99.95", "3"),
            ("99.94999", "5"),
            ("99.9", "7"),
            ("99.75", "11"),
            ("99.74999", "13"),
        ]
    }
    book.asks = {
        Decimal(p): Decimal(q)
        for p, q in [
            ("100.001", "2"),
            ("100.05", "3"),
            ("100.05001", "5"),
            ("100.1", "7"),
            ("100.25", "11"),
            ("100.25001", "13"),
        ]
    }
    with localcontext() as context:
        context.prec = precision
        result = book.features(0)
    # Each exact band boundary is included; the next outward level is excluded.
    assert result["depth"]["5"]["bid"] == pytest.approx(99.999 * 2 + 99.95 * 3)
    assert result["depth"]["5"]["ask"] == pytest.approx(100.001 * 2 + 100.05 * 3)
    assert result["depth"]["10"]["bid"] == pytest.approx(99.999 * 2 + 99.95 * 3 + 99.94999 * 5 + 99.9 * 7)
    assert result["depth"]["10"]["ask"] == pytest.approx(100.001 * 2 + 100.05 * 3 + 100.05001 * 5 + 100.1 * 7)
    assert result["depth"]["25"]["bid"] == pytest.approx(
        99.999 * 2 + 99.95 * 3 + 99.94999 * 5 + 99.9 * 7 + 99.75 * 11
    )
    assert result["depth"]["25"]["ask"] == pytest.approx(
        100.001 * 2 + 100.05 * 3 + 100.05001 * 5 + 100.1 * 7 + 100.25 * 11
    )
