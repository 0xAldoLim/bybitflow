import json

import pytest

from bybit_flow.models import DAY
from bybit_flow.scanner_confirmation import pending_recording_symbols
from bybit_flow.storage import Store


def test_pending_recording_symbols_do_not_decode_historical_snapshots(settings):
    store = Store(settings.data_dir)
    now = 10 * DAY
    cutoff = now - 7 * DAY
    rows = [
        ("pending", "decision", now, "PENDING"),
        ("other-venue", "decision", now, "OTHER"),
        ("labeled", "decision", now, "LABELED"),
        ("incomplete", "decision", now, "INCOMPLETE"),
        ("proxy-only", "decision", now, "PROXY"),
        ("recovered-only", "decision", now, "RECOVERED"),
        ("boundary", "decision", cutoff, "BOUNDARY"),
        ("old", "decision", cutoff - 1, "OLD"),
        ("generation", "generation", now, "GENERATION"),
        ("missing-symbol", "decision", now, None),
    ]
    with store.db:
        store.db.executemany(
            "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
            [
                (
                    ident,
                    ident,
                    stage,
                    at,
                    "candidate-v20",
                    json.dumps(dict(signal=dict(symbol=symbol), evidence="x" * 65536)),
                )
                for ident, stage, at, symbol in rows
            ],
        )
        store.db.executemany(
            "INSERT INTO ml_labels VALUES(?,?,?,?)",
            [
                ("labeled", "prints-v1", now, '{"complete":true}'),
                ("incomplete", "prints-v1", now, '{"complete":false}'),
                ("proxy-only", "monitored-ohlc-v1", now, '{"complete":true}'),
                ("recovered-only", "prints-recovered-v1", now, '{"complete":true}'),
            ],
        )
    original = (
        "SELECT DISTINCT json_extract(s.payload,'$.signal.symbol') FROM ml_snapshots s "
        "WHERE s.stage='decision' AND NOT EXISTS (SELECT 1 FROM ml_labels l "
        "WHERE l.snapshot_id=s.id AND l.policy='prints-v1') AND s.decision_ms>?"
    )
    expected = {row[0] for row in store.db.execute(original, (cutoff,))}
    assert expected == {"PENDING", "OTHER", "PROXY", "RECOVERED", None}
    statements = []
    store.db.set_trace_callback(statements.append)
    actual = pending_recording_symbols(store, cutoff)
    store.db.set_trace_callback(None)
    assert actual == expected
    query = next(
        sql for sql in statements if sql.startswith("SELECT DISTINCT s.symbol FROM ml_recording_symbols")
    )
    plan = [row[3] for row in store.db.execute("EXPLAIN QUERY PLAN " + query)]
    assert any("USING COVERING INDEX ml_recording_symbols_time" in step for step in plan)
    # Verify the execution path, not a machine-dependent timing threshold:
    # SQLite must read the stored index expression, never parse large JSON rows.
    bytecode = store.db.execute("EXPLAIN " + query).fetchall()
    assert not any(row[1] == "Function" and "json_extract" in str(row[5]) for row in bytecode)
    with store.db:
        store.db.execute(
            "INSERT INTO ml_labels VALUES('pending','prints-v1',?,?)", (now, '{"complete":true}')
        )
        store.db.execute(
            "INSERT INTO ml_snapshots VALUES('new','new','decision',?,'candidate-v20',?)",
            (now + 1, '{"signal":{"symbol":"NEW"}}'),
        )
    assert pending_recording_symbols(store, cutoff) == expected - {"PENDING"} | {"NEW"}
    with store.db:
        store.db.execute(
            "UPDATE ml_snapshots SET payload=? WHERE id='new'", ('{"signal":{"symbol":"UPDATED"}}',)
        )
    assert pending_recording_symbols(store, cutoff) == expected - {"PENDING"} | {"UPDATED"}
    with store.db:
        store.db.execute("DELETE FROM ml_snapshots WHERE id='new'")
    assert pending_recording_symbols(store, cutoff) == expected - {"PENDING"}
    store.close()


@pytest.mark.parametrize("previous_level", ["009", "010"])
def test_recording_symbol_index_upgrade_preserves_existing_evidence(settings, monkeypatch, previous_level):
    from bybit_flow import migrations

    registry = migrations.registry
    monkeypatch.setattr(
        migrations, "registry", lambda: tuple(step for step in registry() if step.id <= previous_level)
    )
    store = Store(settings.data_dir)
    with store.db:
        store.db.execute(
            "INSERT INTO ml_snapshots VALUES('frozen','original','decision',1000,'candidate-v4',?)",
            ('{"signal":{"symbol":"ORIGINAL"},"immutable":"evidence"}',),
        )
        store.db.execute("INSERT INTO ml_labels VALUES('frozen','prints-v1',2000,'{\"complete\":false}')")
        store.db.execute("INSERT INTO ml_models VALUES('model',1000,'{}','saved-checksum')")
        store.db.execute("INSERT INTO outbox VALUES('old:initial',NULL,'sent','{}','message',2000)")
    tables = ("ml_snapshots", "ml_labels", "ml_models", "outbox", "migration_registry")
    before = {table: [tuple(row) for row in store.db.execute("SELECT * FROM " + table)] for table in tables}
    store.close()
    monkeypatch.setattr(migrations, "registry", registry)
    store = Store(settings.data_dir)
    for table in tables[:-1]:
        assert [tuple(row) for row in store.db.execute("SELECT * FROM " + table)] == before[table]
    assert [
        tuple(row)
        for row in store.db.execute(
            "SELECT * FROM migration_registry WHERE migration_id<=?", (previous_level,)
        )
    ] == before["migration_registry"]
    assert migrations.status(store)["level"] == "012"
    assert store.db.execute("SELECT 1 FROM sqlite_master WHERE name='ml_recording_symbols_time'").fetchone()
    assert (
        store.db.execute("SELECT symbol FROM ml_recording_symbols WHERE snapshot_id='frozen'").fetchone()[0]
        == "ORIGINAL"
    )
    store.close()
