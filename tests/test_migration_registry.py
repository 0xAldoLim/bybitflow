import json
import sqlite3
from pathlib import Path

import pytest

from bybit_flow.migrations import MigrationIntegrityError, status
from bybit_flow.storage import Store


@pytest.mark.parametrize("fixture", ["legacy_v1.sql", "legacy_v22.sql"])
def test_supported_legacy_schema_preserves_history_and_repeated_startup(tmp_path, signal, fixture):
    db = sqlite3.connect(tmp_path / "research.sqlite")
    db.executescript((Path(__file__).parent / "fixtures" / fixture).read_text(encoding="utf-8"))
    payload = signal.model_dump_json()
    db.execute(
        "INSERT INTO signals VALUES(?,?,?,?,?)",
        (signal.id, signal.symbol, signal.created_ms, signal.state, payload),
    )
    outbox = json.dumps(dict(embeds=[dict(title="Historical fixture")]))
    db.execute(
        "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
        ("fixture:initial", signal.id, "sent", outbox, "fixture-message", 2000),
    )
    if fixture == "legacy_v22.sql":
        db.execute(
            "INSERT INTO ml_snapshots VALUES('frozen',?,'decision',1000,'candidate-v20','{}')", (signal.id,)
        )
        db.execute(
            "INSERT INTO ml_labels VALUES('frozen','prints-v1',2000,?)",
            (json.dumps(dict(complete=False), separators=(",", ":")),),
        )
    db.commit()
    db.close()
    store = Store(tmp_path)
    assert store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0] == payload
    assert store.db.execute("SELECT payload,message_id,status FROM outbox").fetchone()[:] == (
        outbox,
        "fixture-message",
        "sent",
    )
    if fixture == "legacy_v22.sql":
        assert (
            store.db.execute("SELECT payload FROM ml_labels WHERE snapshot_id='frozen'").fetchone()[0]
            == '{"complete":false}'
        )
    migrations = status(store)
    assert migrations["level"] == "009"
    for name in (
        "policy_opportunities",
        "operational_samples",
        "ml_primary_labels",
        "signals_active_lifecycle",
    ):
        assert store.db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (name,)).fetchone()
    store.close()
    store = Store(tmp_path)
    assert status(store) == migrations
    assert store.db.execute("SELECT payload FROM signals WHERE id=?", (signal.id,)).fetchone()[0] == payload
    store.close()


def test_modified_applied_migration_is_rejected(tmp_path):
    store = Store(tmp_path)
    with store.db:
        store.db.execute("UPDATE migration_registry SET checksum='changed' WHERE migration_id='009'")
    store.close()
    with pytest.raises(MigrationIntegrityError, match="009"):
        Store(tmp_path)


@pytest.mark.parametrize("fixture", ["legacy_v1.sql", "legacy_v22.sql"])
def test_application_starts_on_old_fixture_and_exposes_additive_status(tmp_path, fixture):
    from fastapi.testclient import TestClient

    from bybit_flow.app import create_app
    from bybit_flow.config import Settings

    db = sqlite3.connect(tmp_path / "research.sqlite")
    db.executescript((Path(__file__).parent / "fixtures" / fixture).read_text(encoding="utf-8"))
    db.close()
    settings = Settings(_env_file=None, data_dir=tmp_path, macro_news_enabled=False)
    with TestClient(create_app(settings)) as client:
        assert client.get("/healthz").status_code == 200
        operations = client.get("/api/operations").json()
        assert operations["provenance"]["database"]["level"] == "009"
        assert "time_series" in operations
        assert client.get("/api/overview").json()["decisions"]["opportunities"] == []
        assert client.get("/api/forward").json()["opportunities"] == 0
