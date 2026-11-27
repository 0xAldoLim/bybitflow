import sqlite3

import pytest

from bybit_flow.ml.store import FeatureStore
from bybit_flow.slim import AUDIT, plan, rebuild, slim
from bybit_flow.storage import Store, now_ms


def seed(store, signal):
    """One delivered, one never-delivered terminal and one active candidate."""
    features = FeatureStore(store)
    ids = {}
    for name, state in (("delivered", "EXPIRED"), ("ignored", "EXPIRED"), ("active", "CONFIRMED")):
        candidate = signal.model_copy(update={"id": name, "state": state})
        store.signal(candidate)
        ids[name] = dict(
            generation=features.capture(candidate, 2_000, "generation"),
            decision=features.capture(candidate, 2_000, "decision"),
        )
    features.label(ids["ignored"]["decision"], dict(policy="ohlc-path-v1", complete=True), 3_000)
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            ("research:delivered:initial", "delivered", "sent", "{}", "1", 1_500),
        )
    return ids


def counts(store):
    return {
        stage: store.db.execute("SELECT count(*) FROM ml_snapshots WHERE stage=?", (stage,)).fetchone()[0]
        for stage in ("generation", "decision")
    } | {"origins": store.db.execute("SELECT count(*) FROM signal_origins").fetchone()[0]}


def test_dry_run_reports_and_changes_nothing(settings, signal):
    store = Store(settings.data_dir)
    seed(store, signal)
    before = counts(store)
    result = slim(store, dry_run=True)
    assert result["status"] == "DRY_RUN"
    assert result["generation_snapshots"] == 3 and result["signal_origins"] == 1
    assert result["generation_bytes"] > 0 and counts(store) == before
    store.close()


def test_slim_removes_only_unread_rows(settings, signal):
    store = Store(settings.data_dir)
    ids = seed(store, signal)
    labels = store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0]
    result = slim(store, dry_run=False, batch=2)
    assert result["status"] == "SLIMMED"
    assert result["deleted_generation_snapshots"] == 3 and result["deleted_signal_origins"] == 1
    assert counts(store) == {"generation": 0, "decision": 3, "origins": 2}
    kept = {row[0] for row in store.db.execute("SELECT signal_id FROM signal_origins")}
    assert kept == {"delivered", "active"}  # Delivered and still-active setups keep their origin.
    assert store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0] == labels
    assert store.db.execute("SELECT count(*) FROM signals").fetchone()[0] == 3
    assert (
        store.db.execute(
            "SELECT count(*) FROM ml_snapshots WHERE id=?", (ids["ignored"]["decision"],)
        ).fetchone()[0]
        == 1
    )
    assert store.get(AUDIT)["status"] == "SLIMMED"
    assert plan(store)["generation_snapshots"] == 0
    store.close()


def test_generation_snapshot_with_a_label_is_kept(settings, signal):
    store = Store(settings.data_dir)
    ids = seed(store, signal)
    with store.db:
        store.db.execute(
            "INSERT INTO ml_labels VALUES(?,?,?,?)",
            (ids["active"]["generation"], "ohlc-path-v1", 3_000, "{}"),
        )
    slim(store, dry_run=False)
    assert counts(store)["generation"] == 1
    store.close()


@pytest.mark.parametrize(
    "key,payload",
    [
        ("raw_retirement_state_v1", {"status": "RUNNING"}),
        ("cleanup_run_claim_v212", {"expires_ms": None}),
    ],
)
def test_slim_defers_while_a_fingerprinting_run_is_active(settings, signal, key, payload):
    store = Store(settings.data_dir)
    seed(store, signal)
    if payload.get("expires_ms", 0) is None:
        payload = {"expires_ms": now_ms() + 60_000}
    store.put(key, payload)
    before = counts(store)
    assert slim(store, dry_run=False)["status"] == "DEFERRED"
    assert counts(store) == before
    store.close()


def test_rebuild_writes_a_verified_compact_copy_and_leaves_the_original(settings, signal, tmp_path):
    store = Store(settings.data_dir)
    seed(store, signal)
    slim(store, dry_run=False)
    expected = counts(store)
    store.close()
    source = settings.data_dir / "research.sqlite"
    original = source.stat().st_size
    output = tmp_path / "compact.sqlite"
    result = rebuild(source, output)
    assert result["status"] == "REBUILT" and result["compact_bytes"] == output.stat().st_size
    assert source.stat().st_size == original
    copy = sqlite3.connect(output)
    assert copy.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    assert copy.execute("SELECT count(*) FROM ml_snapshots WHERE stage='decision'").fetchone() == (3,)
    assert copy.execute("SELECT count(*) FROM signal_origins").fetchone() == (expected["origins"],)
    copy.close()
    with pytest.raises(ValueError, match="already exists"):
        rebuild(source, output)


def test_generation_snapshots_are_not_written_when_disabled(settings, signal):
    store = Store(settings.data_dir)
    store.snapshot_generation = False
    store.signal(signal.model_copy(update={"id": "quiet", "state": "CONFIRMED"}))
    assert counts(store)["generation"] == 0
    registered = store.db.execute(
        "SELECT count(*) FROM candidate_identities WHERE signal_id='quiet'"
    ).fetchone()[0]
    assert registered == 1
    store.close()


def test_generation_snapshots_default_off_in_configuration():
    from bybit_flow.config import Settings

    assert Settings(_env_file=None).ml_generation_snapshots is False
