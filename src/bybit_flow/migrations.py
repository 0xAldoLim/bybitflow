"""Ordered, checksummed schema registry. Legacy ensure steps preserve startup semantics."""

import hashlib
import inspect
from dataclasses import dataclass
from typing import Callable

CORE_SQL = """
        CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY);
        INSERT OR IGNORE INTO schema_version VALUES(1);
        CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS signals(id TEXT PRIMARY KEY, symbol TEXT, created_ms INTEGER,
            state TEXT, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS transitions(id INTEGER PRIMARY KEY, signal_id TEXT,
            at_ms INTEGER, state TEXT, reason TEXT);
        CREATE TABLE IF NOT EXISTS universe(at_ms INTEGER, symbol TEXT, eligible INTEGER,
            payload TEXT, PRIMARY KEY(at_ms,symbol));
        CREATE TABLE IF NOT EXISTS facts(id INTEGER PRIMARY KEY, asset TEXT, known_ms INTEGER,
            effective_ms INTEGER, expires_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS journal(id INTEGER PRIMARY KEY, at_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS experiments(id TEXT PRIMARY KEY, at_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS segments(id TEXT PRIMARY KEY, at_ms INTEGER, payload TEXT);
        CREATE TABLE IF NOT EXISTS outbox(key TEXT PRIMARY KEY, signal_id TEXT, status TEXT,
            payload TEXT, message_id TEXT, updated_ms INTEGER);
        CREATE TABLE IF NOT EXISTS terminal_events(signal_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
            notification_status TEXT NOT NULL, attempts INTEGER DEFAULT 0, next_ms INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS tv_inbox(event_id TEXT PRIMARY KEY, digest TEXT NOT NULL,
            received_ms INTEGER NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
            result TEXT);
        CREATE INDEX IF NOT EXISTS tv_pending ON tv_inbox(status,received_ms);
        CREATE INDEX IF NOT EXISTS signals_recent ON signals(created_ms DESC);
        CREATE INDEX IF NOT EXISTS signals_active_lifecycle
          ON signals(CASE WHEN state='ALERTED' THEN 0 ELSE 1 END,created_ms DESC)
          WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED');
        INSERT OR IGNORE INTO schema_version VALUES(2);
        """

RELIABILITY_SQL = """
CREATE TABLE IF NOT EXISTS operational_samples(bucket_ms INTEGER PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS storage_growth(bucket_ms INTEGER PRIMARY KEY,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS policy_opportunities(
 id TEXT PRIMARY KEY,candidate_identity TEXT NOT NULL,source TEXT NOT NULL,symbol TEXT NOT NULL,
 decision_ms INTEGER NOT NULL,payload TEXT NOT NULL,UNIQUE(source,candidate_identity));
CREATE INDEX IF NOT EXISTS policy_opportunity_time ON policy_opportunities(decision_ms,id);
CREATE TABLE IF NOT EXISTS policy_results(
 opportunity_id TEXT NOT NULL REFERENCES policy_opportunities(id),policy_id TEXT NOT NULL,
 provenance TEXT NOT NULL,available_ms INTEGER NOT NULL,payload TEXT NOT NULL,
 PRIMARY KEY(opportunity_id,policy_id,provenance));
CREATE INDEX IF NOT EXISTS policy_result_available ON policy_results(available_ms,opportunity_id);
CREATE TABLE IF NOT EXISTS policy_promotions(id TEXT PRIMARY KEY,created_ms INTEGER NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS deployment_records(id TEXT PRIMARY KEY,created_ms INTEGER NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS recovery_status(job_id TEXT PRIMARY KEY,updated_ms INTEGER NOT NULL,payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS recovery_status_time ON recovery_status(updated_ms,job_id);
"""


class MigrationIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Migration:
    id: str
    name: str
    callback: Callable
    target: str = "store"
    repeatable: bool = True
    sql: str = ""

    @property
    def checksum(self):
        return hashlib.sha256((inspect.getsource(self.callback) + self.sql).encode()).hexdigest()


def core(store):
    store.db.executescript(CORE_SQL)
    store.db.commit()


def reliability_schema(store):
    # Executed with the registry receipt in one transaction; no historical row rewrites.
    for statement in RELIABILITY_SQL.split(";"):
        if statement.strip():
            store.db.execute(statement)


def registry():
    from . import funnel, identity, observations, packing, path_research, setup_research
    from .ml import store as features

    return (
        Migration("001", "legacy-core", core, sql=CORE_SQL),
        Migration("002", "legacy-ml", features.migrate, target="db"),
        Migration("003", "legacy-observations", observations.migrate),
        Migration("004", "legacy-packing", packing.migrate),
        Migration("005", "legacy-identities", identity.migrate, target="backup"),
        Migration("006", "legacy-funnel", funnel.migrate),
        Migration("007", "legacy-swing-paths", path_research.migrate),
        Migration("008", "legacy-setup-paths", setup_research.migrate),
        Migration(
            "009", "reliability-evidence-loop", reliability_schema, repeatable=False, sql=RELIABILITY_SQL
        ),
    )


def apply(store, backup_directory=None):
    from .storage import now_ms

    store.db.execute(
        "CREATE TABLE IF NOT EXISTS migration_registry("
        "migration_id TEXT PRIMARY KEY,name TEXT NOT NULL,checksum TEXT NOT NULL,"
        "applied_ms INTEGER NOT NULL,repeatable INTEGER NOT NULL)"
    )
    store.db.commit()
    steps = registry()
    applied = {r[0]: r[1] for r in store.db.execute("SELECT migration_id,checksum FROM migration_registry")}
    # Check every known receipt before making further changes.
    for step in steps:
        if step.id in applied and applied[step.id] != step.checksum:
            raise MigrationIntegrityError("Applied migration checksum mismatch: " + step.id)
    for step in steps:
        if step.id in applied and not step.repeatable:
            continue
        with store.db:
            if step.target == "db":
                step.callback(store.db)
            elif step.target == "backup":
                step.callback(store, backup_directory)
            else:
                step.callback(store)
            store.db.execute(
                "INSERT OR IGNORE INTO migration_registry VALUES(?,?,?,?,?)",
                (step.id, step.name, step.checksum, now_ms(), int(step.repeatable)),
            )


def status(store):
    exists = store.db.execute("SELECT 1 FROM sqlite_master WHERE name='migration_registry'").fetchone()
    if not exists:
        return dict(level=None, status="LEGACY_UNREGISTERED", migrations=[])
    rows = [
        dict(id=r[0], name=r[1], checksum=r[2], applied_ms=r[3], repeatable=bool(r[4]))
        for r in store.db.execute("SELECT * FROM migration_registry ORDER BY migration_id")
    ]
    return dict(level=rows[-1]["id"] if rows else None, status="REGISTERED", migrations=rows)
