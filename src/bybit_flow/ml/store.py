"""Additive SQLite migration, immutable snapshots/labels, atomic Parquet dataset exports."""

import hashlib
import json
import os
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from .features import snapshot


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def migrate(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS ml_snapshots(
      id TEXT PRIMARY KEY, signal_id TEXT NOT NULL, stage TEXT NOT NULL,
      decision_ms INTEGER NOT NULL, schema_version TEXT NOT NULL, payload TEXT NOT NULL,
      UNIQUE(signal_id,stage,schema_version));
    CREATE TABLE IF NOT EXISTS ml_labels(
      snapshot_id TEXT NOT NULL REFERENCES ml_snapshots(id), policy TEXT NOT NULL,
      available_ms INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(snapshot_id,policy));
    CREATE TABLE IF NOT EXISTS ml_models(
      id TEXT PRIMARY KEY, created_ms INTEGER NOT NULL, manifest TEXT NOT NULL, sha256 TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS ml_history(
      id INTEGER PRIMARY KEY, at_ms INTEGER NOT NULL, model_id TEXT, action TEXT, payload TEXT);
    CREATE TABLE IF NOT EXISTS ml_holdouts(
      start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL, experiment_id TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS ml_predictions(
      snapshot_id TEXT NOT NULL, model_id TEXT NOT NULL, at_ms INTEGER NOT NULL,
      probability REAL NOT NULL, accepted INTEGER NOT NULL,
      PRIMARY KEY(snapshot_id,model_id));
    INSERT OR IGNORE INTO schema_version VALUES(3);
    INSERT OR IGNORE INTO schema_version VALUES(4);
    CREATE INDEX IF NOT EXISTS ml_snapshot_identity
      ON ml_snapshots(signal_id,stage,schema_version);
    CREATE INDEX IF NOT EXISTS ml_sequence_lookup ON ml_snapshots(
      json_extract(payload,'$.source'), json_extract(payload,'$.signal.symbol'),
      json_extract(payload,'$.signal.family'), json_extract(payload,'$.signal.direction'), decision_ms DESC
    ) WHERE stage='decision';
    """)
    db.commit()


class FeatureStore:
    def __init__(self, store):
        self.store, self.db = store, store.db

    def capture(self, signal, at_ms, stage):
        from . import SCHEMA_VERSION

        old = self.db.execute(
            "SELECT id FROM ml_snapshots WHERE signal_id=? AND stage=? AND schema_version=?",
            (signal.id, stage, SCHEMA_VERSION),
        ).fetchone()
        if old:
            return old[0]
        membership = self.store.membership_asof(signal.symbol, at_ms)
        if membership:
            details = json.loads(membership["payload"])
            if details.get("exchange", "bybit") != signal.source:
                membership = None  # Another venue's current liquidity is not this candidate's history.
        row = snapshot(signal, at_ms, stage, membership)
        if stage == "decision":
            from .stacking import SEQUENCE_KEYS

            history = self.db.execute(
                "SELECT payload FROM ml_snapshots WHERE stage='decision' AND decision_ms<? "
                "AND decision_ms>=? AND json_extract(payload,'$.source')=? "
                "AND json_extract(payload,'$.signal.symbol')=? "
                "AND json_extract(payload,'$.signal.family')=? "
                "AND json_extract(payload,'$.signal.direction')=? "
                "ORDER BY decision_ms DESC LIMIT 15",
                (at_ms, at_ms - 7_200_000, signal.source, signal.symbol, signal.family, signal.direction),
            ).fetchall()
            prior = [json.loads(r[0]) for r in reversed(history)]
            row["sequence"] = [
                dict(at_ms=r["decision_ms"], values={k: r["values"].get(k) for k in SEQUENCE_KEYS})
                for r in prior + [row]
            ]
        ident = digest(row)
        with self.db:
            self.db.execute(
                "INSERT INTO ml_snapshots VALUES(?,?,?,?,?,?)",
                (ident, signal.id, stage, at_ms, SCHEMA_VERSION, canonical(row)),
            )
        return ident

    def snapshots(self, stage="decision", limit=10_000):
        rows = self.db.execute(
            "SELECT id,payload FROM ml_snapshots WHERE stage=? ORDER BY decision_ms LIMIT ?",
            (stage, limit + 1),
        ).fetchall()
        # Close the bounded read cursor before yielding: the recorder may commit
        # while labeling, and an old WAL read snapshot cannot upgrade to a writer.
        for i, row in enumerate(rows):
            if i >= limit:
                raise ValueError("Dataset exceeds configured bounded research capacity")
            yield {"id": row[0], **json.loads(row[1])}

    def label(self, snapshot_id, result, available_ms):
        row = self.db.execute("SELECT decision_ms FROM ml_snapshots WHERE id=?", (snapshot_id,)).fetchone()
        if not row or available_ms < row[0] or (result.get("exit_ms") or 0) > available_ms:
            raise ValueError("Unknown snapshot or future label")
        text = canonical(result)
        old = self.db.execute(
            "SELECT payload FROM ml_labels WHERE snapshot_id=? AND policy=?", (snapshot_id, result["policy"])
        ).fetchone()
        if old and old[0] != text:
            raise ValueError("Immutable label conflict: use a new versioned label policy")
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO ml_labels VALUES(?,?,?,?)",
                (snapshot_id, result["policy"], available_ms, text),
            )

    def dataset(self, asof_ms, policy="prints-v1", stage="decision", limit=10_000):
        result = []
        for s in self.snapshots(stage, limit):
            row = self.db.execute(
                "SELECT available_ms,payload FROM ml_labels WHERE snapshot_id=? "
                "AND policy=? AND available_ms<=?",
                (s["id"], policy, asof_ms),
            ).fetchone()
            if not row:
                continue
            label = json.loads(row[1])
            if not label.get("complete") or label.get("net_r") is None or label["exit_ms"] > asof_ms:
                continue
            result.append({**s, "label": label, "label_available_ms": row[0]})
        return result

    def export(self, asof_ms, policy="prints-v1", stage="decision", source=None):
        from . import SCHEMA_VERSION

        rows = self.dataset(asof_ms, policy, stage)
        rows = [
            r
            for r in rows
            if r["schema_version"] == SCHEMA_VERSION and (source is None or r["source"] == source)
        ]
        if len({r["source"] for r in rows}) > 1:
            raise ValueError(
                "Multiple source methodologies: export with --source; never pool venues silently"
            )
        if not rows:
            raise ValueError("No complete resolved candidate labels; nothing fabricated")
        ident = digest(rows)
        directory = self.store.root / "ml" / "datasets"
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"{ident}.parquet"
        if not target.exists():
            temporary = directory / f".{uuid.uuid4().hex}.tmp"
            pq.write_table(
                pa.Table.from_pylist(
                    [
                        dict(
                            id=r["id"],
                            decision_ms=r["decision_ms"],
                            label_available_ms=r["label_available_ms"],
                            payload=canonical(r),
                        )
                        for r in rows
                    ]
                ),
                temporary,
                compression="zstd",
            )
            os.replace(temporary, target)
        return target
