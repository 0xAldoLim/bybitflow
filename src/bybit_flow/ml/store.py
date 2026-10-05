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
    CREATE INDEX IF NOT EXISTS ml_snapshot_stage_time
      ON ml_snapshots(stage,decision_ms,id);
    CREATE INDEX IF NOT EXISTS ml_snapshot_signal_time
      ON ml_snapshots(signal_id,stage,decision_ms);
    CREATE INDEX IF NOT EXISTS ml_snapshot_original_lookup
      ON ml_snapshots(signal_id,stage,decision_ms,id);
    CREATE INDEX IF NOT EXISTS ml_sequence_lookup ON ml_snapshots(
      json_extract(payload,'$.source'), json_extract(payload,'$.signal.symbol'),
      json_extract(payload,'$.signal.family'), json_extract(payload,'$.signal.direction'), decision_ms DESC
    ) WHERE stage='decision';
    """)
    db.commit()
    if "scope" not in {r[1] for r in db.execute("PRAGMA table_info(ml_holdouts)")}:
        db.execute("ALTER TABLE ml_holdouts ADD COLUMN scope TEXT NOT NULL DEFAULT 'legacy-primary'")
        db.commit()


class FeatureStore:
    def __init__(self, store):
        self.store, self.db = store, store.db

    def capture(self, signal, at_ms, stage):
        from . import SCHEMA_VERSION

        old = self.db.execute(
            "SELECT id FROM ml_snapshots WHERE signal_id=? AND stage=? ORDER BY decision_ms,id LIMIT 1",
            (signal.id, stage),
        ).fetchone()
        if old:
            return old[0]
        from ..identity import register_candidate

        with self.db:
            register_candidate(self.store, signal)
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
                "AND decision_ms>=? AND schema_version=? AND json_extract(payload,'$.source')=? "
                "AND json_extract(payload,'$.signal.symbol')=? "
                "AND json_extract(payload,'$.signal.family')=? "
                "AND json_extract(payload,'$.signal.direction')=? "
                "AND coalesce(json_extract(payload,'$.signal.horizon_profile'),'LEGACY')=? "
                "ORDER BY decision_ms DESC LIMIT 15",
                (
                    at_ms,
                    at_ms - 7_200_000,
                    SCHEMA_VERSION,
                    signal.source,
                    signal.symbol,
                    signal.family,
                    signal.direction,
                    signal.horizon_profile,
                ),
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
            "SELECT id,payload FROM ml_snapshots WHERE stage=? ORDER BY decision_ms DESC,id DESC LIMIT ?",
            (stage, limit),
        ).fetchall()
        # Close the bounded read cursor before yielding: the recorder may commit
        # while labeling, and an old WAL read snapshot cannot upgrade to a writer.
        # Keep a rolling chronological training window without deleting older rows.
        for row in reversed(rows):
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

    def dataset(
        self,
        asof_ms,
        policy="prints-v1",
        stage="decision",
        limit=10_000,
        source=None,
        schema_version=None,
        confirmation_policy=None,
    ):
        """Return at most `limit` causally available, unique completed outcomes.

        SQL excludes unusable snapshots before bounded keyset pagination. The
        newest eligible observation represents an economic candidate identity.
        """
        if limit <= 0:
            return []
        limit = min(limit, 10_000)
        where = [
            "s.stage=?",
            "l.policy=?",
            "l.available_ms<=?",
            "json_extract(l.payload,'$.complete')=1",
            "json_extract(l.payload,'$.net_r') IS NOT NULL",
            "json_extract(l.payload,'$.exit_ms')<=?",
        ]
        filters = [stage, policy, asof_ms, asof_ms]
        if source is not None:
            where.append("json_extract(s.payload,'$.source')=?")
            filters.append(source)
        if schema_version is not None:
            where.append("s.schema_version=?")
            filters.append(schema_version)
        if confirmation_policy is not None:
            where.append("json_extract(s.payload,'$.signal.evidence.confirmation_policy')=?")
            filters.append(confirmation_policy)
        query = (
            "SELECT s.id,s.payload,l.available_ms,l.payload,"
            "coalesce(c.candidate_identity,s.signal_id),s.decision_ms "
            "FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id "
            "LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id WHERE " + " AND ".join(where)
        )
        result, seen, cursor = [], set(), None
        while len(result) < limit:
            page_where = " AND (s.decision_ms,s.id)<(?,?)" if cursor else ""
            params = filters + (list(cursor) if cursor else []) + [min(500, max(100, limit * 2))]
            rows = self.db.execute(
                query + page_where + " ORDER BY s.decision_ms DESC,s.id DESC LIMIT ?", params
            ).fetchall()
            if not rows:
                break
            for ident, payload, available_ms, label_payload, opportunity, decision_ms in rows:
                cursor = (decision_ms, ident)
                if opportunity in seen:
                    continue
                seen.add(opportunity)
                result.append(
                    {
                        "id": ident,
                        **json.loads(payload),
                        "candidate_identity": opportunity,
                        "label": json.loads(label_payload),
                        "label_available_ms": available_ms,
                    }
                )
                if len(result) >= limit:
                    break
            if len(rows) < params[-1]:
                break
        return list(reversed(result))

    def export(self, asof_ms, policy="prints-v1", stage="decision", source=None):
        from . import SCHEMA_VERSION

        rows = self.dataset(asof_ms, policy, stage, source=source, schema_version=SCHEMA_VERSION)
        if len({r["source"] for r in rows}) > 1:
            raise ValueError(
                "Multiple source methodologies: export with --source; never pool venues silently"
            )
        if not rows:
            raise ValueError("No complete resolved candidate labels; nothing fabricated")
        return self.write_dataset(rows)

    def write_dataset(self, rows):
        """Atomic immutable export shared by explicitly separated research tracks."""
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
