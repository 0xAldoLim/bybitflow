"""Read-only deployment/rollback continuity snapshot; never migrates a database."""

import json
import sqlite3
import sys
from pathlib import Path

FIELDS = (
    "id",
    "symbol",
    "source",
    "family",
    "direction",
    "created_ms",
    "entry",
    "zone",
    "stop",
    "tp1",
    "tp2",
    "expires_ms",
    "trigger_expires_ms",
    "holding_deadline_ms",
    "horizon_profile",
    "version",
    "feature_schema_version",
)


def capture(root, ids=()):
    db = sqlite3.connect(
        (Path(root) / "research.sqlite").resolve().as_uri() + "?mode=ro", uri=True, timeout=5
    )
    try:
        active = {}
        where = "state NOT IN ('EXPIRED','INVALIDATED','RESOLVED')"
        ids = list(ids)
        if ids:
            where += " OR id IN (" + ",".join("?" for _ in ids) + ")"
        for (raw,) in db.execute("SELECT payload FROM signals WHERE " + where, ids):
            signal = json.loads(raw)
            active[signal["id"]] = {k: signal.get(k) for k in FIELDS}
        sent = [
            list(row)
            for row in db.execute(
                "SELECT key,message_id FROM outbox WHERE status='sent' AND key LIKE '%:initial' ORDER BY key"
            )
        ]
        counts = {
            table: db.execute("SELECT count(*) FROM " + table).fetchone()[0]
            for table in ("ml_snapshots", "ml_labels", "ml_models")
        }
        return dict(active=active, sent_initial=sent, counts=counts)
    finally:
        db.close()


def validate(before, after):
    if any(after["active"].get(ident) != plan for ident, plan in before["active"].items()):
        raise ValueError("Original setup plan changed or disappeared")
    if any(receipt not in after["sent_initial"] for receipt in before["sent_initial"]):
        raise ValueError("Original sent receipt disappeared or changed")
    if any(after["counts"].get(k, 0) < n for k, n in before["counts"].items()):
        raise ValueError("Permanent evidence count decreased")
    return True


if __name__ == "__main__":
    from bybit_flow.config import Settings

    print(
        json.dumps(
            capture(
                Settings().data_dir, [x for x in sys.argv[1].split(",") if x] if len(sys.argv) > 1 else ()
            )
        )
    )
