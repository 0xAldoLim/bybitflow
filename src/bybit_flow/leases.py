"""Short, renewable range leases shared by replay and storage maintenance.

The existing five-column schema is retained. JSON reason metadata provides owner,
heartbeat and cursor provenance without rewriting older lease rows.
"""

import json
import os
import socket
import uuid

from .storage import now_ms

TTL_MS = 180_000


def overlaps(store, start, end, at_ms=None, exclude=None):
    return (
        store.db.execute(
            "SELECT id FROM storage_leases WHERE expires_ms>? AND start_ms<=? AND end_ms>=? "
            "AND id!=? LIMIT 1",
            (now_ms() if at_ms is None else at_ms, end, start, exclude or ""),
        ).fetchone()
        is not None
    )


class RangeLease:
    def __init__(self, store, start, end, reason, exclusive=False):
        self.store, self.start, self.end = store, start, end
        self.reason, self.exclusive = reason, exclusive
        self.id, self.last_beat = uuid.uuid4().hex, 0

    def heartbeat(self, start=None, force=False):
        now = now_ms()
        if not force and now - self.last_beat < 30_000:
            return
        proposed = self.start if start is None else max(self.start, start)
        db = self.store.db
        db.execute("BEGIN IMMEDIATE")
        try:
            # Read/read overlap is safe. Destructive operations exclude all readers.
            rows = db.execute(
                "SELECT id,reason FROM storage_leases WHERE expires_ms>? AND start_ms<=? "
                "AND end_ms>=? AND id!=?",
                (now, self.end, proposed, self.id),
            ).fetchall()
            for row in rows:
                try:
                    other = json.loads(row[1])
                except (ValueError, TypeError):
                    other = {}
                if self.exclusive or (isinstance(other, dict) and other.get("exclusive")):
                    raise BlockingIOError("Overlapping storage operation in progress")
            metadata = dict(
                owner=f"{socket.gethostname()}:{os.getpid()}",
                reason=self.reason,
                heartbeat_ms=now,
                cursor_ms=proposed,
                exclusive=self.exclusive,
            )
            db.execute(
                "INSERT OR REPLACE INTO storage_leases VALUES(?,?,?,?,?)",
                (self.id, proposed, self.end, now + TTL_MS, json.dumps(metadata)),
            )
            db.commit()
        except BaseException:
            db.rollback()
            raise
        self.start, self.last_beat = proposed, now

    def __enter__(self):
        self.heartbeat(force=True)
        return self

    def __exit__(self, *args):
        with self.store.db:
            self.store.db.execute("DELETE FROM storage_leases WHERE id=?", (self.id,))


def status(store, at_ms=None):
    now = now_ms() if at_ms is None else at_ms
    rows = [dict(r) for r in store.db.execute("SELECT * FROM storage_leases")]
    return dict(
        active=[r for r in rows if r["expires_ms"] > now], stale=[r for r in rows if r["expires_ms"] <= now]
    )


def reap(store):
    """Archive expired provenance atomically; never remove a live heartbeat."""
    now = now_ms()
    with store.db:
        for row in store.db.execute("SELECT * FROM storage_leases WHERE expires_ms<=?", (now,)).fetchall():
            store.db.execute(
                "INSERT OR IGNORE INTO kv VALUES(?,?)", ("expired_lease:" + row["id"], json.dumps(dict(row)))
            )
        store.db.execute("DELETE FROM storage_leases WHERE expires_ms<=?", (now,))
