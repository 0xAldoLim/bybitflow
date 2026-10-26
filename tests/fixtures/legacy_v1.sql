-- Offline core-only legacy schema; no market data.

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
