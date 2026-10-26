-- Offline schema fixture from pre-registry v2.2; contains no market data.
CREATE TABLE alert_members(
      signal_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, cluster_id TEXT NOT NULL,
      relationship TEXT NOT NULL, created_ms INTEGER NOT NULL);
CREATE TABLE candidate_identities(
      signal_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, candidate_identity TEXT NOT NULL,
      canonical_signal_id TEXT NOT NULL, evaluation_ms INTEGER NOT NULL);
CREATE TABLE experiments(id TEXT PRIMARY KEY, at_ms INTEGER, payload TEXT);
CREATE TABLE facts(id INTEGER PRIMARY KEY, asset TEXT, known_ms INTEGER,
            effective_ms INTEGER, expires_ms INTEGER, payload TEXT);
CREATE TABLE funnel_events(event_key TEXT PRIMARY KEY, at_ms INTEGER NOT NULL);
CREATE TABLE funnel_minutes(minute_ms INTEGER,metric TEXT,dimension TEXT,value TEXT,
        n INTEGER NOT NULL,last_ms INTEGER NOT NULL,signal_id TEXT,PRIMARY KEY(minute_ms,metric,dimension,value));
CREATE TABLE funnel_totals(metric TEXT, dimension TEXT, value TEXT, n INTEGER NOT NULL,
        last_ms INTEGER NOT NULL, signal_id TEXT, PRIMARY KEY(metric,dimension,value));
CREATE TABLE initial_alert_claims(
      fingerprint TEXT PRIMARY KEY, signal_id TEXT NOT NULL, cluster_id TEXT,
      claimed_ms INTEGER NOT NULL, delivery_status TEXT NOT NULL, message_id TEXT);
CREATE TABLE journal(id INTEGER PRIMARY KEY, at_ms INTEGER, payload TEXT);
CREATE TABLE kv(key TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE ml_history(
      id INTEGER PRIMARY KEY, at_ms INTEGER NOT NULL, model_id TEXT, action TEXT, payload TEXT);
CREATE TABLE ml_holdouts(
      start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL, experiment_id TEXT PRIMARY KEY, scope TEXT NOT NULL DEFAULT 'legacy-primary');
CREATE TABLE ml_label_retention(snapshot_id TEXT,policy TEXT,available_ms INTEGER,exit_ms,complete,classification,PRIMARY KEY(snapshot_id,policy));
CREATE TABLE ml_labels(
      snapshot_id TEXT NOT NULL REFERENCES ml_snapshots(id), policy TEXT NOT NULL,
      available_ms INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(snapshot_id,policy));
CREATE TABLE ml_models(
      id TEXT PRIMARY KEY, created_ms INTEGER NOT NULL, manifest TEXT NOT NULL, sha256 TEXT NOT NULL);
CREATE TABLE ml_predictions(
      snapshot_id TEXT NOT NULL, model_id TEXT NOT NULL, at_ms INTEGER NOT NULL,
      probability REAL NOT NULL, accepted INTEGER NOT NULL,
      PRIMARY KEY(snapshot_id,model_id));
CREATE TABLE ml_snapshots(
      id TEXT PRIMARY KEY, signal_id TEXT NOT NULL, stage TEXT NOT NULL,
      decision_ms INTEGER NOT NULL, schema_version TEXT NOT NULL, payload TEXT NOT NULL,
      UNIQUE(signal_id,stage,schema_version));
CREATE TABLE observations(signal_id TEXT PRIMARY KEY, status TEXT NOT NULL, next_ms INTEGER NOT NULL,
        payload TEXT NOT NULL);
CREATE TABLE outbox(key TEXT PRIMARY KEY, signal_id TEXT, status TEXT,
            payload TEXT, message_id TEXT, updated_ms INTEGER);
CREATE TABLE real_delivery_attempts(
      id INTEGER PRIMARY KEY, at_ms INTEGER NOT NULL, signal_id TEXT NOT NULL,
      symbol TEXT NOT NULL, horizon TEXT NOT NULL, fingerprint TEXT NOT NULL,
      status TEXT NOT NULL, relationship TEXT, message_id TEXT, origin_key TEXT UNIQUE);
CREATE TABLE research_checkpoints(signal_id TEXT, at_ms INTEGER, payload TEXT NOT NULL,
        PRIMARY KEY(signal_id,at_ms));
CREATE TABLE research_label_summaries(signal_id TEXT PRIMARY KEY,available_ms INTEGER,source_rowid INTEGER,coverage_complete,family,outcome,horizon);
CREATE TABLE research_labels(signal_id TEXT PRIMARY KEY, available_ms INTEGER, payload TEXT NOT NULL);
CREATE TABLE schema_version(version INTEGER PRIMARY KEY);
CREATE TABLE segment_packs(segment_id TEXT PRIMARY KEY, archive TEXT NOT NULL, bytes INTEGER NOT NULL);
CREATE TABLE segments(id TEXT PRIMARY KEY, at_ms INTEGER, payload TEXT);
CREATE TABLE setup_path_jobs(
      signal_id TEXT PRIMARY KEY,source TEXT,symbol TEXT,next_ms INTEGER,cursor_ms INTEGER,
      deadline_ms INTEGER,payload TEXT);
CREATE TABLE setup_path_labels(
      signal_id TEXT PRIMARY KEY,available_ms INTEGER,payload TEXT);
CREATE TABLE signal_origins(signal_id TEXT PRIMARY KEY, payload TEXT NOT NULL, sha256 TEXT NOT NULL);
CREATE TABLE signals(id TEXT PRIMARY KEY, symbol TEXT, created_ms INTEGER,
            state TEXT, payload TEXT NOT NULL);
CREATE TABLE storage_leases(id TEXT PRIMARY KEY, start_ms INTEGER, end_ms INTEGER, expires_ms INTEGER,
        reason TEXT NOT NULL);
CREATE TABLE swing_path_jobs(signal_id TEXT PRIMARY KEY,source TEXT,symbol TEXT,
        next_ms INTEGER,cursor_ms INTEGER,deadline_ms INTEGER,payload TEXT);
CREATE TABLE swing_path_labels(signal_id TEXT PRIMARY KEY,available_ms INTEGER,payload TEXT);
CREATE TABLE terminal_events(signal_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
            notification_status TEXT NOT NULL, attempts INTEGER DEFAULT 0, next_ms INTEGER DEFAULT 0);
CREATE TABLE transitions(id INTEGER PRIMARY KEY, signal_id TEXT,
            at_ms INTEGER, state TEXT, reason TEXT);
CREATE TABLE tv_inbox(event_id TEXT PRIMARY KEY, digest TEXT NOT NULL,
            received_ms INTEGER NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
            result TEXT);
CREATE TABLE universe(at_ms INTEGER, symbol TEXT, eligible INTEGER,
            payload TEXT, PRIMARY KEY(at_ms,symbol));
CREATE INDEX candidate_fingerprint ON candidate_identities(fingerprint,evaluation_ms);
CREATE INDEX funnel_event_time ON funnel_events(at_ms);
CREATE INDEX funnel_minute_time ON funnel_minutes(minute_ms);
CREATE INDEX ml_completeness_cohort ON ml_snapshots(
      stage,schema_version,decision_ms DESC,id);
CREATE INDEX ml_sequence_lookup ON ml_snapshots(
      json_extract(payload,'$.source'), json_extract(payload,'$.signal.symbol'),
      json_extract(payload,'$.signal.family'), json_extract(payload,'$.signal.direction'), decision_ms DESC
    ) WHERE stage='decision';
CREATE INDEX ml_snapshot_identity
      ON ml_snapshots(signal_id,stage,schema_version);
CREATE INDEX ml_snapshot_original_lookup
      ON ml_snapshots(signal_id,stage,decision_ms,id);
CREATE INDEX ml_snapshot_signal_time
      ON ml_snapshots(signal_id,stage,decision_ms);
CREATE INDEX ml_snapshot_stage_time
      ON ml_snapshots(stage,decision_ms,id);
CREATE INDEX observations_due ON observations(status,next_ms);
CREATE INDEX research_summary_recent ON research_label_summaries(available_ms DESC,source_rowid,coverage_complete,family,outcome,horizon);
CREATE INDEX segments_receipt_window ON segments(
      json_extract(payload,'$.max_receipt_ms'),json_extract(payload,'$.min_receipt_ms'));
CREATE INDEX setup_path_due ON setup_path_jobs(next_ms);
CREATE INDEX signals_active_lifecycle
          ON signals(CASE WHEN state='ALERTED' THEN 0 ELSE 1 END,created_ms DESC)
          WHERE state NOT IN ('INVALIDATED','EXPIRED','RESOLVED');
CREATE INDEX signals_recent ON signals(created_ms DESC);
CREATE INDEX swing_path_due ON swing_path_jobs(next_ms);
CREATE INDEX swing_path_label_time ON swing_path_labels(available_ms);
CREATE INDEX tv_pending ON tv_inbox(status,received_ms);
CREATE TRIGGER ml_label_retention_delete AFTER DELETE ON ml_labels BEGIN
      DELETE FROM ml_label_retention WHERE snapshot_id=OLD.snapshot_id AND policy=OLD.policy;
    END;
CREATE TRIGGER ml_label_retention_insert AFTER INSERT ON ml_labels BEGIN
      INSERT OR REPLACE INTO ml_label_retention(snapshot_id,policy,available_ms,exit_ms,complete,classification) VALUES(NEW.snapshot_id,NEW.policy,NEW.available_ms,json_extract(NEW.payload,'$.exit_ms'),json_extract(NEW.payload,'$.complete'),json_extract(NEW.payload,'$.classification'));
    END;
CREATE TRIGGER ml_label_retention_update AFTER UPDATE ON ml_labels BEGIN
      DELETE FROM ml_label_retention WHERE snapshot_id=OLD.snapshot_id AND policy=OLD.policy;
      INSERT OR REPLACE INTO ml_label_retention(snapshot_id,policy,available_ms,exit_ms,complete,classification) VALUES(NEW.snapshot_id,NEW.policy,NEW.available_ms,json_extract(NEW.payload,'$.exit_ms'),json_extract(NEW.payload,'$.complete'),json_extract(NEW.payload,'$.classification'));
    END;
CREATE VIEW ml_primary_labels AS
    SELECT CASE WHEN json_extract(l.payload,'$.complete')=1 THEN l.rowid ELSE coalesce((SELECT r.rowid FROM ml_labels r WHERE r.snapshot_id=l.snapshot_id AND r.policy='prints-recovered-v1' AND json_extract(r.payload,'$.complete')=1 AND json_extract(r.payload,'$.outcome_policy')='prints-v1' AND json_extract(r.payload,'$.certificate.status')='VERIFIED'),l.rowid) END AS rowid,
      CASE WHEN json_extract(l.payload,'$.complete')=1 THEN l.available_ms ELSE coalesce((SELECT r.available_ms FROM ml_labels r WHERE r.snapshot_id=l.snapshot_id AND r.policy='prints-recovered-v1' AND json_extract(r.payload,'$.complete')=1 AND json_extract(r.payload,'$.outcome_policy')='prints-v1' AND json_extract(r.payload,'$.certificate.status')='VERIFIED'),l.available_ms) END AS available_ms,
      CASE WHEN json_extract(l.payload,'$.complete')=1 THEN l.payload ELSE coalesce((SELECT r.payload FROM ml_labels r WHERE r.snapshot_id=l.snapshot_id AND r.policy='prints-recovered-v1' AND json_extract(r.payload,'$.complete')=1 AND json_extract(r.payload,'$.outcome_policy')='prints-v1' AND json_extract(r.payload,'$.certificate.status')='VERIFIED'),l.payload) END AS payload,
      l.snapshot_id,'prints-v1' AS policy FROM ml_labels l WHERE l.policy='prints-v1';
CREATE TRIGGER research_label_summaries_delete AFTER DELETE ON research_labels BEGIN
      DELETE FROM research_label_summaries WHERE signal_id=OLD.signal_id;
    END;
CREATE TRIGGER research_label_summaries_insert AFTER INSERT ON research_labels BEGIN
      INSERT OR REPLACE INTO research_label_summaries(signal_id,available_ms,source_rowid,coverage_complete,family,outcome,horizon) VALUES(NEW.signal_id,NEW.available_ms,NEW.rowid,json_extract(NEW.payload,'$.coverage_complete'),json_extract(NEW.payload,'$.signal.family'),json_extract(NEW.payload,'$.extended_same_rules_outcome'),json_extract(NEW.payload,'$.best_observed_horizon'));
    END;
CREATE TRIGGER research_label_summaries_update AFTER UPDATE ON research_labels BEGIN
      DELETE FROM research_label_summaries WHERE signal_id=OLD.signal_id;
      INSERT OR REPLACE INTO research_label_summaries(signal_id,available_ms,source_rowid,coverage_complete,family,outcome,horizon) VALUES(NEW.signal_id,NEW.available_ms,NEW.rowid,json_extract(NEW.payload,'$.coverage_complete'),json_extract(NEW.payload,'$.signal.family'),json_extract(NEW.payload,'$.extended_same_rules_outcome'),json_extract(NEW.payload,'$.best_observed_horizon'));
    END;
