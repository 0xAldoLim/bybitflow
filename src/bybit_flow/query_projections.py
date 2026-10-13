"""Compact query projections; original labels remain the authoritative records."""


def migrate(db, kind):
    if kind == "research":
        source, target = "research_labels", "research_label_summaries"
        fields = "signal_id,available_ms,source_rowid,coverage_complete,family,outcome,horizon"
        values = (
            "{row}signal_id,{row}available_ms,{row}rowid,json_extract({row}payload,'$.coverage_complete'),"
            "json_extract({row}payload,'$.signal.family'),"
            "json_extract({row}payload,'$.extended_same_rules_outcome'),"
            "json_extract({row}payload,'$.best_observed_horizon')"
        )
        schema = (
            "signal_id TEXT PRIMARY KEY,available_ms INTEGER,source_rowid INTEGER,"
            "coverage_complete,family,outcome,horizon"
        )
        old_key = "signal_id=OLD.signal_id"
        match_key = "p.signal_id=s.signal_id"
        index = (
            "CREATE INDEX IF NOT EXISTS research_summary_recent ON research_label_summaries("
            "available_ms DESC,source_rowid,coverage_complete,family,outcome,horizon);"
        )
    elif kind == "retention":
        source, target = "ml_labels", "ml_label_retention"
        fields = "snapshot_id,policy,available_ms,exit_ms,complete,classification"
        values = (
            "{row}snapshot_id,{row}policy,{row}available_ms,"
            "json_extract({row}payload,'$.exit_ms'),json_extract({row}payload,'$.complete'),"
            "json_extract({row}payload,'$.classification')"
        )
        schema = (
            "snapshot_id TEXT,policy TEXT,available_ms INTEGER,exit_ms,complete,classification,"
            "PRIMARY KEY(snapshot_id,policy)"
        )
        old_key = "snapshot_id=OLD.snapshot_id AND policy=OLD.policy"
        match_key = "p.snapshot_id=s.snapshot_id AND p.policy=s.policy"
        index = ""
    else:
        raise ValueError("Unknown query projection")
    # Closed identifiers above; no operator-supplied SQL or new outcome policy.
    db.executescript(f"""
    CREATE TABLE IF NOT EXISTS {target}({schema});
    {index}
    CREATE TRIGGER IF NOT EXISTS {target}_insert AFTER INSERT ON {source} BEGIN
      INSERT OR REPLACE INTO {target}({fields}) VALUES({values.format(row="NEW.")});
    END;
    CREATE TRIGGER IF NOT EXISTS {target}_update AFTER UPDATE ON {source} BEGIN
      DELETE FROM {target} WHERE {old_key};
      INSERT OR REPLACE INTO {target}({fields}) VALUES({values.format(row="NEW.")});
    END;
    CREATE TRIGGER IF NOT EXISTS {target}_delete AFTER DELETE ON {source} BEGIN
      DELETE FROM {target} WHERE {old_key};
    END;
    """)
    marker = "query_projection_v1:" + kind
    with db:
        if db.execute("SELECT 1 FROM kv WHERE key=?", (marker,)).fetchone():
            return
        # One atomic backfill. Later source writes update the projection in the
        # same transaction, including REPLACE, UPDATE and DELETE operations.
        db.execute(
            f"INSERT OR IGNORE INTO {target}({fields}) SELECT {values.format(row='s.')} "
            f"FROM {source} s WHERE NOT EXISTS (SELECT 1 FROM {target} p WHERE {match_key})"
        )
        db.execute("INSERT OR IGNORE INTO kv VALUES(?, 'true')", (marker,))
