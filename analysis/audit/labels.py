import sqlite3, json, sys
con = sqlite3.connect("file:/app/data/research.sqlite?mode=ro", uri=True, timeout=60)
cur = con.execute("""select s.signal_id, s.stage, s.decision_ms, l.policy,
 json_extract(l.payload,'$.classification'), json_extract(l.payload,'$.outcome'), json_extract(l.payload,'$.net_r'), json_extract(l.payload,'$.complete'),
 json_extract(s.payload,'$.signal.quality'), json_extract(s.payload,'$.signal.family'), json_extract(s.payload,'$.signal.horizon_profile'),
 json_extract(s.payload,'$.signal.direction'), json_extract(s.payload,'$.signal.symbol'), json_extract(s.payload,'$.signal.risk.net_rr'),
 json_extract(s.payload,'$.signal.risk.accepted'), json_extract(s.payload,'$.signal.entry'), json_extract(s.payload,'$.signal.stop'), json_extract(s.payload,'$.signal.tp1'),
 json_extract(s.payload,'$.signal.evidence.flow_confirmed'), json_extract(s.payload,'$.signal.regime')
 from ml_labels l join ml_snapshots s on s.id=l.snapshot_id""")
out=[list(r) for r in cur]
json.dump(out, sys.stdout)
