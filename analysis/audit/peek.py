import sqlite3, json
con = sqlite3.connect("file:/app/data/research.sqlite?mode=ro", uri=True, timeout=60)
r = con.execute("select s.payload, l.payload, l.policy from ml_snapshots s join ml_labels l on l.snapshot_id=s.id where s.stage='decision' and l.policy='ohlc-path-v1' order by s.rowid desc limit 1").fetchone()
s = json.loads(r[0]); l = json.loads(r[1])
print({k: (v if not isinstance(v,(dict,list)) else type(v).__name__+str(len(v))) for k,v in s.items()})
print('features', len(s.get('features',{})), list(s.get('features',{}).items())[:60])
sig = s.get('signal',{}); print({k: (v if not isinstance(v,(dict,list)) else type(v).__name__+str(len(v))) for k,v in sig.items()})
print(json.dumps(l)[:2500])
print({k: len(json.dumps(v)) for k,v in s.items()})
