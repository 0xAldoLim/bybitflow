import sqlite3, json, sys
con = sqlite3.connect("file:/app/data/research.sqlite?mode=ro", uri=True, timeout=60)
ids = [r[0] for r in con.execute("select distinct signal_id from outbox where key like '%:initial' and status='sent'")]
out = []
for i in ids:
    s = con.execute("select symbol, created_ms, state, payload from signals where id=?", (i,)).fetchone()
    if not s: continue
    tr = con.execute("select at_ms,state,reason from transitions where signal_id=? order by at_ms,id", (i,)).fetchall()
    te = con.execute("select payload from terminal_events where signal_id=?", (i,)).fetchone()
    lab = con.execute("select payload from research_labels where signal_id=?", (i,)).fetchone()
    spl = con.execute("select payload from setup_path_labels where signal_id=?", (i,)).fetchone()
    ob = con.execute("select key,status,updated_ms from outbox where signal_id=?", (i,)).fetchall()
    out.append(dict(id=i, symbol=s[0], created_ms=s[1], state=s[2], payload=json.loads(s[3]), transitions=tr,
        terminal=json.loads(te[0]) if te else None, label=json.loads(lab[0]) if lab else None,
        path=json.loads(spl[0]) if spl else None, outbox=ob))
json.dump(out, sys.stdout)
