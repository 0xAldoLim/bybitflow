import sqlite3
con = sqlite3.connect("file:/app/data/research.sqlite?mode=ro", uri=True, timeout=60)
try:
    for r in con.execute("select name, sum(pgsize)/1e6 mb, sum(ncell) from dbstat group by name order by 2 desc limit 25"):
        print(f"{r[0]:45s} {r[1]:10.1f} MB  cells {r[2]}")
except Exception as e:
    print("dbstat unavailable", e)
    for t in ("ml_snapshots","ml_labels","signals","journal","observations","funnel_events","segments","ml_history","evidence_power_paths","policy_opportunities","candidate_attempts","operational_samples"):
        r=con.execute(f"select count(*), max(rowid) from {t}").fetchone(); print(t, r)
