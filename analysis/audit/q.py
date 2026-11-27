import sqlite3, sys, json
con = sqlite3.connect("file:/app/data/research.sqlite?mode=ro", uri=True, timeout=30)
con.row_factory = sqlite3.Row
sql = sys.stdin.read()
for stmt in [s for s in sql.split(";;") if s.strip()]:
    try:
        cur = con.execute(stmt)
        rows = cur.fetchmany(400)
        if rows:
            print(" | ".join(rows[0].keys()))
        for r in rows:
            print(" | ".join(str(x)[:300] for x in r))
    except Exception as e:
        print("ERR", e, "::", stmt[:80])
    print("----")
