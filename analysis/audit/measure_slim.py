import sqlite3
con = sqlite3.connect("file:/app/data/research.sqlite?mode=ro", uri=True, timeout=60)
for r in con.execute("select stage, count(*), sum(length(cast(payload as blob)))/1e9 from ml_snapshots group by 1"): print("snap", r)
print("labelled generation:", con.execute("select count(*) from ml_labels l join ml_snapshots s on s.id=l.snapshot_id where s.stage='generation'").fetchone())
for t in ("ml_predictions", "ml_holdouts", "ml_label_retention", "ml_history"):
    cols=[c[1] for c in con.execute(f"pragma table_info({t})")]
    print(t, cols)
    if "snapshot_id" in cols:
        print("  refs to generation:", con.execute(f"select count(*) from {t} x join ml_snapshots s on s.id=x.snapshot_id where s.stage='generation'").fetchone())
print("fk list labels", list(con.execute("pragma foreign_key_list(ml_labels)")))
print("signal_origins cols", [c[1] for c in con.execute("pragma table_info(signal_origins)")])
print("triggers", [r[0] for r in con.execute("select name from sqlite_master where type='trigger'")])
print("pragmas", [con.execute(f"pragma {p}").fetchone() for p in ("journal_mode","user_version","application_id","page_size","auto_vacuum","foreign_keys")])
