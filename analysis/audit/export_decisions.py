"""Export every decision-stage snapshot: recorded feature values and the frozen plan.

Run inside the desk container (read-only); writes JSON lines to stdout.
"""

import json
import sqlite3
import sys

con = sqlite3.connect("file:/app/data/research.sqlite?mode=ro", uri=True, timeout=60)
cursor = con.execute(
    """SELECT s.id, s.signal_id, s.decision_ms,
    json_extract(s.payload,'$.source'), json_extract(s.payload,'$.values'),
    json_extract(s.payload,'$.signal.symbol'), json_extract(s.payload,'$.signal.direction'),
    json_extract(s.payload,'$.signal.family'), json_extract(s.payload,'$.signal.horizon_profile'),
    json_extract(s.payload,'$.signal.entry'), json_extract(s.payload,'$.signal.stop'),
    json_extract(s.payload,'$.signal.tp1'), json_extract(s.payload,'$.signal.expected_hold_max'),
    json_extract(s.payload,'$.signal.quality'), json_extract(s.payload,'$.signal.raw_tier'),
    json_array_length(json_extract(s.payload,'$.signal.gates')),
    json_extract(s.payload,'$.signal.risk.accepted'),
    json_extract(s.payload,'$.signal.evidence.flow_confirmed'),
    json_extract(s.payload,'$.signal.setup_thesis_id'),
    json_extract(s.payload,'$.signal.evidence.exchange_symbol')
    FROM ml_snapshots s WHERE s.stage='decision' ORDER BY s.decision_ms"""
)
names = (
    "id signal_id decision_ms source values symbol direction family horizon entry stop tp1 hold "
    "quality tier gates risk_accepted flow_confirmed thesis exchange_symbol"
).split()
for row in cursor:
    record = dict(zip(names, row))
    record["values"] = json.loads(record["values"]) if record["values"] else {}
    sys.stdout.write(json.dumps(record, separators=(",", ":")) + "\n")
