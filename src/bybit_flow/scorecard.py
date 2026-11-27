"""Plain outcome record of the alerts actually delivered to Discord.

Read-only. It answers one question the score cannot answer by itself: of the
setups that were sent, how many reached the planned target, how many reached
the planned stop, and how many simply ran out of time, split by grade.
Observed prices only: no fills, fees or account results are implied.
"""

import json
import math
from collections import Counter, defaultdict

TIERS = ("SSS", "SS", "S", "A", "B", "C", "D", "E", "F")


def outcome(state, reason):
    if state == "RESOLVED":
        return "target"
    if state == "INVALIDATED":
        return "stop" if "stop" in (reason or "").lower() else "invalidated"
    if state == "EXPIRED":
        return "expired"
    return "open"


def _row(counts, planned_rr):
    resolved = counts["target"] + counts["stop"]
    rate = counts["target"] / resolved if resolved else None
    rr = sum(planned_rr) / len(planned_rr) if planned_rr else None
    # Hit rate needed for zero expectancy at the average planned reward:risk.
    breakeven = 1 / (1 + rr) if rr and rr > 0 else None
    interval = None
    if resolved:
        # Wilson 95% interval: honest about small samples.
        z, p = 1.96, rate
        centre = (p + z * z / (2 * resolved)) / (1 + z * z / resolved)
        half = z * math.sqrt(p * (1 - p) / resolved + z * z / (4 * resolved**2)) / (1 + z * z / resolved)
        interval = [round(max(0, centre - half), 3), round(min(1, centre + half), 3)]
    return dict(
        alerts=sum(counts.values()),
        **{k: counts[k] for k in ("target", "stop", "expired", "invalidated", "open")},
        target_rate_of_resolved=round(rate, 3) if rate is not None else None,
        target_rate_95ci=interval,
        average_planned_rr=round(rr, 2) if rr else None,
        breakeven_target_rate=round(breakeven, 3) if breakeven else None,
        gross_r_before_costs=round(counts["target"] * rr - counts["stop"], 1) if rr else None,
        verdict=verdict(interval, breakeven, resolved),
    )


def verdict(interval, breakeven, resolved):
    if not resolved or resolved < 30 or breakeven is None:
        return "INSUFFICIENT SAMPLE"
    if interval[1] < breakeven:
        return "BELOW BREAKEVEN"
    if interval[0] > breakeven:
        return "ABOVE BREAKEVEN"
    return "INCONCLUSIVE"


def scorecard(store, since_ms=0):
    """Group delivered initial alerts by grade, family and horizon."""
    groups = {name: defaultdict(Counter) for name in ("tier", "family", "horizon")}
    rr = {name: defaultdict(list) for name in groups}
    total, total_rr = Counter(), []
    # Start from the small outbox, then read each delivered signal by primary key.
    delivered = [
        row[0]
        for row in store.db.execute(
            "SELECT DISTINCT signal_id FROM outbox WHERE key LIKE '%:initial' AND status='sent'"
        )
    ]
    rows = []
    for ident in delivered:
        row = store.db.execute(
            "SELECT id,state,payload FROM signals WHERE id=? AND created_ms>=?", (ident, since_ms)
        ).fetchone()
        if row:
            rows.append(row)
    for signal_id, state, payload in rows:
        s = json.loads(payload)
        last = store.db.execute(
            "SELECT state,reason FROM transitions WHERE signal_id=? AND state IN "
            "('INVALIDATED','RESOLVED','EXPIRED') ORDER BY at_ms DESC,id DESC LIMIT 1",
            (signal_id,),
        ).fetchone()
        result = outcome(*(last or (state, "")))
        sign = 1 if s.get("direction") == "LONG" else -1
        risk = sign * (s["entry"] - s["stop"])
        planned = sign * (s["tp1"] - s["entry"]) / risk if risk > 0 else None
        keys = dict(
            tier=s.get("raw_tier") or "F",
            family=s.get("family") or "unknown",
            horizon=s.get("horizon_profile") or "LEGACY",
        )
        total[result] += 1
        if planned:
            total_rr.append(planned)
        for name, key in keys.items():
            groups[name][key][result] += 1
            if planned:
                rr[name][key].append(planned)
    order = {t: i for i, t in enumerate(TIERS)}
    return dict(
        basis="Alerts with a sent initial Discord receipt; observed public prices, not account fills",
        note="Expired alerts are neither wins nor losses. Target rate uses resolved alerts only.",
        since_ms=since_ms,
        all=_row(total, total_rr),
        **{
            "by_" + name: {
                key: _row(groups[name][key], rr[name][key])
                for key in sorted(groups[name], key=lambda k: (order.get(k, 99), k))
            }
            for name in groups
        },
    )


def render(report):
    lines = [report["basis"], report["note"], ""]
    header = f"{'':22s}{'alerts':>7}{'target':>8}{'stop':>6}{'expired':>9}{'hit%':>7}{'need%':>7}  verdict"

    def line(name, row):
        rate, need = row["target_rate_of_resolved"], row["breakeven_target_rate"]
        return (
            f"{name:22s}{row['alerts']:>7}{row['target']:>8}{row['stop']:>6}{row['expired']:>9}"
            f"{'' if rate is None else f'{100 * rate:.0f}':>7}{'' if need is None else f'{100 * need:.0f}':>7}"
            f"  {row['verdict']}"
        )

    lines += [header, line("ALL DELIVERED", report["all"])]
    for section in ("by_tier", "by_family", "by_horizon"):
        lines += ["", section.replace("_", " ").upper()]
        lines += [line(key, row) for key, row in report[section].items()]
    return "\n".join(lines)
