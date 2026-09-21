"""Bounded, chronological shadow research. Never edits production weights/stops."""

import json
from statistics import mean

from ..scoring import WEIGHTS
from .store import canonical, digest


def run(store, asof):
    from .confirmation_research import run as confirmation_study

    confirmation_study(store, asof)
    rows, seen = [], set()
    boundary = store.get("score_profile_holdout_end", -1)
    for row in store.db.execute(
        "SELECT s.signal_id,s.decision_ms,s.id,l.available_ms,c.candidate_identity "
        "FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id "
        "LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
        "WHERE s.stage='decision' AND l.policy='prints-v1' AND l.available_ms<=? "
        "AND s.decision_ms>? ORDER BY s.decision_ms,s.id",
        (asof, boundary),
    ).fetchall():
        identity = row[4] or row[0]
        if identity in seen:
            continue
        seen.add(identity)
        snapshot = json.loads(
            store.db.execute("SELECT payload FROM ml_snapshots WHERE id=?", (row[2],)).fetchone()[0]
        )
        outcome = json.loads(
            store.db.execute(
                "SELECT payload FROM ml_labels WHERE snapshot_id=? AND policy='prints-v1'", (row[2],)
            ).fetchone()[0]
        )
        components = snapshot["signal"]["evidence"].get("score_components", {})
        if not outcome.get("complete") or outcome.get("net_r") is None or set(components) != set(WEIGHTS):
            continue
        rows.append(
            dict(
                id=row[0],
                decision_ms=row[1],
                available_ms=row[3],
                net_r=outcome["net_r"],
                fractions={k: v["earned"] / v["weight"] if v["weight"] else 0 for k, v in components.items()},
            )
        )
    report = dict(
        status="INSUFFICIENT_EVIDENCE",
        samples=len(rows),
        minimum=500,
        current_score_profile=dict(WEIGHTS),
        challenger_score_profile=None,
        promotion_status="RESEARCH_ONLY",
        production_enabled=False,
    )
    if len(rows) >= 500:
        n = len(rows)
        a, b, c = int(n * 0.4), int(n * 0.6), int(n * 0.8)
        embargo = 86_400_000
        train = [r for r in rows[:a] if r["available_ms"] < rows[a]["decision_ms"] - embargo]
        calibration = [r for r in rows[a:b] if r["available_ms"] < rows[b]["decision_ms"] - embargo]
        validation = [r for r in rows[b:c] if r["available_ms"] < rows[c]["decision_ms"] - embargo]
        holdout = rows[c:]
        report["partition_sizes"] = list(map(len, (train, calibration, validation, holdout)))
        if min(report["partition_sizes"]) >= 30:
            profiles = [dict(WEIGHTS)]
            # Two small regularized alternatives, rather than parameter mining.
            for destination in ("orderflow", "execution"):
                challenger = dict(WEIGHTS)
                challenger["structure"] -= 2.5
                challenger[destination] += 2.5
                profiles.append(challenger)

            def score(row, profile):
                return sum(profile[k] * row["fractions"][k] for k in profile)

            thresholds = [
                sorted(score(r, p) for r in calibration)[int(len(calibration) * 0.8)] for p in profiles
            ]

            def assess(part, profile, threshold):
                selected = [r for r in part if score(r, profile) >= threshold]
                return dict(
                    n=len(selected),
                    mean_net_r=mean(r["net_r"] for r in selected) if selected else None,
                    market_weeks=len({r["decision_ms"] // 604_800_000 for r in selected}),
                )

            results = [assess(validation, p, t) for p, t in zip(profiles, thresholds)]
            best = max(
                range(len(profiles)), key=lambda i: (results[i]["mean_net_r"] or -100) - (0.02 if i else 0)
            )
            # Reserve once before inspecting holdout. Future studies use only newer cohorts.
            store.put("score_profile_holdout_end", rows[-1]["decision_ms"])
            report.update(
                status="SHADOW_EVALUATED",
                challenger_score_profile=profiles[best],
                validation_result=results,
                holdout_result=assess(holdout, profiles[best], thresholds[best]),
                baseline_holdout=assess(holdout, profiles[0], thresholds[0]),
                weight_differences={k: profiles[best][k] - WEIGHTS[k] for k in WEIGHTS},
                incremental_net_r_interval=None,
                limitation="Descriptive cost-adjusted print outcomes; no validated paired interval or promotion",
                partitions=[[r["id"] for r in part] for part in (train, calibration, validation, holdout)],
            )
            directory = store.root / "ml" / "score-profile-research"
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / (digest(report) + ".json")
            target.write_text(canonical(report), encoding="utf-8")
            report["artifact"] = str(target)
    if report["status"] != "INSUFFICIENT_EVIDENCE" or not store.get("score_profile_research", {}).get(
        "artifact"
    ):
        store.put("score_profile_research", report)
    swing = []
    withdrawals = []
    for row in store.db.execute("SELECT payload FROM research_labels WHERE available_ms<=?", (asof,)):
        p = json.loads(row[0])
        if p["signal"].get("horizon_profile") == "SWING":
            swing.append(p)
        if p.get("withdrawal_research"):
            withdrawals.append(p["withdrawal_research"])
    study = dict(
        status="INSUFFICIENT_EVIDENCE",
        samples=len(swing),
        coverage_complete=sum(bool(p.get("coverage_complete")) for p in swing),
        late_recoveries=sum(bool(p.get("late_target_hit")) for p in swing),
        original_policy="Frozen structural stop plus setup ATR buffer",
        challenger_policy="Decision-time structural/noise buffers; requires independently reserved complete path data",
        holdout_result=None,
        decision="Current Swing stop policy retained.",
        reason="Post-terminal aggregate recovery is insufficient to simulate alternative stops and costs without path ambiguity",
    )
    from ..path_research import summary as path_summary

    study["forward_path_dataset"] = path_summary(store, asof)
    store.put("swing_stop_study", study)
    store.put(
        "withdrawal_research",
        dict(
            samples=len(withdrawals),
            production_ml=False,
            status="INSUFFICIENT_EVIDENCE",
            holdout_result=None,
            winners_prematurely_withdrawn=sum(
                p["classification"] == "WITHDRAWAL_TOO_EARLY" for p in withdrawals
            ),
            stops_avoided=sum(p["classification"] == "WITHDRAWAL_SAVED_STOP" for p in withdrawals),
        ),
    )
    return report
