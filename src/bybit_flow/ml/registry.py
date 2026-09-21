"""Immutable JSON registry. Approval never overrides statistical or operational gates."""

import json
import re

from ..storage import now_ms
from . import POLICY_VERSION, SCHEMA_VERSION
from .store import canonical, digest


def promotion_reasons(m):
    h, report = m["report"]["holdout"], m["report"]
    reasons = []
    if m.get("policy_version") != POLICY_VERSION or m.get("feature_schema_version") != SCHEMA_VERSION:
        reasons.append("incompatible approved policy/schema")
    if not m.get("real_data") or not m.get("verified_costs"):
        reasons.append("real data and verified cost evidence required; assumed-cost labels are research only")
    if m.get("code_dirty") or m.get("code_commit") == "unavailable":
        reasons.append("clean reproducible code commit required")
    if m.get("universe_scope") != ["recorded-membership"]:
        reasons.append("point-in-time eligible universe evidence required")
    if h["n"] < 200 or h["effective_samples"] < 78:
        reasons.append("SSS requires >=200 selected holdout outcomes and >=78 market-week clusters")
    if not h.get("ev_interval") or h["ev_interval"][0] <= 0:
        reasons.append("conservative cost-adjusted expected value not positive")
    if h.get("max_drawdown_r", float("inf")) > 20 or h.get("tail_loss_5pct_r", -100) < -1.5:
        reasons.append("drawdown above 20R or worst-five-percent mean below -1.5R")
    if not report.get("incremental_ev_interval") or report["incremental_ev_interval"][0] <= 0:
        reasons.append("incremental value over frozen deterministic baseline unproven")
    if (
        report["all_holdout"].get("brier") is None
        or report["constant"].get("brier") is None
        or report["all_holdout"]["brier"] >= report["constant"]["brier"]
    ):
        reasons.append("Brier score does not beat base-rate classifier")
    folds = report.get("walk_forward", [])
    if len(folds) < 3 or any(f.get("test", {}).get("net_expectancy_r", -1) <= 0 for f in folds):
        reasons.append("three stable chronological folds required")
    regimes = [v for k, v in report.get("breakdown", {}).items() if k.startswith("regime:") and v["n"] >= 30]
    if len(regimes) < 2 or any(r["net_expectancy_r"] <= 0 for r in regimes):
        reasons.append("multiple supported regimes without negative subgroup expectancy required")
    if m["periods"]["calibration"]["n"] < 200:
        reasons.append("at least 200 independent calibration observations required")
    return reasons


class Registry:
    def __init__(self, store):
        self.store, self.db = store, store.db

    def register(self, manifest):
        payload, checksum = canonical(manifest), digest(manifest)
        if len(payload) > 20_000_000 or not re.fullmatch(r"[a-f0-9]{32}", manifest["id"]):
            raise ValueError("Invalid model identifier or excessive artifact size")
        with self.db:
            self.db.execute(
                "INSERT INTO ml_models VALUES(?,?,?,?)",
                (manifest["id"], manifest["created_ms"], payload, checksum),
            )
            self.event(manifest["id"], "registered", {"reasons": promotion_reasons(manifest)})
        directory = self.store.root / "ml" / "models" / manifest["id"]
        directory.mkdir(parents=True, exist_ok=False)
        (directory / "model.json").write_text(payload)
        (directory / "MODEL_CARD.md").write_text(
            "# Research model " + manifest["id"] + "\n\n"
            "Status: challenger, NOT approved. Model attribution is not causation.\n\n"
            "Evidence, provenance, limitations and metrics:\n\n```json\n"
            + json.dumps({k: v for k, v in manifest.items() if k != "model"}, indent=2)
            + "\n```\n"
        )

    def event(self, ident, action, payload):
        self.db.execute(
            "INSERT INTO ml_history(at_ms,model_id,action,payload) VALUES(?,?,?,?)",
            (now_ms(), ident, action, canonical(payload)),
        )

    def get(self, ident):
        row = self.db.execute("SELECT manifest,sha256 FROM ml_models WHERE id=?", (ident,)).fetchone()
        if not row:
            raise ValueError("Unknown registry model")
        result = json.loads(row[0])
        if digest(result) != row[1]:
            raise ValueError("Model integrity failure; abstaining")
        return result

    def champion(self):
        ident = self.store.get("ml_champion")
        if not ident or self.is_degraded(ident):
            return None
        model = self.get(ident)
        approved = self.db.execute(
            "SELECT 1 FROM ml_history WHERE model_id=? AND action='approved'", (ident,)
        ).fetchone()
        if not approved or promotion_reasons(model):
            return None
        return model

    def is_degraded(self, ident):
        return bool(
            self.db.execute(
                "SELECT 1 FROM ml_history WHERE model_id=? AND action='degraded'", (ident,)
            ).fetchone()
        )

    def promote(self, ident, reviewer):
        if not reviewer.strip():
            raise ValueError("Named manual approval required")
        m = self.get(ident)
        reasons = promotion_reasons(m)
        if self.is_degraded(ident):
            reasons.append("degraded artifacts cannot be reapproved; train a new challenger")
        current = self.champion()
        if current and current["id"] != ident:
            # A challenger cannot be compared against a champion on previously seen outcomes.
            if m["periods"]["holdout"]["start"] <= current["periods"]["holdout"]["end"]:
                reasons.append("challenger holdout is not unseen by champion")
            # Paired frozen champion comparisons are required, not an arbitrary win-rate improvement.
            comparison = m["report"].get("champion_comparison", {})
            if (
                comparison.get("model_id") != current["id"]
                or not comparison.get("incremental_interval")
                or comparison["incremental_interval"][0] <= 0
            ):
                reasons.append("paired unseen champion comparison required")
        with self.db:
            self.event(ident, "rejected" if reasons else "approved", dict(reviewer=reviewer, reasons=reasons))
        if reasons:
            raise ValueError("; ".join(reasons))
        import uuid

        directory = self.store.root / "ml" / "models" / ident
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"DEPLOYMENT-{uuid.uuid4().hex}.md").write_text(
            "# Approved research model deployment\n\n"
            + canonical(
                dict(
                    model_id=ident,
                    reviewer=reviewer,
                    at_ms=now_ms(),
                    manifest_sha256=digest(m),
                    validation_period=m["periods"],
                    performance=m["report"]["holdout"],
                    limitations="Historical simulated outcomes; future edge is not guaranteed. Alerts only.",
                )
            )
        )
        self.store.put("ml_previous_champion", self.store.get("ml_champion"))
        self.store.put("ml_champion", ident)

    def degrade(self, ident, reasons):
        self.store.put("ml_degraded", dict(model_id=ident, at_ms=now_ms(), reasons=reasons))
        with self.db:
            self.event(ident, "degraded", {"reasons": reasons})

    def rollback(self, reviewer):
        ident = self.store.get("ml_previous_champion")
        if not ident:
            raise ValueError("No previously approved champion available")
        if self.is_degraded(ident):
            raise ValueError("Previous champion is degraded; remain abstaining")
        approved = self.db.execute(
            "SELECT 1 FROM ml_history WHERE model_id=? AND action='approved'", (ident,)
        ).fetchone()
        if not reviewer.strip() or not approved or promotion_reasons(self.get(ident)):
            raise ValueError("Rollback requires named reviewer and previously qualified artifact")
        self.store.put("ml_previous_champion", self.store.get("ml_champion"))
        self.store.put("ml_champion", ident)
        with self.db:
            self.event(ident, "rollback", {"reviewer": reviewer})

    def cached_summary(self):
        """Operator views use the worker's summary, never a full label scan."""
        cached = self.store.get("ml_summary_cache", {})
        return cached | dict(
            summary_status="CACHED" if cached else "AWAITING_WORKER_SUMMARY",
            models=cached.get("models", []),
            champion=self.store.get("ml_champion"),
            pipeline=self.store.get("ml_pipeline"),
            monitoring=self.store.get("ml_monitor"),
            cycle=self.store.get("ml_cycle"),
            horizon_model=self.store.get("horizon_model"),
            score_profile_research=self.store.get("score_profile_research"),
            swing_stop_study=self.store.get("swing_stop_study"),
            confirmation_policy_research=self.store.get("confirmation_policy_research"),
            replay_progress=self.store.get("ml_replay_progress"),
            recording_audit=self.store.get("ml_recordings"),
            learning_note="Training uses complete independent outcomes and chronological validation; no automatic promotion.",
        )

    def summary(self):
        models = []
        for row in self.db.execute("SELECT id FROM ml_models ORDER BY created_ms DESC LIMIT 10"):
            m = self.get(row[0])
            models.append(
                {k: v for k, v in m.items() if k != "model"}
                | {
                    "importance": m["model"]["importance"],
                    "thresholds": m["model"]["thresholds"],
                    "promotion_reasons": promotion_reasons(m),
                }
            )
        return dict(
            pipeline=self.store.get("ml_pipeline"),
            score_profile_research=self.store.get("score_profile_research"),
            swing_stop_study=self.store.get("swing_stop_study"),
            confirmation_policy_research=self.store.get("confirmation_policy_research"),
            withdrawal_research=self.store.get("withdrawal_research"),
            horizon_model=self.store.get(
                "horizon_model",
                {
                    "status": "INSUFFICIENT_EVIDENCE",
                    "samples": 0,
                    "production_enabled": False,
                    "recommended_horizon": None,
                },
            ),
            champion=self.store.get("ml_champion"),
            models=models,
            drift=self.store.get("ml_degraded"),
            monitoring=self.store.get("ml_monitor"),
            replay_progress=self.store.get("ml_replay_progress"),
            recording_audit=self.store.get("ml_recordings"),
            learning_note="Requires real resolved decisions and chronological train/calibration/validation/holdout partitions (at least 500 complete labels). No automatic model promotion.",
            cycle=self.store.get("ml_cycle"),
            snapshots=self.db.execute("SELECT COUNT(*) FROM ml_snapshots").fetchone()[0],
            decision_snapshots=self.db.execute(
                "SELECT COUNT(*) FROM ml_snapshots WHERE stage='decision'"
            ).fetchone()[0],
            labels=self.db.execute("SELECT COUNT(*) FROM ml_labels").fetchone()[0],
            complete_labels=self.db.execute(
                "SELECT COUNT(*) FROM ml_labels WHERE json_extract(payload,'$.complete')=1"
            ).fetchone()[0],
            sequence_outcomes=self.db.execute(
                "SELECT COUNT(DISTINCT coalesce(c.candidate_identity,s.signal_id)) FROM ml_snapshots s JOIN ml_labels l ON l.snapshot_id=s.id LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
                "WHERE s.stage='decision' AND l.policy='prints-v1' AND json_extract(l.payload,'$.complete')=1 "
                "AND json_array_length(s.payload,'$.sequence')=16"
            ).fetchone()[0],
            history=[dict(r) for r in self.db.execute("SELECT * FROM ml_history ORDER BY id DESC LIMIT 100")],
        )
