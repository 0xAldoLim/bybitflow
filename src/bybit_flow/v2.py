"""Product cutover identity and compatibility; no alternate strategy pipeline."""

import hashlib
import json
import os
import subprocess
from copy import deepcopy
from pathlib import Path
from time import time_ns

VERSION = "2.0.0"
SCHEMA = "candidate-v20"
SCORE_PROFILE = "bybitflow-v2-evidence-1"
CUTOVER_KEY = "bybitflow_v2_cutover"
ROOT = Path(__file__).resolve().parents[2]

# Lifecycle state, observed outcomes and coverage can advance. The original plan cannot.
FROZEN_FIELDS = (
    "id",
    "symbol",
    "source",
    "created_ms",
    "version",
    "feature_schema_version",
    "entry",
    "zone",
    "stop",
    "tp1",
    "tp2",
    "expires_ms",
    "trigger_expires_ms",
    "holding_deadline_ms",
    "primary_tracking_deadline",
    "invalidation",
    "reason",
    "regime",
    "direction",
    "family",
    "horizon_profile",
    "context_timeframe",
    "setup_timeframe",
    "execution_timeframe",
    "expected_hold_min",
    "expected_hold_max",
    "lifecycle_version",
    "setup_thesis_id",
    "session",
    "entry_session",
    "quality",
    "raw_tier",
    "final_tier",
    "qualification",
    "risk",
    "gates",
    "calibrated_probability",
    "probability_uncertainty",
    "expected_net_r",
    "expected_net_r_uncertainty",
    "validation_status",
    "model_version",
    "data_coverage",
)
FROZEN_EVIDENCE = (
    "score_components",
    "score_profile",
    "score_reasons",
    "orderflow_score_basis",
    "location_quality",
    "production_policies",
    "confirmation_policy",
    "confirmation",
    "structural_trigger",
    "trigger_bar_end",
    "execution_window_ms",
    "execution_window_end_ms",
    "level_policy",
    "stop_plan",
    "target_method",
    "tp2_method",
    "flow_confirmation_mode",
    "flow_substitution",
    "market_alignment",
    "v8_gate",
    "ml",
)


def is_legacy(signal, epoch):
    return bool(epoch and signal.created_ms < epoch["started_ms"] and signal.feature_schema_version != SCHEMA)


def preserve(signal, saved, epoch):
    """Protect the saved creation plan before any scoring/snapshot write can occur."""
    if (
        not epoch
        or saved["created_ms"] >= epoch["started_ms"]
        or saved.get("feature_schema_version") == SCHEMA
    ):
        return signal
    for name in FROZEN_FIELDS:
        if name in saved:
            value = deepcopy(saved[name])
            if (
                name in {"zone", "probability_uncertainty", "expected_net_r_uncertainty"}
                and value is not None
            ):
                value = tuple(value)
            setattr(signal, name, value)
    for name in FROZEN_EVIDENCE:
        if name in saved.get("evidence", {}):
            signal.evidence[name] = deepcopy(saved["evidence"][name])
        else:
            signal.evidence.pop(name, None)
    for name in ("v2", "trade_size_state", "event_rotation_state"):
        signal.evidence.pop(name, None)
    return signal


def code_commit():
    configured = os.environ.get("FLOW_CODE_COMMIT")
    if configured and configured != "unknown":
        return configured
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            timeout=5,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def initialize(store, at_ms, *, commit=None, documents=None):
    existing = store.get(CUTOVER_KEY)
    if existing:
        return existing
    documents = documents or Path.cwd() / "docs" / "v2"
    if not documents.exists():
        documents = ROOT / "docs" / "v2"
    if documents.exists():
        research = dict(
            research_manifest_hash=hashlib.sha256(
                (documents / "INSILICO_SOURCE_MANIFEST.md").read_bytes()
            ).hexdigest(),
            research_ledger_hash=hashlib.sha256(
                (documents / "INSILICO_RESEARCH_LEDGER.md").read_bytes()
            ).hexdigest(),
        )
    else:
        from importlib.resources import files

        research = json.loads(files("bybit_flow").joinpath("research_v2.json").read_text(encoding="utf-8"))
    marker = dict(
        started_ms=at_ms,
        code_commit=commit or code_commit(),
        candidate_schema=SCHEMA,
        score_profile=SCORE_PROFILE,
        **research,
    )

    with store.db:
        store.db.execute(
            "INSERT OR IGNORE INTO kv(key,payload) VALUES(?,?)", (CUTOVER_KEY, json.dumps(marker))
        )
    return store.get(CUTOVER_KEY)


def overlaps_legacy(signal, rows, epoch):
    """A cutover cannot recreate an original still-active structural opportunity."""
    if not epoch:
        return False
    for row in rows:
        if row["created_ms"] >= epoch["started_ms"] or row.get("feature_schema_version") == SCHEMA:
            continue
        if all(
            row.get(key) == getattr(signal, key)
            for key in (
                "source",
                "symbol",
                "direction",
                "family",
                "horizon_profile",
            )
        ) and row.get("evidence", {}).get("trigger_bar_end") == signal.evidence.get("trigger_bar_end"):
            return True
    return False


def status(store):
    from collections import Counter

    rows = store.db.execute(
        "SELECT payload FROM ml_snapshots WHERE stage='decision' AND schema_version=? ORDER BY decision_ms DESC LIMIT 500",
        (SCHEMA,),
    ).fetchall()
    signals = [json.loads(row[0])["signal"] for row in rows]
    asof = time_ns() // 1_000_000
    return dict(
        version=VERSION,
        candidate_schema=SCHEMA,
        score_profile=SCORE_PROFILE,
        cutover=store.get(CUTOVER_KEY),
        new_v20_count=store.db.execute(
            "SELECT count(*) FROM ml_snapshots WHERE stage='generation' AND schema_version=?", (SCHEMA,)
        ).fetchone()[0],
        v20_decisions=store.db.execute(
            "SELECT count(*) FROM ml_snapshots WHERE stage='decision' AND schema_version=?", (SCHEMA,)
        ).fetchone()[0],
        complete_v20_outcomes=store.db.execute(
            "SELECT count(DISTINCT coalesce(c.candidate_identity,s.signal_id)) FROM ml_snapshots s "
            "JOIN ml_labels l ON l.snapshot_id=s.id LEFT JOIN candidate_identities c ON c.signal_id=s.signal_id "
            "WHERE s.schema_version=? AND s.stage='decision' AND l.policy='prints-v1' "
            "AND json_extract(l.payload,'$.complete')=1 AND json_extract(l.payload,'$.net_r') IS NOT NULL "
            "AND l.available_ms<=? AND coalesce(json_extract(l.payload,'$.exit_ms'),l.available_ms)<=?",
            (SCHEMA, asof, asof),
        ).fetchone()[0],
        recent_decision_sample=len(signals),
        flow_states=dict(
            Counter(
                s.get("evidence", {}).get("v2", {}).get("mechanism", {}).get("state", "UNKNOWN")
                for s in signals
            )
        ),
        liquidation_states=dict(
            Counter(
                s.get("evidence", {}).get("v2", {}).get("liquidation_sequence", {}).get("state", "UNKNOWN")
                for s in signals
            )
        ),
        event_features_ready=sum(
            bool(s.get("evidence", {}).get("event_rotation_state", {}).get("available")) for s in signals
        ),
        score_distribution=dict(Counter(s.get("raw_tier", "F") for s in signals)),
        ml="Deterministic authority; a compatible v20 model requires its own source-specific complete outcomes",
    )
