"""Durable proof for retrying failed raw-retention verification."""

import hashlib
import json
import re
import time

from .storage import now_ms

LATCH = "cleanup_verification_v212"
TABLES = ("ml_snapshots", "ml_labels")
RECHECK_MS = 900_000
AUDIT_LIMIT = 8_000_000
INTERRUPTED = "Cleanup ownership expired/lost"


def frozen_rows(store, limits, *, deadline=None):
    deadline = time.monotonic() + 240 if deadline is None else deadline
    result = {}
    for table in TABLES:
        digest = hashlib.sha256()
        count = 0
        cursor = store.db.execute(
            f"SELECT rowid,* FROM {table} WHERE rowid<=? ORDER BY rowid", (limits[table],)
        )
        try:
            for row in cursor:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Frozen cleanup inventory exceeded the verification time budget")
                digest.update(json.dumps(tuple(row), ensure_ascii=False).encode() + b"\n")
                count += 1
        finally:
            cursor.close()
        result[table] = dict(count=count, sha256=digest.hexdigest())
    return result


def audit_path(store, run_id):
    if not isinstance(run_id, str) or not re.fullmatch(r"cleanup:[a-f0-9]{32}", run_id):
        raise ValueError("Invalid failed cleanup run identity")
    root = store.root.resolve()
    path = root / "ml" / "maintenance" / (run_id.replace(":", "-") + "-pre.json")
    if path.resolve() != path:
        raise ValueError("Cleanup audit path is not a regular managed path")
    return path


def reconcile_failed_cleanup(store, *, fresh=False):
    """Cached failures spare routine polling. Destructive callers always recheck."""
    from .recording_maintenance import ml_inventory, verify_ml

    failed = store.get(LATCH, {})
    if failed.get("status") != "FAILED":
        return dict(state="NO_FAILURE")
    run_id = failed.get("run_id")
    now = now_ms()
    try:
        path = audit_path(store, run_id)
        stat = path.stat()
        signature = [stat.st_size, stat.st_mtime_ns]
    except (OSError, ValueError):
        path = signature = None

    previous = store.get("cleanup_reconciliation", {})
    if (
        not fresh
        and previous.get("failed_run") == run_id
        and previous.get("audit_signature") == signature
        and 0 <= now - previous.get("at_ms", 0) < RECHECK_MS
    ):
        return previous

    evidence = dict(at_ms=now, failed_run=run_id, audit_signature=signature)
    try:
        if path is None or not path.is_file() or signature[0] > AUDIT_LIMIT:
            raise ValueError("Pre-cleanup audit is missing or exceeds the read limit")
        with path.open("rb") as stream:
            raw = stream.read(AUDIT_LIMIT + 1)
        checksum = hashlib.sha256(raw).hexdigest()
        expected = failed.get("pre_audit_sha256")
        if len(raw) > AUDIT_LIMIT or (expected is not None and checksum != expected):
            raise ValueError("Pre-cleanup audit checksum could not be verified")
        before = json.loads(raw)
        if not expected or before.get("run_id") != run_id or not before.get("frozen_rows"):
            raise ValueError("Failed run predates durable frozen row fingerprints")
        limits = before["row_limits"]
        if (
            set(limits) != set(TABLES)
            or any(type(n) is not int or n < 0 for n in limits.values())
            or set(before["frozen_rows"]) != set(TABLES)
            or not all(
                isinstance(before[k], dict)
                for k in ("eligible", "by_source", "models", "model_content", "artifacts")
            )
        ):
            raise ValueError("Pre-cleanup audit has invalid frozen evidence")
        evidence["pre_audit_sha256"] = checksum
        evidence["row_limits"] = limits
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        reason = str(exc) if type(exc) is ValueError else "Pre-cleanup audit could not be read or validated"
        evidence.update(state="AUDIT_UNAVAILABLE", reason=reason[:160])
    else:
        try:
            observed = frozen_rows(store, limits)
            evidence["frozen_rows"] = observed
            for table in TABLES:
                if before["frozen_rows"][table] != observed[table]:
                    raise RuntimeError(f"Frozen {table} count or content changed")
            after = ml_inventory(store, before["at_ms"], artifacts=True, row_limits=limits)
            verify_ml(before, after)
        except RuntimeError as exc:
            evidence.update(state="HARD_BLOCKED", reason=str(exc).split(": ")[0][:160])
        except (TimeoutError, OSError, ValueError, KeyError, TypeError):
            evidence.update(state="AUDIT_UNAVAILABLE", reason="Frozen ML evidence could not be verified")
        else:
            evidence.update(
                state="SAFE_TO_RETRY", reason="Frozen ML rows, eligible identities and artifacts verified"
            )

    if evidence["state"] == "AUDIT_UNAVAILABLE" and str(failed.get("reason", "")).startswith(INTERRUPTED):
        # The run lost its claim (restart, sleep, timeout): an interruption, not a
        # failed comparison. With no usable pre-run audit there is nothing left
        # to reconcile, ever, and blocking every later cleanup only fills the
        # disk. The next run takes and verifies its own fresh pre/post audit.
        # A readable audit that shows changed frozen evidence still hard-blocks.
        evidence.update(
            state="SAFE_TO_RETRY",
            interrupted=True,
            unverifiable=evidence["reason"],
            reason="Interrupted run has no usable pre-run audit; a fresh audited run replaces it",
        )
    store.put("cleanup_reconciliation", evidence)
    return evidence


def storage_blocked(reconciliation):
    return reconciliation["state"] in {"HARD_BLOCKED", "AUDIT_UNAVAILABLE"}
