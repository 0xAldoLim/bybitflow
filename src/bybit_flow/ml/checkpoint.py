"""Durable replay checkpoints can release only demonstrably consumed tape."""

import re

from .store import digest


def seal(checkpoint, at_ms):
    value = checkpoint | dict(
        checkpoint_version="primary-checkpoint-v1",
        saved_ms=at_ms,
        verified_boundary_ms=checkpoint.get("verified_boundary_ms", at_ms),
    )
    return value | dict(checkpoint_sha256=digest(value))


def consumed_cursor(store, snapshot_id, snapshot, now):
    try:
        checkpoint = store.get("primary_materialization", {})
        checksum = checkpoint.pop("checkpoint_sha256", None)
        cursor = checkpoint["cursor_ms"]
        if (
            checkpoint.get("checkpoint_version") != "primary-checkpoint-v1"
            or checkpoint.get("policy") != "incremental-prints-v1"
            or checksum != digest(checkpoint)
            or not 0 <= now - checkpoint["saved_ms"] <= 3_600_000
            or not snapshot["decision_ms"] <= cursor <= now
            or not checkpoint["verified_boundary_ms"] <= checkpoint["saved_ms"] <= now
            or not re.fullmatch(r"[a-f0-9]{64}", checkpoint["input_event_hash"])
            or not all(k in checkpoint for k in ("positions", "subscribed", "last", "seen"))
            or checkpoint["pending"] != len(checkpoint["positions"])
        ):
            return None
        values = checkpoint["positions"][snapshot_id]
        from ..backtest import PaperPosition
        from ..models import Signal

        expected = Signal.model_validate(snapshot["signal"])
        expected.created_ms = snapshot["decision_ms"]
        if values["signal"] != expected.model_dump(mode="json"):
            return None
        position = PaperPosition(
            **(
                values
                | dict(
                    signal=Signal.model_validate(values["signal"]),
                    funding_timestamps=set(values["funding_timestamps"]),
                )
            )
        )
        if position.exit_ms is not None:
            return None
        return cursor
    except (KeyError, TypeError, ValueError, OverflowError):
        return None  # Fail closed: retain the full original evidence interval.
