"""Precise, deduplicated confirmation coverage diagnostics."""

from .funnel import emit


def record(scanner, signal, now, reasons, start, end):
    signal.coverage.update(reason_codes=reasons, window_start_ms=start, window_end_ms=end)
    if not reasons:
        return
    tape = scanner.streams.tapes.get(signal.symbol)
    warm_at = ((tape.coverage_start + end - start + 59999) // 60000 * 60000) if tape else None
    expiry = signal.trigger_expires_ms or signal.expires_ms
    signal.coverage.update(
        warm_at_ms=warm_at,
        readiness="WAITING_FOR_FLOW_WARMUP"
        if warm_at is not None and warm_at < expiry
        else "EXECUTION_COVERAGE_NEVER_BECAME_COMPLETE",
    )
    emit(
        scanner.store,
        "coverage_incomplete_evaluations",
        now,
        signal=signal,
        key=f"coverage-eval:{signal.id}:{now}",
    )
    for reason in reasons:
        emit(
            scanner.store,
            "coverage_incomplete_unique_windows",
            now,
            signal=signal,
            reason=reason,
            key=f"coverage-window:{signal.id}:{start}:{end}:{reason}",
        )


def summary(store, now):
    result = dict(incomplete_evaluations_1h=0, incomplete_unique_windows_1h=0, top_reasons={})
    for metric, dimension, value, count in store.db.execute(
        "SELECT metric,dimension,value,sum(n) FROM funnel_minutes WHERE minute_ms>=? AND metric IN ('coverage_incomplete_evaluations','coverage_incomplete_unique_windows') GROUP BY metric,dimension,value",
        (now - 3600000,),
    ):
        if dimension == "all":
            result[
                "incomplete_evaluations_1h"
                if metric.endswith("evaluations")
                else "incomplete_unique_windows_1h"
            ] = count
        elif dimension == "reason" and metric.endswith("windows"):
            result["top_reasons"][value] = count
    return result
