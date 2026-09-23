"""Operator diagnostics. Secret values and exception messages never enter output."""

import asyncio
import json
import shutil
import tempfile
from importlib.metadata import version

from .exchanges import VENUES, market_probe
from .notifications import Notifier, iso
from .storage import now_ms


async def doctor(settings, store, network=True):
    checks = {}
    checks["configuration"] = {
        "status": "OK",
        "market_source": settings.market_source,
        "alerts_only": True,
        "tradingview_required": False,
    }
    checks["database"] = {
        "status": "OK" if store.db.execute("SELECT 1").fetchone()[0] == 1 else "FAIL",
        "scope": "operational read; full integrity checks run separately",
    }
    try:
        with tempfile.TemporaryFile(dir=settings.data_dir) as f:
            f.write(b"storage-check")
            f.flush()
        free = shutil.disk_usage(settings.data_dir).free
        checks["storage"] = {"status": "OK" if free >= 2_000_000_000 else "WARN", "free_bytes": free}
    except OSError:
        checks["storage"] = {"status": "FAIL"}
    source = settings.market_source
    names = VENUES if source in {"auto", "multi"} else (source,)
    if network:
        reports = await asyncio.gather(*(market_probe(n, settings) for n in names))
        for report in reports:
            store.put("probe:" + report["exchange"], report)
            checks[report["exchange"]] = report
    else:
        for name in names:
            checks[name] = store.get("probe:" + name, {"status": "NOT_TESTED"})
    checks["scanner"] = store.get("scanner", {"state": "NOT_OBSERVED"})
    from .horizons import session_context

    active = store.active_signals()
    checks["session"] = session_context(now_ms())
    checks["setups"] = dict(
        active=len(active),
        legacy_active=sum(s.get("horizon_profile", "LEGACY") == "LEGACY" for s in active),
        current_active=sum(s.get("horizon_profile", "LEGACY") != "LEGACY" for s in active),
    )
    checks["post_terminal"] = dict(
        store.db.execute("SELECT status,count(*) FROM observations GROUP BY status")
    )
    checks["storage_budget"] = store.get("storage_status", {"status": "run storage status for measurement"})
    checks["cleanup"] = store.get("storage_health", {})
    runtime = store.get("runtime_health", {})
    checks["runtime"] = runtime | {
        "status": (
            "FRESH"
            if runtime.get("streams_fresh")
            and runtime.get("source_feed_available")
            and runtime.get("source_ready")
            else "DEGRADED"
        )
        if 0 <= now_ms() - runtime.get("at_ms", 0) <= 90_000
        else "STALE"
    }
    checks["stream"] = store.get("stream_health", {"status": "NOT_OBSERVED"})
    from .thesis_health import summary as health_summary

    checks["thesis_health"] = health_summary(store)
    from .coverage import summary as coverage_summary

    checks["active_lifecycle"] = store.get("active_lifecycle", {"status": "BOOTSTRAPPING"})
    checks["candidate_coverage"] = coverage_summary(store, now_ms())
    checks["market_stream"] = dict(
        connected=checks["runtime"]["status"] != "STALE"
        and runtime.get("persistent_connected", runtime.get("source_feed_available", False)),
        last_market_event_ms=runtime.get("last_market_event_ms"),
        reconnecting=not runtime.get("source_feed_available", False),
    )
    checks["probe_health"] = {name: checks[name] for name in names}
    checks["primary_persistent_stream_health"] = runtime.get("primary_persistent_stream_health", {})
    checks["active_lifecycle_stream_health"] = runtime.get("active_lifecycle_stream_health", {})
    checks["market_stream"]["reconnecting"] = not checks["market_stream"]["connected"]
    for key in (
        "selected_streams_total",
        "selected_streams_fresh",
        "active_required_streams_total",
        "active_required_streams_fresh",
    ):
        checks["market_stream"][key] = runtime.get(key, 0)
    delivery = dict(
        store.db.execute(
            "SELECT notification_status,count(*) FROM terminal_events GROUP BY notification_status"
        )
    )
    checks["terminal_delivery"] = dict(
        terminal_pending_delivery=delivery.get("pending", 0),
        terminal_failed_delivery=delivery.get("failed", 0),
        oldest_pending_terminal_ms=store.db.execute(
            "SELECT min(json_extract(payload,'$.detected_ms')) FROM terminal_events WHERE notification_status='pending'"
        ).fetchone()[0],
        last_terminal_delivery_ms=store.get("last_terminal_delivery_ms"),
        note="Lifecycle state correct; Discord terminal projection pending"
        if delivery.get("pending")
        else "No pending terminal projection",
    )
    checks["macro"] = store.get("macro_health", {"status": "NOT_OBSERVED"})
    checks["recorder_runtime"] = store.get("recorder_health", {"status": "NOT_OBSERVED"})
    checks["recorder_runtime"]["last_recorder_write_ms"] = checks["recorder_runtime"].get("last_success_ms")
    checks["recorder_runtime"]["current_enqueue_rate"] = checks["recorder_runtime"].get("enqueue_rate_1m")
    checks["recorder"] = {
        "segments": store.db.execute("SELECT count(*) FROM segments").fetchone()[0],
        "note": "segment count is not proof the running recorder is healthy",
    }
    checks["discord"] = {
        "status": "CONFIGURED" if settings.research_webhook.get_secret_value() else "NOT_CONFIGURED",
        "delivery": "run test-discord to establish actual delivery",
    }
    from .identity import delivery_status

    checks["real_signal_delivery"] = delivery_status(store)
    from .funnel import status as funnel_status

    checks["signal_funnel"] = funnel_status(
        store, configured=bool(settings.research_webhook.get_secret_value())
    )
    from .ml.registry import Registry

    summary = Registry(store).cached_summary()
    from .ml.operations import status as ml_status

    checks["ml"] = ml_status(store, summary, settings.ml_enabled)
    warnings = []
    if not settings.scan_enabled:
        warnings.append("Scanner disabled: FLOW_SCAN_ENABLED=false")
    if not checks["market_stream"]["connected"]:
        warnings.append("No healthy live source; no trade")
    elif network and not any(checks[n].get("status") == "HEALTHY" for n in names):
        warnings.append("Public connectivity probe degraded; persistent feed remains active.")
    if not settings.admin_token.get_secret_value():
        warnings.append("Dashboard password unset; Docker remote bind requires FLOW_ADMIN_TOKEN")
    return {"at_ms": now_ms(), "checks": checks, "warnings": warnings}


def human_report(report):
    rows = ["CHECK                 RESULT", "--------------------  ----------------------------------------"]
    for name, detail in report["checks"].items():
        rows.append(f"{name:20}  {json.dumps(detail, ensure_ascii=False)}")
    rows.extend("WARNING: " + warning for warning in report["warnings"])
    return "\n".join(rows)


async def discord_test(settings, store, transport=None):
    secret = settings.research_webhook.get_secret_value()
    if not secret:
        return {
            "status": "NOT_CONFIGURED",
            "action": "Configure FLOW_RESEARCH_WEBHOOK in the private environment",
        }
    now = now_ms()
    health = {name: store.get("probe:" + name, {}).get("status", "NOT_TESTED") for name in VENUES}
    payload = {
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": "BYBITFLOW CONNECTION TEST",
                "description": "NOT A TRADE SIGNAL\nBybitFlow outbound Discord connectivity test. No setup, price recommendation, probability, or execution.",
                "timestamp": iso(now),
                "fields": [
                    {"name": "Application", "value": version("bybit-flow")},
                    {"name": "Market source", "value": settings.market_source},
                    {"name": "Last diagnostic results (not live-feed proof)", "value": json.dumps(health)},
                ],
            }
        ],
    }
    result = await Notifier(settings, store, transport).deliver(f"operator-test:{now}", None, payload, secret)
    store.put("discord_connection_test", dict(status=result, at_ms=now))
    return {
        "status": result,
        "at_ms": now,
        "note": "Confirm the message in Discord; ambiguous attempts are not retried",
    }
