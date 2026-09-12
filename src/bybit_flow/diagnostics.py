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
        "status": "OK" if store.db.execute("PRAGMA quick_check").fetchone()[0] == "ok" else "FAIL"
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
    runtime = store.get("runtime_health", {})
    checks["runtime"] = runtime | {
        "status": "FRESH" if 0 <= now_ms() - runtime.get("at_ms", 0) <= 90_000 else "NOT_OBSERVED_OR_STALE"
    }
    checks["stream"] = store.get("stream_health", {"status": "NOT_OBSERVED"})
    checks["recorder"] = {
        "segments": store.db.execute("SELECT count(*) FROM segments").fetchone()[0],
        "note": "segment count is not proof the running recorder is healthy",
    }
    checks["discord"] = {
        "status": "CONFIGURED" if settings.research_webhook.get_secret_value() else "NOT_CONFIGURED",
        "delivery": "run test-discord to establish actual delivery",
    }
    from .ml.registry import Registry

    summary = Registry(store).summary()
    checks["ml"] = {
        "status": "OK",
        "enabled": settings.ml_enabled,
        "champion": summary.get("champion"),
        "snapshots": store.db.execute("SELECT count(*) FROM ml_snapshots").fetchone()[0],
        "labels": store.db.execute("SELECT count(*) FROM ml_labels").fetchone()[0],
    }
    warnings = []
    if not settings.scan_enabled:
        warnings.append("Scanner disabled: FLOW_SCAN_ENABLED=false")
    if network and not any(checks[n].get("status") == "HEALTHY" for n in names):
        warnings.append("No tested source has healthy REST and genuine trade WS data; no trade")
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
                "description": "NOT A TRADE SIGNAL\nNo candidate, fill, position or ML outcome is created.",
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
    return {
        "status": result,
        "at_ms": now,
        "note": "Confirm the message in Discord; ambiguous attempts are not retried",
    }
