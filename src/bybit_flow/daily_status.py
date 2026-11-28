"""One status message a day, so a quiet channel can be told apart from a dead system.

Alerts are rare by design. Without a regular sign of life, "no setups qualified"
and "the scanner has been down since Tuesday" look identical in Discord. The
message states what was evaluated, what was sent, what was held back and why.
If it stops arriving, the system is not running.
"""

import asyncio
import time
from collections import Counter

from .storage import now_ms

DAY = 86_400_000
BLOCKS = "delivery_blocks_v1"
REASONS = {
    "BELOW_MIN_ALERT_TIER": "below the minimum grade",
    "RULE_BLOCKED": "rule blocked as wrong-sided",
    "RULE_NOT_ACCEPTED": "rule not accepted",
}


def note_block(store, signal, code, at_ms=None):
    """Remember, once per setup, why a confirmed setup was not sent."""
    blocks = store.get(BLOCKS, {})
    if signal.id in blocks:
        return
    blocks[signal.id] = [code, now_ms() if at_ms is None else at_ms, signal.symbol, signal.raw_tier]
    for ident in sorted(blocks, key=lambda k: blocks[k][1])[:-500]:  # Bounded history.
        del blocks[ident]
    store.put(BLOCKS, blocks)


def mention(setting):
    """Discord content and allowed_mentions for the configured ping, or nothing."""
    value = (setting or "").strip()
    if value in {"everyone", "here"}:
        return "@" + value, {"parse": ["everyone"]}
    kind, _, ident = value.partition(":")
    if kind == "user" and ident.isdigit():
        return f"<@{ident}>", {"users": [ident]}
    if kind == "role" and ident.isdigit():
        return f"<@&{ident}>", {"roles": [ident]}
    return None, {"parse": []}


def summary(store, settings, now):
    since = now - DAY
    db = store.db
    one = lambda sql, *args: db.execute(sql, args).fetchone()[0]  # noqa: E731
    sent = one(
        "SELECT count(*) FROM outbox WHERE key LIKE '%:initial' AND status='sent' AND updated_ms>?", since
    )
    last = one("SELECT max(updated_ms) FROM outbox WHERE key LIKE '%:initial' AND status='sent'")
    blocks = Counter(v[0] for v in store.get(BLOCKS, {}).values() if v[1] > since)
    scanner, recorder = store.get("scanner", {}), store.get("recorder_health", {})
    storage = store.get("storage_status", {})
    started = store.get("scanner_process_start", {}).get("at_ms")
    return dict(
        at_ms=now,
        candidates=one("SELECT count(*) FROM signals WHERE created_ms>?", since),
        confirmed=one(
            "SELECT count(DISTINCT signal_id) FROM transitions WHERE state='CONFIRMED' AND at_ms>?", since
        ),
        alerts_sent=sent,
        last_alert_ms=last,
        held_back=dict(blocks),
        uncertain=one(
            "SELECT count(*) FROM outbox WHERE key LIKE '%:initial' AND status IN ('uncertain','sending') "
            "AND updated_ms>?",
            since,
        ),
        scanner_state=scanner.get("state", "unknown"),
        scanner_age_minutes=round((now - scanner.get("at_ms", now)) / 60_000),
        recorder_state=recorder.get("state", "unknown"),
        storage_percent=storage.get("usage_percent"),
        running_hours=round((now - started) / 3_600_000, 1) if started else None,
        minimum_grade=settings.min_alert_tier,
    )


def embed(s):
    ago = "never" if not s["last_alert_ms"] else f"{(s['at_ms'] - s['last_alert_ms']) / 3_600_000:.1f}h ago"
    held = ", ".join(f"{n} {REASONS.get(code, code)}" for code, n in sorted(s["held_back"].items())) or "none"
    healthy = s["scanner_state"] not in {"error", "blocked_by_storage", "disabled", "unknown"}
    lines = [
        f"**{s['alerts_sent']} setup alerts sent** in the last 24h (last one {ago}).",
        f"{s['candidates']} candidates evaluated, {s['confirmed']} confirmed. Held back: {held}.",
        f"Scanner: {s['scanner_state']} (updated {s['scanner_age_minutes']} min ago) · "
        f"recorder: {s['recorder_state']}"
        + (
            f" · storage {s['storage_percent']:.0f}%"
            if isinstance(s["storage_percent"], (int, float))
            else ""
        )
        + (f" · running {s['running_hours']}h" if s["running_hours"] is not None else ""),
    ]
    if s["uncertain"]:
        lines.append(f"⚠ {s['uncertain']} alert deliveries were not confirmed by Discord.")
    return dict(
        allowed_mentions=dict(parse=[]),
        embeds=[
            dict(
                title="DAILY STATUS · " + ("running" if healthy else "NEEDS ATTENTION"),
                description="\n".join(lines),
                color=0x4CC9A4 if healthy else 0xD9534F,
                footer=dict(
                    text=f"Minimum grade {s['minimum_grade']}. "
                    "If this message stops arriving, the system is not running."
                ),
            )
        ],
    )


def due(now, hour_utc):
    """The status day starts at the configured UTC hour."""
    return (now - hour_utc * 3_600_000) // DAY


async def send(notifier, now=None):
    settings, store = notifier.settings, notifier.store
    if not settings.daily_status:
        return "disabled"
    secret = settings.monitoring_webhook.get_secret_value() or settings.research_webhook.get_secret_value()
    if not secret:
        return "dry-run"
    now = now or now_ms()
    key = f"status:daily:{due(now, settings.daily_status_hour_utc)}"
    if store.db.execute("SELECT 1 FROM outbox WHERE key=?", (key,)).fetchone():
        return "already-attempted"
    return await notifier.deliver(key, None, embed(summary(store, settings, now)), secret)


async def loop(runtime):
    """Checks every five minutes; the outbox key makes it once per status day."""
    started = time.monotonic()
    while True:
        # Give the scanner a few minutes to report a real state after a restart.
        if time.monotonic() - started >= 180:
            await send(runtime.notifier)
        await asyncio.sleep(300)
