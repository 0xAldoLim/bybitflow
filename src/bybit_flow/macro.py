"""Scheduled high-impact USD events; no release values or trading execution."""

import asyncio
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from .storage import now_ms

SOURCE = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"


class ForexFactory:
    async def fetch(self):
        async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
            response = await client.get(SOURCE)
            response.raise_for_status()
            if len(response.content) > 2_000_000:
                raise ValueError("Calendar exceeds bounded response size")
            return parse(response.json())


def parse(rows):
    events = []
    for row in rows:
        if row.get("country") != "USD" or row.get("impact", "").upper() != "HIGH":
            continue
        date = datetime.fromisoformat(row["date"])
        if date.tzinfo is None:
            raise ValueError("Calendar event lacks explicit provider timezone")
        local = date.astimezone(ZoneInfo("America/New_York"))
        if not 8 <= local.hour < 17 or local.weekday() >= 5:
            continue
        title, scheduled = row["title"], int(date.timestamp() * 1000)
        events.append(
            dict(
                event_id=hashlib.sha256(f"{title}:{scheduled}".encode()).hexdigest()[:20],
                title=title,
                scheduled_ms=scheduled,
                currency="USD",
                impact="HIGH",
                provider_timezone=str(date.tzinfo),
                source=SOURCE,
            )
        )
    return sorted(events, key=lambda e: e["scheduled_ms"])


def state(store, settings, now):
    cache = store.get("macro_calendar", {})
    fresh = 0 <= now - cache.get("fetched_ms", 0) <= 6 * 3_600_000
    events = cache.get("events", []) if fresh and settings.macro_news_enabled else []
    intervals = []
    for event in sorted(events, key=lambda e: e["scheduled_ms"]):
        start = event["scheduled_ms"] - settings.macro_news_pre_minutes * 60_000
        end = event["scheduled_ms"] + settings.macro_news_post_minutes * 60_000
        if intervals and start <= intervals[-1][1]:
            intervals[-1][1] = max(end, intervals[-1][1])
            intervals[-1][2].append(event)
        else:
            intervals.append([start, end, [event]])
    active = next((r for r in intervals if r[0] <= now < r[1]), None)
    upcoming = next((e for e in events if e["scheduled_ms"] >= now), None)
    return dict(
        provider="Forex Factory",
        status="DISABLED"
        if not settings.macro_news_enabled
        else "HEALTHY"
        if fresh and not cache.get("error_type")
        else "DEGRADED",
        cache_fresh=fresh,
        last_refresh_ms=cache.get("fetched_ms"),
        macro_pause_active=bool(active),
        pause_until_ms=active[1] if active else None,
        events=active[2] if active else [],
        next_event=upcoming,
        minutes_to_high_impact_usd=(upcoming["scheduled_ms"] - now) / 60_000 if upcoming else None,
        available_ms=cache.get("fetched_ms"),
        degraded_policy="fail-open",
    )


async def run(scanner, provider=None):
    provider = provider or ForexFactory()
    store, settings = scanner.store, scanner.settings
    while True:
        now = now_ms()
        cache = store.get("macro_calendar", {})
        if (
            settings.macro_news_enabled
            and now - cache.get("attempt_ms", 0) >= settings.macro_refresh_minutes * 60_000
        ):
            try:
                cache = dict(events=await provider.fetch(), fetched_ms=now_ms(), source=SOURCE)
            except Exception as exc:
                cache["error_type"] = type(exc).__name__
            cache["attempt_ms"] = now
            store.put("macro_calendar", cache)
        current = state(store, settings, now_ms())
        previous = store.get("macro_health", {})
        store.put("macro_health", current)
        if current["macro_pause_active"] != previous.get("macro_pause_active", False):
            secret = settings.research_webhook.get_secret_value()
            if secret:
                paused = current["macro_pause_active"]
                window = current["pause_until_ms"] if paused else previous.get("pause_until_ms")
                payload = {
                    "allowed_mentions": {"parse": []},
                    "embeds": [
                        {
                            "title": "HIGH IMPACT USD NEWS PAUSE" if paused else "NEWS PAUSE ENDED",
                            "description": (
                                "\n".join(e["title"] for e in current["events"])
                                + f"\nNew entries paused until <t:{window // 1000}:F>."
                                if paused
                                else "New entries require fresh confirmation. Existing setups remain monitored."
                            ),
                        }
                    ],
                }
                await scanner.notifier.deliver(f"macro:{window}:{paused}", None, payload, secret)
        await asyncio.sleep(15)
