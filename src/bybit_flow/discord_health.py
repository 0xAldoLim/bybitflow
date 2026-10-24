"""Bounded connectivity checks; delivery receipts and retry policy remain in the notifier."""

import asyncio
import hashlib
import math
from urllib.parse import urlparse

import httpx

from .storage import now_ms

CHECK_INTERVAL_MS = 300_000
CHECK_FRESH_MS = 900_000
REQUEST_TIMEOUT_SECONDS = 10


def endpoints(settings):
    return {
        "signals": settings.research_webhook.get_secret_value(),
        "monitoring": settings.monitoring_webhook.get_secret_value(),
        "operations": settings.ops_webhook.get_secret_value(),
    }


def endpoint_hash(secret):
    return hashlib.sha256(secret.encode()).hexdigest()


def reachability(store, settings, now):
    result = {}
    for name, secret in endpoints(settings).items():
        row = store.get(f"discord_reachability:{name}", {})
        if secret and row.get("endpoint_hash") == endpoint_hash(secret):
            result[name] = {k: v for k, v in row.items() if k != "endpoint_hash"} | dict(
                fresh=0 <= now - row.get("at_ms", 0) <= CHECK_FRESH_MS
            )
    return result


def retry_delay(response):
    """Honor Discord's rate-limit delay without persisting a response body."""
    values = [response.headers.get("Retry-After")]
    try:
        body = response.json()
        if isinstance(body, dict):
            values.append(body.get("retry_after"))
    except ValueError:
        pass
    delays = [CHECK_INTERVAL_MS]
    for value in values:
        try:
            seconds = float(value)
            if math.isfinite(seconds) and seconds > 0:
                delays.append(math.ceil(seconds * 1000))
        except (TypeError, ValueError, OverflowError):
            pass
    return max(delays)


async def check_webhooks(settings, store, transport=None):
    """GET only. Never send a test card, reconcile a receipt, or change the outbox."""
    for name, secret in endpoints(settings).items():
        if not secret:
            continue
        now = now_ms()
        previous = store.get(f"discord_reachability:{name}", {})
        same_endpoint = previous.get("endpoint_hash") == endpoint_hash(secret)
        if same_endpoint and 0 <= now - previous.get("at_ms", 0) and now < previous.get("retry_at_ms", 0):
            continue
        previous = previous if same_endpoint else {}
        detail = dict(state="UNAVAILABLE", category="UNKNOWN", http_status=None)
        delay = min(CHECK_INTERVAL_MS, 30_000 * 2 ** min(previous.get("consecutive_failures", 0), 4))
        try:
            parsed = urlparse(secret)
            parts = parsed.path.split("/")
            valid = (
                parsed.scheme == "https"
                and parsed.hostname == "discord.com"
                and parsed.username is None
                and parsed.port in {None, 443}
                and parsed.path.startswith("/api/webhooks/")
                and len(parts) >= 5
                and bool(parts[3])
                and bool(parts[4])
            )
        except ValueError:
            valid = False
        if not valid:
            detail.update(state="INVALID_CONFIG", category="CONFIGURATION")
            delay = CHECK_INTERVAL_MS
        else:
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS, transport=transport) as client:
                    async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
                        response = await client.get(secret, follow_redirects=False)
                detail.update(http_status=response.status_code, category="HTTP")
                if response.status_code == 200:
                    body = response.json()
                    if not isinstance(body, dict) or str(body.get("id")) != parts[3]:
                        raise ValueError("Webhook identity unavailable")
                    detail["state"] = "AVAILABLE"
                    delay = CHECK_INTERVAL_MS
                elif response.status_code == 429:
                    detail["state"] = "RATE_LIMITED"
                    delay = retry_delay(response)
                elif response.status_code in {401, 403, 404}:
                    delay = CHECK_INTERVAL_MS
            except (httpx.TransportError, TimeoutError, ValueError) as exc:
                detail.update(
                    category="DATA_VALIDATION"
                    if isinstance(exc, ValueError)
                    else "TIMEOUT"
                    if isinstance(exc, (httpx.TimeoutException, TimeoutError))
                    else "CONNECTION_ERROR",
                    error_type=type(exc).__name__,
                )
        checked = now_ms()
        available = detail["state"] == "AVAILABLE"
        store.put(
            f"discord_reachability:{name}",
            detail
            | dict(
                at_ms=checked,
                endpoint_hash=endpoint_hash(secret),
                last_success_ms=checked if available else previous.get("last_success_ms"),
                consecutive_failures=0 if available else previous.get("consecutive_failures", 0) + 1,
                retry_at_ms=checked + delay,
                method="GET",
            ),
        )


async def monitor(settings, store):
    while True:
        await check_webhooks(settings, store)
        await asyncio.sleep(30)


DESTINATIONS = ("signals", "monitoring", "operations")
FAILED = {"error", "failed", "rejected", "uncertain", "pending", "rate-limited"}


def record_transport(store, detail, destination, method):
    result = detail | dict(destination=destination, method=method)
    store.put(f"discord_transport:{destination}", result)
    # Retain the existing POST diagnostic for consumers of the signal funnel.
    store.put("discord_terminal_transport" if method == "PATCH" else "discord_transport", result)


def transport_health(store, settings=None, now=None):
    """Read bounded caches, without changing delivery state or scanning history."""
    destinations = {
        name: result for name in DESTINATIONS if (result := store.get(f"discord_transport:{name}", {}))
    }
    post = store.get("discord_transport", {})
    terminal = store.get("discord_terminal_transport", {})
    legacy_terminal_ms = store.get("last_terminal_delivery_ms", 0) or 0
    if not terminal and legacy_terminal_ms:
        terminal = dict(at_ms=legacy_terminal_ms, status="sent", destination="signals", method="PATCH")
    if terminal and "signals" not in destinations:
        destinations["signals"] = terminal

    historical = None
    if post and not post.get("destination"):
        # Older releases cached only POST failures. A later confirmed PATCH is
        # evidence of a subsequent write, not a receipt for that earlier POST.
        if terminal.get("status") == "sent" and terminal.get("at_ms", 0) > post.get("at_ms", 0):
            if post.get("status") in FAILED:
                historical = post
        else:
            destinations["unknown"] = post
    return dict(
        destinations=destinations,
        reachability=reachability(store, settings, now if now is not None else now_ms()) if settings else {},
        historical_unconfirmed_attempt=historical,
        history_note=(
            "Transport results describe the last actual request to each destination, not continuous connectivity. "
            "A later successful write does not confirm an earlier uncertain initial card. "
            "Initial-delivery counts and receipts remain unchanged; uncertain initials are not automatically resent."
            " Read-only reachability checks do not prove message delivery or write permission."
        ),
    )
