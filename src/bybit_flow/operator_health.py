"""Compact operator warnings from measured runtime state; no qualification changes."""

from .liquidity import SpreadHistory


def recovery_detail(health, now):
    labels = {
        "DNS_RESOLUTION": "Exchange hostname resolution failed.",
        "TLS_CERTIFICATE": "Exchange certificate verification failed; verification remains enabled.",
        "NETWORK_TIMEOUT": "The exchange request timed out.",
        "CONNECTION_ERROR": "The exchange connection failed.",
        "ACCESS_OR_RATE_LIMIT": "Exchange access is denied or temporarily rate limited; cooldowns remain in force.",
        "DATA_VALIDATION": "The public response failed validation.",
    }
    detail = labels.get(health.get("error_category"), "")
    retry = health.get("retry_at_ms")
    if isinstance(retry, (int, float)) and retry > now:
        detail += f" Automatic retry in approximately {max(1, (retry - now + 999) // 1000):g} seconds."
    return detail.strip()


def runtime_health_stale(settings, store, now):
    if not settings.scan_enabled:
        return False
    started = store.get("scanner_process_start", {}).get("at_ms", 0)
    if started and 0 <= now - started < 90_000:
        return False  # Allow the new process to publish its first heartbeat.
    observed = store.get("runtime_health", {}).get("at_ms")
    if observed is None:
        return bool(started and now - started >= 90_000)
    return not 0 <= now - observed <= 90_000


def warnings(settings, store, scanner, recorder, now):
    result = []

    def add(code, title, message, action, *, severity="warning", at_ms=None):
        result.append(
            dict(code=code, severity=severity, title=title, message=message, action=action, at_ms=at_ms)
        )

    if not settings.scan_enabled:
        add(
            "SCAN_DISABLED",
            "Scanner disabled",
            "New setups are not being evaluated.",
            "Enable FLOW_SCAN_ENABLED and restart the desk service.",
            severity="error",
        )
    if not settings.research_alerts or not settings.research_webhook.get_secret_value():
        add(
            "DISCORD_DISABLED",
            "Signal delivery disabled",
            "Discord cannot receive new setup cards.",
            "Check FLOW_RESEARCH_ALERTS and the signal webhook configuration.",
            severity="error",
        )

    clock = store.get("clock_health", {})
    if clock.get("source") != scanner.exchange:
        clock = {}
    if not clock:
        probe = store.get("probe:" + scanner.exchange, {})
        clock = dict(
            at_ms=probe.get("at_ms"),
            skew_ms=probe.get("clock_skew_ms"),
            round_trip_ms=probe.get("rest_latency_ms", 0),
        )
    age = now - (clock.get("at_ms") or 0)
    skew = clock.get("skew_ms")
    round_trip = clock.get("round_trip_ms") or 0
    # Exchange time may be generated anywhere within the request interval,
    # including after waiting for the shared REST limiter. A slow request
    # cannot establish drift merely from its midpoint estimate.
    minimum_drift = max(0, abs(skew) - max(0, round_trip) / 2) if isinstance(skew, (int, float)) else 0
    check = store.get("clock_check_health", {})
    check_failed = (
        settings.scan_enabled
        and check.get("source") == scanner.exchange
        and check.get("state") == "DEGRADED"
        and check.get("at_ms", 0) >= (clock.get("at_ms") or 0)
        and 0 <= now - check.get("at_ms", 0) <= 90_000
    )
    if check_failed:
        add(
            "CLOCK_CHECK_FAILED",
            "Exchange clock verification interrupted",
            "The latest exchange time request failed. This does not establish clock drift. "
            + recovery_detail(check, now),
            "Clock verification retries automatically. Syncing Windows time does not repair exchange connectivity; no restart is needed for a transient connection failure.",
            at_ms=check.get("at_ms"),
        )
    elif settings.scan_enabled and (
        not isinstance(skew, (int, float)) or not 0 <= age <= max(360_000, settings.scan_seconds * 2000)
    ):
        add(
            "CLOCK_UNVERIFIED",
            "Clock check unavailable",
            "There is no recent exchange clock measurement. An unavailable check does not establish clock drift.",
            "Clock verification retries independently of market scans. Check exchange connectivity if it persists; syncing Windows time alone cannot repair a failed connection.",
        )
    elif minimum_drift >= 1000:
        direction = "ahead of" if skew > 0 else "behind"
        add(
            "CLOCK_DRIFT",
            "Clock synchronization needed",
            f"The system clock is at least {minimum_drift / 1000:.2f} seconds {direction} the exchange. Clock drift can block fresh market data.",
            "Windows Settings → Time & language → Date & time → Set time automatically → Sync now. If drift remains, check Docker time.",
            severity="error",
            at_ms=clock.get("at_ms"),
        )
    elif isinstance(skew, (int, float)) and abs(skew) >= 1000:
        add(
            "CLOCK_CHECK_DELAYED",
            "Clock check delayed",
            "The exchange time request was too slow to distinguish clock drift from request delay.",
            "The next scan repeats the check. Check Exchange health if response delays persist; this measurement does not establish that the clock needs synchronization.",
            at_ms=clock.get("at_ms"),
        )

    if scanner.status.get("state") == "error":
        add(
            "SCAN_ERROR",
            "Market scan interrupted",
            (scanner.status.get("reason") or "The latest market scan failed.")
            + " "
            + recovery_detail(store.get("scan_health", {}), now),
            "The scanner retries automatically and preserves existing setup plans and subscriptions. Check Exchange health if connection failures persist.",
            severity="error",
            at_ms=scanner.status.get("at_ms"),
        )
    if not recorder.healthy:
        add(
            "RECORDING_BLOCKED",
            "Recording blocked",
            recorder.reason,
            "Check Data health and available disk space. Use safe retention; preserve active setup evidence.",
            severity="error",
        )
    storage = store.get("storage_status", {})
    usage = storage.get("usage_percent", 0)
    if isinstance(usage, (int, float)) and usage >= 90:
        add(
            "STORAGE_PRESSURE",
            "Recording storage nearly full",
            f"{usage:.1f}% of the configured storage budget is in use.",
            "Check safe retention and free disk space. Do not delete the database, models or active setup history.",
            severity="error" if usage >= 100 else "warning",
            at_ms=storage.get("at_ms"),
        )

    quotes = store.get("quote_health", {})
    if (
        settings.scan_enabled
        and quotes
        and (
            quotes.get("error_type")
            or now - quotes.get("at_ms", 0) > max(180_000, settings.quote_sample_seconds * 3000)
        )
    ):
        add(
            "QUOTE_COLLECTION",
            "Quote collection interrupted",
            "Spread-history observations are failing or have stopped arriving. "
            + recovery_detail(quotes, now),
            "Quote collection retries automatically. Valid quotes rebuild spread history after connectivity recovers; the sample and coverage requirements stay in force.",
            at_ms=quotes.get("at_ms"),
        )
    runtime = store.get("runtime_health", {})
    if runtime_health_stale(settings, store, now):
        add(
            "RUNTIME_HEALTH_STALE",
            "Live health reporting stalled",
            "Live-feed health has stopped refreshing. Current market coverage cannot be verified from this report.",
            "Timed-out scans retry automatically. Check service logs if recovery does not progress; preserve the data volume and active setup history.",
            severity="error",
            at_ms=runtime.get("at_ms"),
        )
    streams = runtime.get("primary_persistent_stream_health", {})
    if (
        settings.scan_enabled
        and 0 <= now - runtime.get("at_ms", 0) <= 60_000
        and streams.get("total", 0)
        and not streams.get("fresh", 0)
    ):
        add(
            "FEED_STALE",
            "Live market data unavailable",
            "No selected market currently has both a fresh order book and fresh trades.",
            "Check exchange connectivity and DNS. New signals remain blocked until the live feeds recover.",
            severity="error",
            at_ms=runtime.get("at_ms"),
        )

    degraded_since = runtime.get("feeds_degraded_since_ms")
    if (
        settings.scan_enabled
        and 0 <= now - runtime.get("at_ms", 0) <= 60_000
        and 0 < streams.get("fresh", 0) < streams.get("total", 0)
        and isinstance(degraded_since, (int, float))
        and now - degraded_since >= 120_000
    ):
        add(
            "FEED_DEGRADED",
            "Some market feeds are still recovering",
            f"{streams['fresh']} of {streams['total']} selected markets have both a fresh order book and fresh trades.",
            "Individual streams reconnect automatically. Affected markets remain blocked until fresh evidence returns; existing setups retain their original plans and monitoring.",
            at_ms=runtime.get("at_ms"),
        )

    selected = runtime.get("selected", []) if runtime.get("source") == scanner.exchange else []
    desired = set(scanner.status.get("deep_symbols", []))
    if (
        settings.scan_enabled
        and desired
        and runtime.get("source") == scanner.exchange
        and 0 <= now - runtime.get("at_ms", 0) <= 60_000
        and now - scanner.status.get("at_ms", now) >= 120_000
        and len(selected) < min(len(desired), settings.deep_symbols)
        and desired - set(selected)
    ):
        add(
            "SUBSCRIPTIONS_MISSING",
            "Market subscriptions incomplete",
            f"{len(desired - set(selected))} selected markets are missing from the live subscription list. New setups in those markets cannot be confirmed.",
            "Check Data health and service logs. The next market scan retries selection; active setup plans remain independently monitored.",
            severity="error",
            at_ms=runtime.get("at_ms"),
        )
    missing = 0
    for symbol in selected:
        key = (
            f"spreads:{settings.spread_bucket_seconds}s:{settings.spread_window_minutes}m:{scanner.exchange}:{symbol}"
            if settings.spread_bucket_seconds != 300 or settings.spread_window_minutes != 360
            else "spreads:" + ("" if scanner.exchange == "bybit" else scanner.exchange + ":") + symbol
        )
        history = SpreadHistory(
            store.get(key, []), settings.spread_bucket_seconds * 1000, settings.spread_window_minutes * 60_000
        )
        assessment = history.assess(now, settings.max_spread_bps, settings.spread_min_samples)
        missing += any(
            "warming up" in reason or "coverage insufficient" in reason for reason in assessment["reasons"]
        )
    if missing:
        add(
            "SPREAD_HISTORY_GAPS",
            "Spread history rebuilding",
            f"{missing} of {len(selected)} monitored markets lack sufficient valid spread observations. These markets cannot qualify new signals yet.",
            "Keep the scanner running with a synchronized clock and working exchange connection. Missing history clears as fresh quotes accumulate.",
        )

    confirmation = store.get("confirmation_health", {})
    if confirmation.get("state") == "DEGRADED":
        add(
            "CONFIRMATION_ERROR",
            "Setup confirmation interrupted",
            "The latest confirmation cycle failed.",
            "Check Data health and service logs. Confirmation retries automatically.",
            severity="error",
            at_ms=confirmation.get("last_error_ms"),
        )
    transport = store.get("discord_transport", {})
    if transport.get("status") in {"error", "failed", "rejected", "uncertain"}:
        add(
            "DISCORD_DELIVERY",
            "Discord delivery problem",
            "The latest Discord delivery did not have a confirmed successful receipt.",
            "Check the webhook configuration and Discord connectivity test. Do not assume that an uncertain card was delivered.",
            severity="error",
            at_ms=transport.get("at_ms"),
        )
    return result
