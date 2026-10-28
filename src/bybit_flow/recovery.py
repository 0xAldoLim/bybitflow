"""Bounded network recovery diagnostics; never grant trading qualification."""

import asyncio
import socket
import ssl
import traceback
from pathlib import Path

import httpx


async def rest_batch(*awaitables):
    """A failed batch must release every sibling request before its caller retries."""
    tasks = [asyncio.ensure_future(item) for item in awaitables]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


def error_category(exc):
    if isinstance(exc, RuntimeError) and str(exc) == "Recording circuit open":
        return "RECORDING_UNAVAILABLE"
    chain, pending, seen = [], [exc], set()
    while pending:
        item = pending.pop()
        if item is None or id(item) in seen:
            continue
        seen.add(id(item))
        chain.append(item)
        pending.extend((item.__cause__, item.__context__))
    for types, category in (
        ((ssl.SSLCertVerificationError,), "TLS_CERTIFICATE"),
        ((socket.gaierror,), "DNS_RESOLUTION"),
        ((TimeoutError, httpx.TimeoutException), "NETWORK_TIMEOUT"),
        ((PermissionError,), "ACCESS_OR_RATE_LIMIT"),
        ((ConnectionError, httpx.TransportError, OSError), "CONNECTION_ERROR"),
        ((ValueError,), "DATA_VALIDATION"),
    ):
        if any(isinstance(item, types) for item in chain):
            return category
    return "SUBSYSTEM_ERROR"


def failure_health(previous, exc, at_ms, retry_seconds):
    previous = previous if isinstance(previous, dict) else {}
    last_success = previous.get("last_success_ms")
    if last_success is None and (previous.get("state") == "HEALTHY" or previous.get("status") == "observed"):
        last_success = previous.get("at_ms")
    frames = traceback.extract_tb(exc.__traceback__)
    origin = frames[-1] if frames else None
    return dict(
        at_ms=at_ms,
        state="DEGRADED",
        location=f"{Path(origin.filename).name}:{origin.name}:{origin.lineno}" if origin else None,
        error_type=type(exc).__name__,
        error_category=error_category(exc),
        last_error_ms=at_ms,
        last_success_ms=last_success,
        failure_since_ms=previous.get("failure_since_ms") or at_ms,
        consecutive_failures=previous.get("consecutive_failures", 0) + 1,
        retry_at_ms=at_ms + int(retry_seconds * 1000),
    )


def success_health(at_ms):
    return dict(at_ms=at_ms, state="HEALTHY", last_success_ms=at_ms, consecutive_failures=0)
