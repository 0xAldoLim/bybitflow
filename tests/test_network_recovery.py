import asyncio
import socket
import ssl
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from bybit_flow.operator_health import warnings
from bybit_flow.recovery import error_category, failure_health, rest_batch
from bybit_flow.scanner import Scanner
from bybit_flow.storage import Store


async def test_failed_rest_batch_cancels_siblings_and_releases_request_slots():
    semaphore, running = asyncio.Semaphore(1), asyncio.Event()
    finished = []

    async def sibling():
        async with semaphore:
            running.set()
            try:
                await asyncio.Event().wait()
            finally:
                finished.append("request cancelled")

    async def failure():
        await running.wait()
        raise httpx.ConnectTimeout("offline fixture")

    with pytest.raises(httpx.ConnectTimeout):
        await rest_batch(sibling(), failure())
    assert finished == ["request cancelled"]
    assert not semaphore.locked()
    assert await rest_batch(asyncio.sleep(0, result="retry succeeded")) == ["retry succeeded"]


async def test_outer_scan_cancellation_leaves_no_rest_children():
    running, closed = asyncio.Event(), asyncio.Event()

    async def request():
        running.set()
        try:
            await asyncio.Event().wait()
        finally:
            closed.set()

    task = asyncio.create_task(rest_batch(request()))
    await running.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()


@pytest.mark.parametrize(
    "cause,expected",
    [
        (socket.gaierror(-2, "fixture"), "DNS_RESOLUTION"),
        (ssl.SSLCertVerificationError("fixture"), "TLS_CERTIFICATE"),
        (httpx.ConnectTimeout("fixture"), "NETWORK_TIMEOUT"),
        (ConnectionError("fixture"), "CONNECTION_ERROR"),
        (PermissionError("fixture"), "ACCESS_OR_RATE_LIMIT"),
    ],
)
def test_wrapped_transport_errors_are_classified_without_secret_messages(cause, expected):
    wrapped = httpx.ConnectError("https://example.invalid/SECRET")
    wrapped.__cause__ = cause
    assert error_category(wrapped) == expected
    state = failure_health(dict(status="observed", at_ms=1000), wrapped, 2000, 15)
    assert state["last_success_ms"] == 1000
    assert state["retry_at_ms"] == 17_000
    assert "SECRET" not in str(state)
    assert failure_health(state, wrapped, 3000, 15)["failure_since_ms"] == 2000


def test_partial_feed_outage_is_visible_after_grace_then_clears(settings):
    settings.scan_enabled = True
    store = Store(settings.data_dir)
    now = 2_000_000
    scanner = SimpleNamespace(exchange="binance", status=dict(state="collecting"))
    recorder = SimpleNamespace(healthy=True)
    runtime = dict(
        at_ms=now,
        source="binance",
        primary_persistent_stream_health=dict(total=30, fresh=11),
        feeds_degraded_since_ms=now - 30_000,
    )
    try:
        store.put("runtime_health", runtime)
        assert "FEED_DEGRADED" not in {w["code"] for w in warnings(settings, store, scanner, recorder, now)}
        runtime["feeds_degraded_since_ms"] = now - 120_000
        store.put("runtime_health", runtime)
        assert "FEED_DEGRADED" in {w["code"] for w in warnings(settings, store, scanner, recorder, now)}
        runtime["primary_persistent_stream_health"]["fresh"] = 30
        store.put("runtime_health", runtime)
        assert "FEED_DEGRADED" not in {w["code"] for w in warnings(settings, store, scanner, recorder, now)}
    finally:
        store.close()


async def test_quote_failure_retries_then_rebuilds_only_real_samples(settings, monkeypatch):
    store = Store(settings.data_dir)
    scanner = Scanner.__new__(Scanner)
    scanner.settings, scanner.store, scanner.source_ready = settings, store, True
    scanner.context, scanner.oi_series = {}, {}
    scanner.pending_symbols = lambda: []
    scanner.streams = SimpleNamespace(selected=["BTCUSDT"])
    scanner.api = SimpleNamespace(
        name="binance",
        get=AsyncMock(
            side_effect=[
                httpx.ConnectTimeout("fixture"),
                dict(
                    time=2_000_000,
                    result=dict(list=[dict(symbol="BTCUSDT", bid1Price="100", ask1Price="100.01")]),
                ),
            ]
        ),
    )
    store.put("quote_health", dict(status="observed", at_ms=1_000_000))
    clock, delays = 1_500_000, []

    async def sleep(delay):
        nonlocal clock
        delays.append(delay)
        if len(delays) == 1:
            state = store.get("quote_health")
            assert state["error_category"] == "NETWORK_TIMEOUT"
            assert state["last_success_ms"] == 1_000_000
            assert state["retry_at_ms"] == clock + 15_000
            assert not store.get(scanner.spread_key("BTCUSDT"))
            clock = 2_000_000
        else:
            raise asyncio.CancelledError

    monkeypatch.setattr("bybit_flow.scanner.asyncio.sleep", sleep)
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: clock)
    try:
        with pytest.raises(asyncio.CancelledError):
            await scanner.quote_loop()
        assert delays[0] == 15 and 0 < delays[1] <= settings.quote_sample_seconds
        health = store.get("quote_health")
        assert health["state"] == "HEALTHY" and "error_type" not in health
        history = scanner.spread_history("BTCUSDT")
        assert len(history.export()) == 1
        assert not history.assess(clock)["eligible"]  # Recovery cannot bypass warm-up.
    finally:
        store.close()


async def test_clock_network_failure_retries_quickly_without_retaining_false_drift(settings, monkeypatch):
    store = Store(settings.data_dir)
    scanner = SimpleNamespace(
        exchange="binance", settings=settings, store=store, status=dict(state="collecting")
    )
    settings.research_alerts = False
    settings.scan_enabled = True
    clock, delays, closed = 2_000_000, [], []
    store.put("clock_health", dict(source="binance", at_ms=clock - 30_000, skew_ms=1950))
    outcomes = [httpx.ConnectTimeout("fixture"), dict(time=clock)]

    class API:
        def __init__(self, *args):
            pass

        async def get(self, endpoint):
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        async def close(self):
            closed.append(True)

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 1:
            recorder = SimpleNamespace(healthy=True)
            current = warnings(settings, store, scanner, recorder, clock)
            assert "CLOCK_CHECK_FAILED" in {w["code"] for w in current}
            assert "CLOCK_DRIFT" not in {w["code"] for w in current}
        else:
            raise asyncio.CancelledError

    monkeypatch.setattr("bybit_flow.scanner.VenueAPI", API)
    monkeypatch.setattr("bybit_flow.scanner.asyncio.sleep", sleep)
    monkeypatch.setattr("bybit_flow.scanner.now_ms", lambda: clock)
    try:
        with pytest.raises(asyncio.CancelledError):
            await Scanner.clock_loop(scanner)
        assert delays == [15, 60] and len(closed) == 2
        assert store.get("clock_check_health")["state"] == "HEALTHY"
        assert store.get("clock_health")["skew_ms"] == 0
        assert not any(
            w["code"].startswith("CLOCK_")
            for w in warnings(settings, store, scanner, SimpleNamespace(healthy=True), clock)
        )
    finally:
        store.close()
