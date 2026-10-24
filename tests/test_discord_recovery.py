import asyncio
from types import SimpleNamespace

import httpx
import pytest
from pydantic import SecretStr

from bybit_flow.discord_health import (
    CHECK_FRESH_MS,
    check_webhooks,
    monitor,
    reachability,
    record_transport,
    transport_health,
)
from bybit_flow.identity import delivery_status
from bybit_flow.operator_health import warnings
from bybit_flow.storage import Store


@pytest.fixture
def recovery(settings, monkeypatch):
    settings = settings.model_copy(
        update={
            "scan_enabled": True,
            "research_alerts": True,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/123/signals-SECRET"),
            "monitoring_webhook": SecretStr("https://discord.com/api/webhooks/456/monitoring-SECRET"),
            "ops_webhook": SecretStr(""),
        }
    )
    store = Store(settings.data_dir)
    clock = [2_000_000]
    monkeypatch.setattr("bybit_flow.discord_health.now_ms", lambda: clock[0])
    store.put("clock_health", dict(source="binance", at_ms=clock[0], skew_ms=0))
    try:
        yield settings, store, clock
    finally:
        store.close()


def discord_warnings(settings, store, now):
    scanner = SimpleNamespace(exchange="binance", status={"state": "collecting"})
    recorder = SimpleNamespace(healthy=True, reason="recording")
    return [w for w in warnings(settings, store, scanner, recorder, now) if w["code"].startswith("DISCORD")]


def success(request):
    assert request.method == "GET"
    assert "wait" not in request.url.params
    return httpx.Response(200, json={"id": request.url.path.split("/")[3], "token": "SECRET"})


async def test_read_only_recovery_preserves_uncertain_receipts_and_real_delivery_counts(recovery):
    settings, store, clock = recovery
    with store.db:
        store.db.execute(
            "INSERT INTO outbox VALUES(?,?,?,?,?,?)",
            ("research:unconfirmed:initial", "unconfirmed", "uncertain", "{}", None, clock[0] - 1000),
        )
    failure = dict(status="uncertain", category="CONNECTION_ERROR", at_ms=clock[0] - 1000)
    record_transport(store, failure, "monitoring", "POST")
    before_outbox = list(store.db.execute("SELECT * FROM outbox"))
    before_counts = delivery_status(store)
    assert discord_warnings(settings, store, clock[0])[0]["severity"] == "error"
    await check_webhooks(settings, store, httpx.MockTransport(success))
    result = discord_warnings(settings, store, clock[0])
    assert len(result) == 1
    assert result[0]["code"] == "DISCORD_DELIVERY_HISTORY" and result[0]["severity"] == "warning"
    assert "reachable now" in result[0]["message"]
    assert list(store.db.execute("SELECT * FROM outbox")) == before_outbox
    assert delivery_status(store) == before_counts
    assert transport_health(store)["destinations"]["monitoring"]["status"] == "uncertain"
    assert "SECRET" not in str(reachability(store, settings, clock[0]))
    assert "SECRET" not in store.get("discord_reachability:monitoring").__str__()


async def test_another_destination_or_stale_probe_cannot_establish_recovery(recovery):
    settings, store, clock = recovery
    record_transport(
        store, dict(status="uncertain", category="TIMEOUT", at_ms=clock[0] - 1), "monitoring", "POST"
    )

    def partial(request):
        if "/456/" in request.url.path:
            raise httpx.ConnectTimeout("https://discord.com/SECRET", request=request)
        return success(request)

    await check_webhooks(settings, store, httpx.MockTransport(partial))
    result = discord_warnings(settings, store, clock[0])
    assert {w["code"] for w in result} == {"DISCORD_CONNECTIVITY", "DISCORD_DELIVERY"}
    assert "SECRET" not in str(result)
    clock[0] += 30_001
    await check_webhooks(settings, store, httpx.MockTransport(success))
    assert discord_warnings(settings, store, clock[0])[0]["code"] == "DISCORD_DELIVERY_HISTORY"
    clock[0] += CHECK_FRESH_MS + 1
    result = discord_warnings(settings, store, clock[0])
    assert "DISCORD_DELIVERY" in {w["code"] for w in result}
    assert "DISCORD_DELIVERY_HISTORY" not in {w["code"] for w in result}
    assert "DISCORD_CONNECTIVITY_UNKNOWN" in {w["code"] for w in result}


@pytest.mark.parametrize("status,category", [("rate-limited", "HTTP"), ("rejected", "HTTP")])
async def test_get_does_not_clear_write_rejection_or_rate_limit(recovery, status, category):
    settings, store, clock = recovery
    record_transport(store, dict(status=status, category=category, at_ms=clock[0] - 1), "signals", "POST")
    await check_webhooks(settings, store, httpx.MockTransport(success))
    result = discord_warnings(settings, store, clock[0])
    assert len(result) == 1 and result[0]["code"] == "DISCORD_DELIVERY"
    assert result[0]["severity"] == "error"
    record_transport(store, dict(status="sent", at_ms=clock[0] + 1), "signals", "PATCH")
    assert discord_warnings(settings, store, clock[0] + 1) == []


async def test_new_write_failure_after_probe_still_reports_error(recovery):
    settings, store, clock = recovery
    await check_webhooks(settings, store, httpx.MockTransport(success))
    record_transport(
        store, dict(status="uncertain", category="TIMEOUT", at_ms=clock[0] + 1), "signals", "POST"
    )
    assert discord_warnings(settings, store, clock[0] + 1)[0]["code"] == "DISCORD_DELIVERY"


async def test_checks_back_off_and_honor_rate_limits_across_worker_restarts(recovery):
    settings, store, clock = recovery
    calls = []

    def limited(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "1200"}, json={"retry_after": 600})

    await check_webhooks(settings, store, httpx.MockTransport(limited))
    assert len(calls) == 2
    # Recreating a worker or calling the check again does not reset the durable cooldown.
    clock[0] += 600_000
    await check_webhooks(settings, store, httpx.MockTransport(limited))
    assert len(calls) == 2
    clock[0] += 600_001
    await check_webhooks(settings, store, httpx.MockTransport(success))
    assert all(v["state"] == "AVAILABLE" for v in reachability(store, settings, clock[0]).values())


async def test_webhook_change_and_backward_clock_jump_invalidate_old_success(recovery):
    settings, store, clock = recovery
    await check_webhooks(settings, store, httpx.MockTransport(success))
    changed = settings.model_copy(
        update={"monitoring_webhook": SecretStr("https://discord.com/api/webhooks/789/NEW-SECRET")}
    )
    assert "monitoring" not in reachability(store, changed, clock[0])
    calls = []

    def record(request):
        calls.append(request)
        return success(request)

    await check_webhooks(changed, store, httpx.MockTransport(record))
    assert len(calls) == 1 and "/789/" in calls[0].url.path
    clock[0] -= 1000
    await check_webhooks(changed, store, httpx.MockTransport(record))
    assert len(calls) == 3  # Both future-dated caches are rechecked, not trusted.


@pytest.mark.parametrize("status,body", [(404, {}), (302, {}), (200, {"id": "wrong"}), (200, ["invalid"])])
async def test_denied_redirected_and_invalid_responses_fail_closed(recovery, status, body):
    settings, store, clock = recovery
    calls = []

    def invalid(request):
        calls.append(request)
        return httpx.Response(status, json=body, headers={"Location": "https://example.invalid/SECRET"})

    await check_webhooks(settings, store, httpx.MockTransport(invalid))
    assert len(calls) == 2  # Redirect targets are never contacted.
    assert all(v["state"] == "UNAVAILABLE" for v in reachability(store, settings, clock[0]).values())
    assert all(w["code"] == "DISCORD_CONNECTIVITY" for w in discord_warnings(settings, store, clock[0]))


async def test_total_request_deadline_and_cancellation_do_not_touch_delivery(recovery, monkeypatch):
    settings, store, clock = recovery
    monkeypatch.setattr("bybit_flow.discord_health.REQUEST_TIMEOUT_SECONDS", 0.01)

    async def stall(request):
        await asyncio.Event().wait()

    await asyncio.wait_for(check_webhooks(settings, store, httpx.MockTransport(stall)), timeout=1)
    assert all(v["category"] == "TIMEOUT" for v in reachability(store, settings, clock[0]).values())
    assert store.db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    clock[0] += 31_000
    monkeypatch.setattr("bybit_flow.discord_health.REQUEST_TIMEOUT_SECONDS", 10)
    task = asyncio.create_task(check_webhooks(settings, store, httpx.MockTransport(stall)))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_monitor_repeats_independently_of_market_scans(recovery, monkeypatch):
    settings, store, clock = recovery
    calls = []

    async def check(*args):
        calls.append(args)

    async def sleep(delay):
        assert delay == 30
        raise asyncio.CancelledError

    monkeypatch.setattr("bybit_flow.discord_health.check_webhooks", check)
    monkeypatch.setattr("bybit_flow.discord_health.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await monitor(settings, store)
    assert calls == [(settings, store)]


async def test_unconfigured_endpoints_do_not_make_requests(recovery):
    settings, store, clock = recovery
    empty = settings.model_copy(
        update={"research_webhook": SecretStr(""), "monitoring_webhook": SecretStr("")}
    )

    def forbidden(request):
        pytest.fail("Unconfigured webhook must not make a request")

    await check_webhooks(empty, store, httpx.MockTransport(forbidden))
    assert reachability(store, empty, clock[0]) == {}
