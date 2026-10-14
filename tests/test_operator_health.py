from types import SimpleNamespace

import pytest
from pydantic import SecretStr

from bybit_flow.operator_health import warnings
from bybit_flow.storage import Store


def context(settings, now=2_000_000):
    settings = settings.model_copy(
        update={
            "scan_enabled": True,
            "research_alerts": True,
            "research_webhook": SecretStr("https://discord.com/api/webhooks/test/SECRET"),
        }
    )
    store = Store(settings.data_dir)
    store.put("clock_health", dict(source="binance", at_ms=now, skew_ms=40))
    scanner = SimpleNamespace(exchange="binance", status=dict(state="collecting"))
    recorder = SimpleNamespace(healthy=True, reason="recording")
    return settings, store, scanner, recorder, now


@pytest.mark.parametrize("skew", [-1950, 1950])
def test_clock_warning_appears_before_scan_hard_limit_and_clears_after_sync(settings, skew):
    settings, store, scanner, recorder, now = context(settings)
    try:
        store.put("clock_health", dict(source="binance", at_ms=now, skew_ms=skew))
        result = warnings(settings, store, scanner, recorder, now)
        assert [row["code"] for row in result] == ["CLOCK_DRIFT"]
        assert "Sync now" in result[0]["action"]
        assert ("behind" if skew < 0 else "ahead of") in result[0]["message"]
        store.put("clock_health", dict(source="binance", at_ms=now + 1, skew_ms=15))
        assert warnings(settings, store, scanner, recorder, now + 1) == []
    finally:
        store.close()


@pytest.mark.parametrize(
    "clock", [dict(source="okx", at_ms=2_000_000, skew_ms=0), dict(source="binance", at_ms=1, skew_ms=0)]
)
def test_old_or_other_source_clock_is_not_reported_as_synchronized(settings, clock):
    settings, store, scanner, recorder, now = context(settings)
    try:
        store.put("clock_health", clock)
        assert warnings(settings, store, scanner, recorder, now)[0]["code"] == "CLOCK_UNVERIFIED"
    finally:
        store.close()


@pytest.mark.parametrize(
    "skew,round_trip,expected", [(-1732, 3947, "CLOCK_CHECK_DELAYED"), (-4000, 4000, "CLOCK_DRIFT")]
)
def test_queued_request_is_not_misidentified_as_clock_drift(settings, skew, round_trip, expected):
    settings, store, scanner, recorder, now = context(settings)
    try:
        store.put("clock_health", dict(source="binance", at_ms=now, skew_ms=skew, round_trip_ms=round_trip))
        result = warnings(settings, store, scanner, recorder, now)
        assert [row["code"] for row in result] == [expected]
        if expected == "CLOCK_CHECK_DELAYED":
            assert result[0]["severity"] == "warning"
            assert "Sync now" not in result[0]["action"]
    finally:
        store.close()


def test_runtime_fault_warnings_clear_on_actual_recovery_without_changing_state(settings):
    settings, store, scanner, recorder, now = context(settings)
    try:
        recorder.healthy = False
        recorder.reason = "storage cap reached"
        store.put("storage_status", dict(usage_percent=100, at_ms=now))
        store.put("quote_health", dict(error_type="ConnectTimeout", at_ms=now))
        store.put("confirmation_health", dict(state="DEGRADED", last_error_ms=now))
        store.put("discord_transport", dict(status="uncertain", at_ms=now))
        store.put("runtime_health", dict(at_ms=now, primary_persistent_stream_health=dict(total=30, fresh=0)))
        keys = (
            "storage_status",
            "quote_health",
            "confirmation_health",
            "discord_transport",
            "runtime_health",
        )
        before = {key: store.get(key) for key in keys}
        result = warnings(settings, store, scanner, recorder, now)
        assert {row["code"] for row in result} == {
            "RECORDING_BLOCKED",
            "STORAGE_PRESSURE",
            "QUOTE_COLLECTION",
            "CONFIRMATION_ERROR",
            "DISCORD_DELIVERY",
            "FEED_STALE",
        }
        assert {key: store.get(key) for key in keys} == before
        assert "SECRET" not in str(result)
        recorder.healthy = True
        for key in keys:
            store.put(key, {})
        assert warnings(settings, store, scanner, recorder, now) == []
    finally:
        store.close()


def test_spread_warning_requires_actual_gaps_and_clears_with_valid_distinct_quotes(settings):
    settings, store, scanner, recorder, now = context(settings)
    settings = settings.model_copy(update={"spread_bucket_seconds": 60, "spread_window_minutes": 30})
    try:
        store.put("runtime_health", dict(source="binance", selected=["BTCUSDT"]))
        assert {row["code"] for row in warnings(settings, store, scanner, recorder, now)} == {
            "SPREAD_HISTORY_GAPS"
        }
        rows = [dict(bucket=i, known_ms=i * 60_000, event_ms=i * 60_000, bps=2) for i in range(21, 34)]
        store.put("spreads:60s:30m:binance:BTCUSDT", rows)
        assert warnings(settings, store, scanner, recorder, now) == []
    finally:
        store.close()


def test_claimed_selection_with_missing_live_subscriptions_is_visible(settings):
    settings, store, scanner, recorder, now = context(settings)
    try:
        scanner.status.update(deep_symbols=["BTCUSDT", "ETHUSDT"], at_ms=now - 180_000)
        store.put("runtime_health", dict(source="binance", at_ms=now, selected=["BTCUSDT"]))
        codes = {row["code"] for row in warnings(settings, store, scanner, recorder, now)}
        assert "SUBSCRIPTIONS_MISSING" in codes
        store.put("runtime_health", dict(source="binance", at_ms=now, selected=["BTCUSDT", "ETHUSDT"]))
        assert "SUBSCRIPTIONS_MISSING" not in {
            row["code"] for row in warnings(settings, store, scanner, recorder, now)
        }
        # Active lifecycle requirements may legitimately replace an optional
        # scan market at capacity; that is not a lost-subscription episode.
        store.put("runtime_health", dict(source="binance", at_ms=now, selected=["BTCUSDT", "ACTIVEUSDT"]))
        assert "SUBSCRIPTIONS_MISSING" not in {
            row["code"] for row in warnings(settings, store, scanner, recorder, now)
        }
    finally:
        store.close()
