"""Quote latency must not manufacture stale data or systematically miss buckets."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from bybit_flow.exchanges import VenueAPI
from bybit_flow.liquidity import SpreadHistory
from bybit_flow.scanner import Scanner


@pytest.mark.parametrize("funding_delay", [0, 20_000])
@pytest.mark.parametrize("stale_quote", [False, True])
async def test_funding_latency_does_not_age_new_quotes_or_refresh_stale_ones(
    settings, monkeypatch, funding_delay, stale_quote
):
    clock = 1_000_000

    def response(request):
        nonlocal clock
        if request.url.path.endswith("/premiumIndex"):
            clock += funding_delay
            return httpx.Response(
                200, json=[dict(symbol="BTCUSDT", time=1_000_000, lastFundingRate="0.0001")]
            )
        assert request.url.path.endswith("/bookTicker")
        return httpx.Response(
            200,
            json=[
                dict(
                    symbol="BTCUSDT",
                    bidPrice="99.99",
                    askPrice="100.01",
                    time=clock - (20_000 if stale_quote else 0),
                )
            ],
        )

    monkeypatch.setattr("bybit_flow.exchanges.now_ms", lambda: clock)
    api = VenueAPI("binance", settings, transport=httpx.MockTransport(response))
    try:
        body = await api.get("tickers", category="linear")
        ticker = body["result"]["list"][0]
        history = SpreadHistory(bucket_ms=60_000)
        history.add(ticker["observed_ms"], clock, float(ticker["bid1Price"]), float(ticker["ask1Price"]))
        assert history.assess(clock, min_samples=1)["eligible"] is not stale_quote
        assert ticker["funding_observed_ms"] == 1_000_000
        assert history.export()[0]["known_ms"] == clock
    finally:
        await api.close()


@pytest.mark.parametrize("duration,fail_first", [(2, False), (2, True), (75, False)])
async def test_quote_cadence_includes_request_time_and_never_replays_missed_samples(
    monkeypatch, duration, fail_first
):
    import bybit_flow.scanner as module

    clock, starts, sleeps, observed = 0, [], [], []

    async def get(*args, **kwargs):
        nonlocal clock
        starts.append(clock)
        clock += duration
        if fail_first and len(starts) == 1:
            raise OSError("temporary quote failure")
        return {"time": clock * 1000}

    async def sleep(delay):
        nonlocal clock
        sleeps.append(delay)
        clock += delay
        if len(sleeps) == 3:
            raise asyncio.CancelledError

    monkeypatch.setattr(
        module,
        "asyncio",
        SimpleNamespace(get_running_loop=lambda: SimpleNamespace(time=lambda: clock), sleep=sleep),
    )
    monkeypatch.setattr(module, "now_ms", lambda: clock * 1000)
    scanner = SimpleNamespace(
        source_ready=True,
        settings=SimpleNamespace(quote_sample_seconds=60),
        api=SimpleNamespace(get=get),
        store=Mock(),
        record_quotes=lambda body, receipt: observed.append(receipt),
    )
    with pytest.raises(asyncio.CancelledError):
        await Scanner.quote_loop(scanner)
    assert starts == [i * max(60, duration) for i in range(3)]
    assert sleeps == [max(0, 60 - duration)] * 3
    assert len(observed) == 3 - int(fail_first)
    if fail_first:
        scanner.store.put.assert_called_once_with("quote_health", {"at_ms": 2000, "error_type": "OSError"})
