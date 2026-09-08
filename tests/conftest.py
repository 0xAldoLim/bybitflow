import math

import pytest

from bybit_flow.config import Settings
from bybit_flow.models import Candle, Instrument, Signal


@pytest.fixture
def settings(tmp_path):
    return Settings(_env_file=None, data_dir=tmp_path, rest_requests_per_second=10)


@pytest.fixture
def instrument_row():
    return {
        "symbol": "TESTUSDT",
        "baseCoin": "TEST",
        "settleCoin": "USDT",
        "quoteCoin": "USDT",
        "contractType": "LinearPerpetual",
        "status": "Trading",
        "launchTime": "1000",
        "isPreListing": False,
        "fundingInterval": 480,
        "priceFilter": {"tickSize": "0.01"},
        "lotSizeFilter": {
            "qtyStep": "0.001",
            "minOrderQty": "0.001",
            "minNotionalValue": "5",
            "maxMktOrderQty": "100000",
        },
        "leverageFilter": {"maxLeverage": "50"},
    }


@pytest.fixture
def instrument(instrument_row):
    return Instrument.parse(instrument_row)


@pytest.fixture
def signal():
    return Signal(
        id="test-signal",
        symbol="TESTUSDT",
        direction="LONG",
        family="trend_pullback",
        created_ms=1000,
        expires_ms=3_601_000,
        regime="trending up",
        entry=100,
        zone=(99, 101),
        stop=95,
        tp1=115,
        tp2=120,
        invalidation="Price below 95",
        reason="Synthetic test fixture, not market data",
    )


def synthetic_bars(count=200, interval=3_600_000, start=0):
    bars = []
    for i in range(count):
        opening = 100 + i * 0.12 + math.sin(i / 4) * 2
        close = opening + math.sin(i) * 0.3
        bars.append(
            Candle(
                start + i * interval,
                interval,
                opening,
                max(opening, close) + 0.8,
                min(opening, close) - 0.8,
                close,
                100 + i % 11,
                (100 + i % 11) * close,
            )
        )
    return bars


@pytest.fixture
def bars():
    return synthetic_bars()
