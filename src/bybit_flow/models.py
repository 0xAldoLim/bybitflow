from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

D = Decimal
DAY = 86_400_000


@dataclass(frozen=True)
class Candle:
    start: int
    interval: int  # milliseconds
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float

    @property
    def end(self):
        return self.start + self.interval


@dataclass(frozen=True)
class Trade:
    symbol: str
    event_ms: int
    receipt_ms: int
    trade_id: str
    side: str
    price: Decimal
    size: Decimal

    def __post_init__(self):
        if self.side not in {"Buy", "Sell"} or self.price <= 0 or self.size <= 0:
            raise ValueError("Invalid executed trade")


class Instrument(BaseModel):
    symbol: str
    base: str
    settle: str
    launch_ms: int
    tick: Decimal
    qty_step: Decimal
    min_qty: Decimal
    min_notional: Decimal
    max_qty: Decimal
    max_leverage: float
    funding_interval_minutes: int
    metadata: dict

    @classmethod
    def parse(cls, r):
        lot = r["lotSizeFilter"]
        return cls(
            symbol=r["symbol"],
            base=r["baseCoin"],
            settle=r["settleCoin"],
            launch_ms=int(r["launchTime"]),
            tick=r["priceFilter"]["tickSize"],
            qty_step=lot["qtyStep"],
            min_qty=lot["minOrderQty"],
            min_notional=lot.get("minNotionalValue", "0"),
            max_qty=lot.get("maxMarketOrderQty", lot.get("maxMktOrderQty", "0")),
            max_leverage=float(r["leverageFilter"]["maxLeverage"]),
            funding_interval_minutes=int(r["fundingInterval"]),
            metadata=r,
        )


State = Literal[
    "WATCHLIST", "PENDING CONFIRMATION", "CONFIRMED", "ALERTED", "INVALIDATED", "EXPIRED", "RESOLVED"
]


class Signal(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)
    id: str
    symbol: str
    direction: Literal["LONG", "SHORT"]
    family: str
    version: str = "rules-0.1.0"
    created_ms: int
    expires_ms: int
    source: Literal["bybit", "tradingview"] = "bybit"
    trigger_expires_ms: int | None = None
    holding_deadline_ms: int | None = None
    state: State = "WATCHLIST"
    regime: str
    entry: float
    zone: tuple[float, float]
    stop: float
    tp1: float
    tp2: float
    invalidation: str
    reason: str
    quality: float = 0
    raw_tier: str = "F"
    final_tier: str = "RESEARCH"
    qualification: dict = Field(default_factory=lambda: {"status": "Uncalibrated", "probability": None})
    evidence: dict = Field(default_factory=dict)
    risk: dict = Field(default_factory=dict)
    gates: list[str] = Field(default_factory=list)
    coverage: dict = Field(default_factory=dict)
