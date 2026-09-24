from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, computed_field

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


@dataclass(frozen=True, slots=True)
class Trade:
    symbol: str
    event_ms: int
    receipt_ms: int
    trade_id: str
    side: str
    price: Decimal
    size: Decimal
    exchange: str = "bybit"
    exchange_symbol: str | None = None
    aggressor_side_quality: str = "exchange_reported"

    def __post_init__(self):
        if self.side not in {"Buy", "Sell"} or self.price <= 0 or self.size <= 0:
            raise ValueError("Invalid executed trade")


class Instrument(BaseModel):
    exchange: str = "bybit"
    exchange_symbol: str | None = None
    quote: str = "USDT"
    contract_multiplier: Decimal = Decimal("1")
    quantity_unit: str = "base"
    contract_type: str = "linear_perpetual"
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
            exchange_symbol=r["symbol"],
            quote=r.get("quoteCoin", r["settleCoin"]),
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
    source: Literal["bybit", "binance", "okx", "tradingview"] = "bybit"
    horizon_profile: Literal["LEGACY", "SHORT_INTRADAY", "CORE_INTRADAY", "SWING", "EXTENDED_SWING"] = (
        "LEGACY"
    )
    context_timeframe: str = "240"
    setup_timeframe: str = "60"
    execution_timeframe: str = "15"
    expected_hold_min: int = 60
    expected_hold_max: int = 240
    primary_tracking_deadline: int | None = None
    lifecycle_version: str = "legacy"
    setup_thesis_id: str | None = None
    session: dict = Field(default_factory=dict)
    entry_session: str | None = None
    synthetic: bool = False
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
    calibrated_probability: float | None = Field(None, ge=0, le=1)
    probability_uncertainty: tuple[float, float] | None = None
    expected_net_r: float | None = None
    expected_net_r_uncertainty: tuple[float, float] | None = None
    validation_status: str = "unvalidated"
    model_version: str | None = None
    feature_schema_version: str = "candidate-v4"
    data_coverage: float | None = Field(None, ge=0, le=1)

    @computed_field
    @property
    def raw_quality_score(self) -> float:
        return self.quality

    @computed_field
    @property
    def rejection_reasons(self) -> list[str]:
        return self.gates

    @computed_field
    @property
    def quality_score(self) -> float:
        return self.quality

    @computed_field
    @property
    def quality_tier(self) -> str:
        return self.raw_tier

    @computed_field
    @property
    def score_components(self) -> dict:
        return self.evidence.get("score_components", {})

    @computed_field
    @property
    def score_reasons(self) -> list:
        return self.evidence.get("score_reasons", [])
