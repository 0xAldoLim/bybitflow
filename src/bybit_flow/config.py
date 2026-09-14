from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    market_source: Literal["auto", "binance", "bybit", "okx", "multi"] = "auto"
    ml_enabled: bool = False
    ml_filter_research: bool = False
    validated_alert_tiers: list[Literal["SSS", "SS", "S"]] = Field(default_factory=lambda: ["SSS"])
    ops_webhook: SecretStr = SecretStr("")
    model_config = SettingsConfigDict(env_prefix="FLOW_", env_file=".env", extra="ignore")
    data_dir: Path = Path("data")
    scan_enabled: bool = False
    scan_seconds: int = Field(900, ge=60)
    execution_window_seconds: int = Field(900, ge=60, le=900)
    settle_coins: list[Literal["USDT", "USDC"]] = ["USDT"]
    core_watchlist: list[str] = ["BTCUSDT", "ETHUSDT"]
    deep_symbols: int = Field(8, ge=1, le=30)
    min_age_days: int = Field(30, ge=30)
    min_daily_turnover: float = Field(20_000_000, gt=0)
    max_spread_bps: float = Field(5, gt=0, le=50)
    quote_sample_seconds: int = Field(60, ge=30, le=300)
    spread_min_samples: int = Field(12, ge=12, le=72)
    spread_bucket_seconds: int = Field(300, ge=60, le=300)
    spread_window_minutes: int = Field(360, ge=15, le=360)
    hypothetical_notional: float = Field(1000, gt=0)
    equity: float | None = Field(None, gt=0)
    risk_fraction: float = Field(0.0025, gt=0, le=0.01)
    daily_loss_limit: float = 0.015
    weekly_loss_limit: float = 0.04
    correlated_risk_limit: float = 0.0075
    leverage: float = Field(3, ge=1, le=3)
    maintenance_margin_assumption: float = Field(0.01, gt=0, lt=0.1)
    liquidation_buffer_multiple: float = Field(3, ge=2)
    min_net_rr: float = Field(2, ge=1)
    taker_fee_bps: float = Field(5.5, ge=0)
    slippage_bps: float = Field(2, ge=0)
    funding_reserve_bps: float = Field(3, ge=0)
    book_depth: Literal[50, 200] = 50
    book_stale_ms: int = Field(5000, ge=1000)
    trade_stale_ms: int = Field(15000, ge=1000)
    queue_size: int = Field(10000, ge=100, le=10000)
    queue_byte_limit: int = Field(32_000_000, ge=1_000_000, le=128_000_000)
    tape_max_trades: int = Field(50_000, ge=1000, le=200_000)
    max_storage_gb: float = Field(10, gt=0.1)
    recording_retention_enabled: bool = False
    ml_two_stage: bool = False
    raw_retention_days: int = Field(14, ge=1)
    rest_requests_per_second: float = Field(3, gt=0, le=10)
    research_alerts: bool = False
    sss_research: bool = False
    tv_enabled: bool = False
    tv_proxy_research: bool = False
    tv_require_native_confirmation: bool = False
    tv_token: SecretStr = SecretStr("")
    tv_symbols: list[str] = ["BTCUSDT", "ETHUSDT"]
    tv_max_age_ms: int = Field(90_000, ge=1000, le=180_000)
    tv_queue_limit: int = Field(1000, ge=10, le=10000)
    research_webhook: SecretStr = SecretStr("")
    discord_webhook: SecretStr = SecretStr("")
    admin_token: SecretStr = SecretStr("")
    dashboard_url: str = "http://127.0.0.1:8000"
    cooldown_minutes: int = Field(240, ge=15)

    @field_validator("equity", mode="before")
    @classmethod
    def empty_equity(cls, value):
        return None if value == "" else value

    def public(self):
        return self.model_dump(
            mode="json",
            exclude={"research_webhook", "discord_webhook", "admin_token", "tv_token", "ops_webhook"},
        )
