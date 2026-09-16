"""Explicitly labeled end-to-end test. No signal, feature or label persistence."""

from decimal import Decimal

from .horizons import assign
from .models import Instrument, Signal
from .notifications import Notifier, embed
from .orderflow import Book
from .risk import evaluate_risk
from .scoring import score
from .storage import now_ms


async def test_signal(settings, store, transport=None):
    now = now_ms()
    s = Signal(
        id=f"synthetic-{now}",
        symbol="TESTUSDT",
        direction="LONG",
        family="trend_pullback",
        created_ms=now,
        expires_ms=now + 120_000,
        regime="trending up",
        entry=100,
        zone=(99, 101),
        stop=95,
        tp1=115,
        tp2=120,
        invalidation="Synthetic stop only",
        reason="Synthetic pipeline verification",
        synthetic=True,
        evidence={
            "trigger_bar_end": now,
            "h4": {"efficiency": 0.5},
            "h1": {"atr": 3},
            "flow": {"available": True, "delta_pct": 30, "stacked_buy": 4},
            "derivatives": {"funding_rate": 0.0001, "oi_change_pct": 1},
        },
    )
    assign(s, "CORE_INTRADAY")
    instrument = Instrument(
        symbol=s.symbol,
        base="TEST",
        settle="USDT",
        launch_ms=0,
        tick="0.01",
        qty_step="0.001",
        min_qty="0.001",
        min_notional="5",
        max_qty="100000",
        max_leverage=50,
        funding_interval_minutes=480,
        metadata={"synthetic": True},
    )
    book = Book()
    book.valid = True
    book.bids = {Decimal("99.99"): Decimal("100000")}
    book.asks = {Decimal("100.01"): Decimal("100000")}
    book.receipt_ms = book.event_ms = now
    cfg = settings.model_copy(update={"equity": None})
    s.risk = evaluate_risk(s, instrument, cfg, book, funding_rate=0.0001)
    s.gates = list(s.risk["reasons"])
    score(s, True, True)
    if not s.risk["accepted"]:
        return dict(status="risk-test-failed", reasons=s.gates)
    s.state = "CONFIRMED"
    payload = embed(s, settings.dashboard_url)
    payload["embeds"][0]["title"] = "TEST SIGNAL · NOT A REAL TRADE"
    payload["embeds"][0]["description"] = (
        "Synthetic candidate → quality score → horizon → risk → Discord.\n"
        + payload["embeds"][0]["description"]
    )
    payload["embeds"][0].pop("url", None)
    secret = settings.research_webhook.get_secret_value()
    result = (
        await Notifier(settings, store, transport).deliver(f"test-signal:{now}", None, payload, secret)
        if secret
        else "NOT_CONFIGURED"
    )
    return dict(
        status=result,
        synthetic=True,
        horizon=s.horizon_profile,
        quality=s.quality,
        risk_passed=s.risk["accepted"],
        signal_rows_created=0,
        ml_rows_created=0,
    )
